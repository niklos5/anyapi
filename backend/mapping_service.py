from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from backend.mapping_executor import flatten_target_schema
from backend.roaster_mapping_executor import MappingExecutor
from backend.roaster_mapping_repair import repair_mapping_spec
from backend.roaster_mapping_validator import validate_mapping_spec
from backend.schema_fingerprint import SchemaStructureExtractor

_BEDROCK_CLIENT: Optional[Any] = None


def _get_bedrock_client() -> Any:
    global _BEDROCK_CLIENT
    if _BEDROCK_CLIENT is None:
        region = os.getenv("BEDROCK_REGION") or os.getenv("AWS_REGION") or "us-east-1"
        _BEDROCK_CLIENT = boto3.client("bedrock-runtime", region_name=region)
    return _BEDROCK_CLIENT


def _bedrock_model_id() -> Optional[str]:
    return os.getenv("BEDROCK_MODEL_ID")


def _build_bedrock_prompt(
    input_schema: Dict[str, Any], target_schema: Any, items_path: str
) -> str:
    return (
        "You are an expert data mapper. Generate a JSON mapping spec in the roaster format.\n"
        "Return ONLY valid JSON (no markdown, no extra text).\n\n"
        "Rules:\n"
        f"- The output must be a JSON object with keys: version, defaults, broadcast, mappings.\n"
        f"- mappings.items.path must be the JSONPath array: \"{items_path}\".\n"
        "- mappings.items.map should map target fields to source paths.\n"
        "- Use JSONPath strings that start with '$.' for sources.\n"
        "- If you cannot find a source for a target, set source to null.\n"
        "- Do not invent fields that are not in the target schema.\n\n"
        "Input schema (JSONPath -> type):\n"
        f"{json.dumps(input_schema, indent=2)}\n\n"
        "Target schema (JSON or JSONPath map):\n"
        f"{json.dumps(target_schema, indent=2)}\n"
    )


def _invoke_bedrock(prompt: str) -> Optional[str]:
    model_id = _bedrock_model_id()
    if not model_id:
        return None
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 2048,
        "temperature": 0,
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        ],
    }
    client = _get_bedrock_client()
    response = client.invoke_model(
        modelId=model_id,
        body=json.dumps(body),
    )
    payload = json.loads(response["body"].read())
    content = payload.get("content") or []
    if content and isinstance(content, list):
        text = content[0].get("text") if isinstance(content[0], dict) else None
        return text if isinstance(text, str) else None
    return None


def _generate_mapping_with_bedrock(
    payload: Any, target_schema: Any
) -> Optional[Dict[str, Any]]:
    if not _bedrock_model_id():
        return None
    extractor = SchemaStructureExtractor(max_items_per_array=10)
    input_schema = extractor.extract(payload)
    items_path = _choose_items_path(payload)
    prompt = _build_bedrock_prompt(input_schema, target_schema, items_path)
    try:
        raw_text = _invoke_bedrock(prompt)
    except (BotoCoreError, ClientError, ValueError, KeyError) as exc:
        print(f"Bedrock mapping generation failed: {exc}")
        return None
    if not raw_text:
        return None
    mapping_spec, _ = repair_mapping_spec(raw_text)
    return mapping_spec


def _normalize_target_path(path: str) -> str:
    normalized = path
    if normalized.startswith("$."):
        normalized = normalized[2:]
    elif normalized.startswith("$"):
        normalized = normalized[1:]
    normalized = normalized.replace("[*]", "")
    normalized = normalized.replace("[]", "")
    return normalized


def _normalize_source_path(path: Any) -> Optional[str]:
    if path is None:
        return None
    if not isinstance(path, str):
        return None
    trimmed = path.strip()
    if not trimmed:
        return None
    if trimmed.startswith("$"):
        return trimmed
    return f"$.{trimmed}"


def _choose_items_path(payload: Any) -> str:
    if isinstance(payload, list):
        return "$[]"
    if isinstance(payload, dict):
        for key in ("items", "data", "records"):
            value = payload.get(key)
            if isinstance(value, list):
                return f"$.{key}[]"
    return "$.items[]"


def _mapping_transform(transform: Optional[str]) -> Optional[str]:
    if transform == "string":
        return "to_string"
    if transform == "number":
        return "to_float"
    if transform == "integer":
        return "to_int"
    if transform == "boolean":
        return "to_boolean"
    if transform == "date":
        return "to_string"
    return None


def _build_roaster_mapping_from_list(
    mapping_spec: Dict[str, Any], payload: Any
) -> Dict[str, Any]:
    items_path = _choose_items_path(payload)
    entries = mapping_spec.get("mappings") or []
    defaults = mapping_spec.get("defaults") or {}
    roaster_map: Dict[str, Any] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        target = entry.get("target")
        if not isinstance(target, str):
            continue
        source = entry.get("source")
        if isinstance(source, list):
            sources = [_normalize_source_path(item) for item in source]
            sources = [item for item in sources if item]
            normalized_source: Any = sources if sources else None
        else:
            normalized_source = _normalize_source_path(source)
        spec: Dict[str, Any] = {"source": normalized_source}
        transform = _mapping_transform(entry.get("transform"))
        if transform:
            spec["transform"] = transform
        if entry.get("required") is True:
            spec["required"] = True
        if isinstance(entry.get("match"), dict):
            spec["match"] = entry["match"]
        if entry.get("default") is not None:
            defaults[target] = entry["default"]
        roaster_map[target] = spec

    return {
        "version": "1.0",
        "defaults": defaults,
        "broadcast": {},
        "mappings": {"items": {"path": items_path, "map": roaster_map}},
    }


def _auto_mapping_spec(payload: Any, target_schema: Any) -> Dict[str, Any]:
    extractor = SchemaStructureExtractor(max_items_per_array=10)
    input_schema = extractor.extract(payload)
    items_path = _choose_items_path(payload)
    target_paths: Dict[str, Any] = _extract_target_paths(target_schema)
    item_targets = {
        _normalize_target_path(path): path
        for path in target_paths.keys()
        if isinstance(path, str) and ".items[]" in path
    }
    normalized_sources = {
        _normalize_target_path(path): path
        for path in input_schema.keys()
        if isinstance(path, str)
    }

    def _pick_source(target_field: str) -> Optional[str]:
        if target_field in normalized_sources:
            return normalized_sources[target_field]
        target_tail = target_field.split(".")[-1]
        for normalized, original in normalized_sources.items():
            if normalized.split(".")[-1] == target_tail:
                return original
        return None

    roaster_map: Dict[str, Any] = {}
    for normalized_target in sorted(item_targets.keys()):
        source_path = _pick_source(normalized_target)
        roaster_map[normalized_target] = {
            "source": source_path if source_path else None
        }

    return {
        "version": "1.0",
        "defaults": {},
        "broadcast": {},
        "mappings": {"items": {"path": items_path, "map": roaster_map}},
    }


def _prepare_roaster_mapping(
    mapping_spec: Optional[Dict[str, Any]],
    payload: Any,
    target_schema: Any,
) -> Dict[str, Any]:
    mappings_value = mapping_spec.get("mappings") if mapping_spec else None
    if not mapping_spec or not mappings_value:
        generated = _generate_mapping_with_bedrock(payload, target_schema)
        return generated if generated else _auto_mapping_spec(payload, target_schema)
    if isinstance(mappings_value, list):
        return _build_roaster_mapping_from_list(mapping_spec, payload)
    if isinstance(mappings_value, dict):
        return mapping_spec
    generated = _generate_mapping_with_bedrock(payload, target_schema)
    return generated if generated else _auto_mapping_spec(payload, target_schema)


def _extract_target_paths(target_schema: Any) -> Dict[str, Any]:
    if isinstance(target_schema, dict):
        if any(isinstance(key, str) and key.startswith("$") for key in target_schema.keys()):
            return target_schema
    if isinstance(target_schema, (dict, list)):
        return flatten_target_schema(target_schema)
    return {}


def _extract_preview_rows(data: Any, limit: int = 3) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)][:limit]
    if isinstance(data, dict):
        items = data.get("items")
        if isinstance(items, list):
            return [row for row in items if isinstance(row, dict)][:limit]
    return []


def _detect_issues(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    if not rows:
        return issues

    field_types: Dict[str, set[str]] = {}
    null_fields: Dict[str, int] = {}

    for row in rows:
        for key, value in row.items():
            types = field_types.setdefault(key, set())
            if value is None or value == "":
                null_fields[key] = null_fields.get(key, 0) + 1
                continue
            types.add(type(value).__name__)

    for field, types in field_types.items():
        if len(types) > 1:
            issues.append(
                {
                    "field": field,
                    "level": "warning",
                    "message": f"Mixed value types detected ({', '.join(sorted(types))}).",
                }
            )
    for field, count in null_fields.items():
        issues.append(
            {
                "field": field,
                "level": "warning",
                "message": f"{count} sample rows missing values.",
            }
        )

    return issues


def analyze_payload(data: Any) -> Dict[str, Any]:
    extractor = SchemaStructureExtractor(max_items_per_array=10)
    schema = extractor.extract(data)
    preview = _extract_preview_rows(data)
    issues = _detect_issues(preview)
    return {"schema": schema, "preview": preview, "issues": issues}


def execute_mapping(
    payload: Any,
    mapping_spec: Optional[Dict[str, Any]],
    target_schema: Any,
) -> Dict[str, Any]:
    roaster_mapping = _prepare_roaster_mapping(mapping_spec, payload, target_schema)
    flattened = _extract_target_paths(target_schema)
    target_paths = [
        _normalize_target_path(path)
        for path in flattened.keys()
        if isinstance(path, str) and ".items[]" in path
    ]

    roaster_mapping, _ = repair_mapping_spec(roaster_mapping, allowed_targets=set(target_paths))
    if not roaster_mapping:
        raise ValueError("Unable to build mapping spec.")
    validation_errors = validate_mapping_spec(roaster_mapping)
    if validation_errors:
        raise ValueError("; ".join(validation_errors))

    executor = MappingExecutor(roaster_mapping, canonical_schema_paths=target_paths)
    result = executor.execute(payload)
    return result
