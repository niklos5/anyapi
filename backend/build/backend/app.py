from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import Depends, FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from auth import require_auth
from mapping_executor import flatten_target_schema
from roaster_mapping_executor import MappingExecutor
from roaster_mapping_repair import repair_mapping_spec
from roaster_mapping_validator import validate_mapping_spec
from schema_fingerprint import SchemaStructureExtractor
from storage import (
    create_job,
    create_schema,
    delete_schema,
    get_job,
    get_schema,
    get_schema_by_api_key,
    get_schema_by_id,
    list_jobs,
    list_schemas,
    update_job,
    update_schema,
)


class AnalyzeRequest(BaseModel):
    data: Any


class MappingSpec(BaseModel):
    targetSchema: Optional[Any] = None
    mappings: Any = Field(default_factory=list)
    defaults: Optional[Dict[str, Any]] = None


class MappingAgentOptions(BaseModel):
    enabled: bool = False
    maxIterations: Optional[int] = Field(default=None, ge=1, le=5)


class CreateJobRequest(BaseModel):
    name: str
    sourceType: str
    data: Any
    mapping: MappingSpec
    mappingAgent: Optional[MappingAgentOptions] = None


class DeploySchemaRequest(BaseModel):
    name: str
    schemaDefinition: Optional[Any] = None
    schemaSample: Optional[Any] = None
    defaultMapping: Optional[MappingSpec] = None
    metadata: Optional[Dict[str, Any]] = None


class UpdateSchemaRequest(BaseModel):
    name: Optional[str] = None
    schemaDefinition: Optional[Any] = None
    defaultMapping: Optional[MappingSpec] = None
    metadata: Optional[Dict[str, Any]] = None


class IngestSchemaRequest(BaseModel):
    data: Any
    name: Optional[str] = None
    sourceType: Optional[str] = None
    mapping: Optional[MappingSpec] = None
    mappingAgent: Optional[MappingAgentOptions] = None


app = FastAPI(title="AnyApi Roaster Service", version="0.1.0")
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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


def _build_bedrock_refinement_prompt(
    *,
    input_schema: Dict[str, Any],
    target_schema: Any,
    items_path: str,
    mapping_spec: Dict[str, Any],
    issues: Dict[str, Any],
    input_preview: List[Dict[str, Any]],
    output_preview: List[Dict[str, Any]],
) -> str:
    return (
        "You are an expert data mapper. Improve the existing JSON mapping spec in the roaster format.\n"
        "Return ONLY valid JSON (no markdown, no extra text).\n\n"
        "Rules:\n"
        "- The output must be a JSON object with keys: version, defaults, broadcast, mappings.\n"
        f"- mappings.items.path must be the JSONPath array: \"{items_path}\".\n"
        "- mappings.items.map should map target fields to source paths.\n"
        "- Use JSONPath strings that start with '$.' for sources.\n"
        "- If you cannot find a source for a target, set source to null.\n"
        "- Do not invent fields that are not in the target schema.\n\n"
        "Input schema (JSONPath -> type):\n"
        f"{json.dumps(input_schema, indent=2)}\n\n"
        "Target schema (JSON or JSONPath map):\n"
        f"{json.dumps(target_schema, indent=2)}\n\n"
        "Current mapping spec:\n"
        f"{json.dumps(mapping_spec, indent=2)}\n\n"
        "Detected issues:\n"
        f"{json.dumps(issues, indent=2)}\n\n"
        "Sample input rows:\n"
        f"{json.dumps(input_preview, indent=2)}\n\n"
        "Sample output rows:\n"
        f"{json.dumps(output_preview, indent=2)}\n"
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


def _parse_mapping_agent_options(options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    enabled_env = os.getenv("MAPPING_AGENT_ENABLED", "").strip().lower() in {"1", "true", "yes"}
    max_env = os.getenv("MAPPING_AGENT_MAX_ITERATIONS")
    max_env_value = int(max_env) if max_env and max_env.isdigit() else 3
    if not isinstance(options, dict):
        return {"enabled": enabled_env, "max_iterations": max_env_value}
    enabled = options.get("enabled", enabled_env)
    max_iterations = options.get("maxIterations", options.get("max_iterations", max_env_value))
    try:
        max_iterations = int(max_iterations)
    except (TypeError, ValueError):
        max_iterations = max_env_value
    max_iterations = max(1, min(max_iterations, 5))
    return {"enabled": bool(enabled), "max_iterations": max_iterations}


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


def _generate_mapping_with_agent(
    *,
    mapping_spec: Optional[Dict[str, Any]],
    payload: Any,
    target_schema: Any,
    target_paths: List[str],
    options: Dict[str, Any],
) -> Dict[str, Any]:
    items_path = _choose_items_path(payload)
    base_mapping: Optional[Dict[str, Any]] = None
    logger.info(
        "Mapping agent starting (max_iterations=%s)",
        options.get("max_iterations"),
    )

    if mapping_spec and mapping_spec.get("mappings"):
        mappings_value = mapping_spec.get("mappings")
        if isinstance(mappings_value, list):
            base_mapping = _build_roaster_mapping_from_list(mapping_spec, payload)
        elif isinstance(mappings_value, dict):
            base_mapping = mapping_spec
    if base_mapping is None:
        generated = _generate_mapping_with_bedrock(payload, target_schema)
        base_mapping = generated if generated else _auto_mapping_spec(payload, target_schema)

    input_schema = SchemaStructureExtractor(max_items_per_array=10).extract(payload)
    preview_rows = _extract_preview_rows(payload)
    current_mapping = base_mapping

    for _ in range(options["max_iterations"]):
        logger.info("Mapping agent iteration start")
        current_mapping, _ = repair_mapping_spec(
            current_mapping, allowed_targets=set(target_paths)
        )
        if not current_mapping:
            current_mapping = _auto_mapping_spec(payload, target_schema)
        issues, result = _summarize_mapping_issues(
            mapping_spec=current_mapping,
            payload=payload,
            target_paths=target_paths,
        )
        if not _has_mapping_issues(issues):
            logger.info("Mapping agent converged with no remaining issues.")
            return current_mapping
        logger.info(
            "Mapping agent issues: validation=%s missing_sources=%s no_values=%s sparse=%s execution_error=%s",
            len(issues.get("validationErrors") or []),
            len(issues.get("missingSourceFields") or []),
            len(issues.get("fieldsWithNoValues") or []),
            len(issues.get("fieldsWithSparseValues") or []),
            bool(issues.get("executionError")),
        )
        if not _bedrock_model_id():
            logger.warning("Mapping agent stopping: BEDROCK_MODEL_ID not set.")
            return current_mapping

        prompt = _build_bedrock_refinement_prompt(
            input_schema=input_schema,
            target_schema=target_schema,
            items_path=items_path,
            mapping_spec=current_mapping,
            issues=issues,
            input_preview=preview_rows,
            output_preview=_extract_output_preview(result),
        )
        raw_text = _invoke_bedrock(prompt)
        if not raw_text:
            logger.warning("Mapping agent stopping: Bedrock returned empty response.")
            return current_mapping
        improved_mapping, _ = repair_mapping_spec(
            raw_text, allowed_targets=set(target_paths)
        )
        if not improved_mapping or improved_mapping == current_mapping:
            logger.info("Mapping agent stopping: no improvements returned.")
            return current_mapping
        current_mapping = improved_mapping

    logger.info("Mapping agent reached max iterations.")
    return current_mapping


def _prepare_roaster_mapping(
    mapping_spec: Optional[Dict[str, Any]],
    payload: Any,
    target_schema: Any,
    *,
    mapping_agent: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    flattened = _extract_target_paths(target_schema)
    target_paths = [
        _normalize_target_path(path)
        for path in flattened.keys()
        if isinstance(path, str) and ".items[]" in path
    ]
    agent_options = _parse_mapping_agent_options(mapping_agent)
    if agent_options["enabled"]:
        return _generate_mapping_with_agent(
            mapping_spec=mapping_spec,
            payload=payload,
            target_schema=target_schema,
            target_paths=target_paths,
            options=agent_options,
        )

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


def _extract_output_preview(result: Any, limit: int = 3) -> List[Dict[str, Any]]:
    if isinstance(result, dict):
        items = result.get("items")
        if isinstance(items, list):
            return [row for row in items if isinstance(row, dict)][:limit]
    return []


def _get_nested_value(data: Any, dotted_path: str) -> Any:
    if not isinstance(dotted_path, str):
        return None
    cursor: Any = data
    for part in dotted_path.split("."):
        if isinstance(cursor, list):
            return cursor
        if not isinstance(cursor, dict) or part not in cursor:
            return None
        cursor = cursor.get(part)
    return cursor


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    if value == "":
        return True
    if isinstance(value, list) and len(value) == 0:
        return True
    if isinstance(value, dict) and len(value) == 0:
        return True
    return False


def _collect_leaf_sources(map_block: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    leaves: Dict[str, Any] = {}
    for target_field, spec in map_block.items():
        if not isinstance(target_field, str) or not isinstance(spec, dict):
            continue
        target_path = f"{prefix}{target_field}" if prefix else target_field
        if "path" in spec and "map" in spec and isinstance(spec.get("map"), dict):
            leaves.update(_collect_leaf_sources(spec["map"], prefix=f"{target_path}."))
            continue
        leaves[target_path] = spec.get("source")
    return leaves


def _summarize_mapping_issues(
    *,
    mapping_spec: Dict[str, Any],
    payload: Any,
    target_paths: List[str],
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    issues: Dict[str, Any] = {
        "validationErrors": [],
        "missingSourceFields": [],
        "fieldsWithNoValues": [],
        "fieldsWithSparseValues": [],
        "executionError": None,
    }

    validation_errors = validate_mapping_spec(mapping_spec)
    if validation_errors:
        issues["validationErrors"] = validation_errors

    items_map = (
        mapping_spec.get("mappings", {}).get("items", {}).get("map", {})
        if isinstance(mapping_spec.get("mappings"), dict)
        else {}
    )
    if isinstance(items_map, dict):
        leaf_sources = _collect_leaf_sources(items_map)
        missing_sources = [
            field
            for field, sources in leaf_sources.items()
            if sources is None or (isinstance(sources, list) and not sources)
        ]
        issues["missingSourceFields"] = missing_sources[:40]

    try:
        executor = MappingExecutor(mapping_spec, canonical_schema_paths=target_paths)
        result = executor.execute(payload)
    except Exception as exc:
        issues["executionError"] = str(exc)
        return issues, None

    items = result.get("items") if isinstance(result, dict) else None
    if not isinstance(items, list) or not items:
        issues["executionError"] = "Mapping output has no items."
        return issues, result

    total = len(items)
    fields_with_no_values: List[str] = []
    fields_with_sparse_values: List[Dict[str, Any]] = []
    for target_path in target_paths:
        non_null = 0
        for item in items:
            value = _get_nested_value(item, target_path)
            if not _is_missing_value(value):
                non_null += 1
        if non_null == 0:
            fields_with_no_values.append(target_path)
        elif non_null < total:
            if non_null / total < 0.5:
                fields_with_sparse_values.append(
                    {"field": target_path, "nonNull": non_null, "total": total}
                )

    issues["fieldsWithNoValues"] = fields_with_no_values[:40]
    issues["fieldsWithSparseValues"] = fields_with_sparse_values[:40]
    return issues, result


def _has_mapping_issues(issues: Dict[str, Any]) -> bool:
    if issues.get("executionError"):
        return True
    for key in ("validationErrors", "missingSourceFields", "fieldsWithNoValues", "fieldsWithSparseValues"):
        if issues.get(key):
            return True
    return False

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


@app.post("/analyze")
def analyze_payload(
    request: AnalyzeRequest, claims: Dict[str, Any] = Depends(require_auth)
) -> Dict[str, Any]:
    extractor = SchemaStructureExtractor(max_items_per_array=10)
    schema = extractor.extract(request.data)
    preview = _extract_preview_rows(request.data)
    issues = _detect_issues(preview)
    return {"schema": schema, "preview": preview, "issues": issues}


@app.post("/schemas")
def deploy_schema(
    request: DeploySchemaRequest, claims: Dict[str, Any] = Depends(require_auth)
) -> Dict[str, Any]:
    partner_id = str(claims.get("partner_id"))
    default_mapping = request.defaultMapping.dict() if request.defaultMapping else None
    schema_definition = request.schemaDefinition
    if schema_definition is None and request.schemaSample is not None:
        extractor = SchemaStructureExtractor(max_items_per_array=10)
        schema_definition = extractor.extract(request.schemaSample)
    if schema_definition is None:
        raise HTTPException(status_code=400, detail="schemaDefinition or schemaSample is required.")
    api_key = f"api_{uuid4().hex}"
    record = create_schema(
        name=request.name,
        partner_id=partner_id,
        schema_definition=schema_definition,
        default_mapping=default_mapping,
        metadata=request.metadata,
        api_key=api_key,
    )
    return {
        "schema": {
            "id": record.id,
            "name": record.name,
            "schemaDefinition": record.schema_definition,
            "defaultMapping": record.default_mapping,
            "metadata": record.metadata,
            "createdAt": record.created_at,
            "updatedAt": record.updated_at,
            "version": record.version,
        },
        "apiKey": api_key,
    }


@app.get("/schemas")
def get_schemas(claims: Dict[str, Any] = Depends(require_auth)) -> Dict[str, Any]:
    partner_id = str(claims.get("partner_id"))
    return {
        "schemas": [
            {
                "id": schema.id,
                "name": schema.name,
                "schemaDefinition": schema.schema_definition,
                "defaultMapping": schema.default_mapping,
                "metadata": schema.metadata,
                "createdAt": schema.created_at,
                "updatedAt": schema.updated_at,
                "version": schema.version,
            }
            for schema in list_schemas(partner_id)
        ]
    }


@app.get("/schemas/{schema_id}")
def get_schema_detail(
    schema_id: str, claims: Dict[str, Any] = Depends(require_auth)
) -> Dict[str, Any]:
    partner_id = str(claims.get("partner_id"))
    record = get_schema(schema_id, partner_id)
    if not record:
        raise HTTPException(status_code=404, detail="Schema not found")
    return {
        "schema": {
            "id": record.id,
            "name": record.name,
            "schemaDefinition": record.schema_definition,
            "defaultMapping": record.default_mapping,
            "metadata": record.metadata,
            "createdAt": record.created_at,
            "updatedAt": record.updated_at,
            "version": record.version,
        }
    }


@app.put("/schemas/{schema_id}")
def put_schema_detail(
    schema_id: str,
    request: UpdateSchemaRequest,
    claims: Dict[str, Any] = Depends(require_auth),
) -> Dict[str, Any]:
    partner_id = str(claims.get("partner_id"))
    payload: Dict[str, Any] = {}
    if request.name is not None:
        payload["name"] = request.name
    if request.schemaDefinition is not None:
        payload["schema_definition"] = request.schemaDefinition
    if request.defaultMapping is not None:
        payload["default_mapping"] = request.defaultMapping.dict()
    if request.metadata is not None:
        payload["metadata"] = request.metadata
    record = update_schema(schema_id, partner_id, **payload)
    if not record:
        raise HTTPException(status_code=404, detail="Schema not found")
    return {
        "schema": {
            "id": record.id,
            "name": record.name,
            "schemaDefinition": record.schema_definition,
            "defaultMapping": record.default_mapping,
            "metadata": record.metadata,
            "createdAt": record.created_at,
            "updatedAt": record.updated_at,
            "version": record.version,
        }
    }


@app.delete("/schemas/{schema_id}")
def delete_schema_detail(
    schema_id: str, claims: Dict[str, Any] = Depends(require_auth)
) -> Dict[str, Any]:
    partner_id = str(claims.get("partner_id"))
    deleted = delete_schema(schema_id, partner_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Schema not found")
    return {"deleted": True}


@app.post("/schemas/{schema_id}/ingest")
def ingest_to_schema(
    schema_id: str,
    request: IngestSchemaRequest,
    authorization: str = Header(default=""),
    x_api_key: str = Header(default="", alias="x-api-key"),
) -> Dict[str, Any]:
    schema = None
    partner_id = None
    if authorization:
        claims = require_auth(authorization)
        partner_id = str(claims.get("partner_id"))
        schema = get_schema(schema_id, partner_id)
    elif x_api_key:
        schema = get_schema_by_api_key(x_api_key)
        if schema and schema.id != schema_id:
            schema = None
        if schema:
            partner_id = schema.partner_id
    else:
        raise HTTPException(status_code=401, detail="Missing authentication")

    if not schema:
        raise HTTPException(status_code=404, detail="Schema not found")

    mapping_spec = request.mapping.dict() if request.mapping else None
    if mapping_spec is None and schema.default_mapping:
        mapping_spec = schema.default_mapping

    target_schema = (mapping_spec or {}).get("targetSchema") or schema.schema_definition
    mapping_agent = request.mappingAgent.dict() if request.mappingAgent else None
    roaster_mapping = _prepare_roaster_mapping(
        mapping_spec,
        request.data,
        target_schema,
        mapping_agent=mapping_agent,
    )

    flattened = _extract_target_paths(target_schema)
    target_paths = [
        _normalize_target_path(path)
        for path in flattened.keys()
        if isinstance(path, str) and ".items[]" in path
    ]

    roaster_mapping, _ = repair_mapping_spec(roaster_mapping, allowed_targets=set(target_paths))
    if not roaster_mapping:
        raise HTTPException(status_code=400, detail="Unable to build mapping spec.")
    validation_errors = validate_mapping_spec(roaster_mapping)
    if validation_errors:
        raise HTTPException(status_code=400, detail="; ".join(validation_errors))

    executor = MappingExecutor(roaster_mapping, canonical_schema_paths=target_paths)
    result = executor.execute(request.data)

    record = create_job(
        name=request.name or f"Ingest {schema.name}",
        source_type=request.sourceType or "api",
        partner_id=partner_id,
        data=request.data,
        mapping=roaster_mapping,
        target_schema=target_schema,
        schema_id=schema_id,
    )
    update_job(record.id, partner_id=partner_id, status="completed", result=result)

    return {
        "job": {
            "id": record.id,
            "name": record.name,
            "sourceType": record.source_type,
            "status": "completed",
            "createdAt": record.created_at,
            "schemaId": schema_id,
        },
        "result": result,
    }


@app.post("/jobs")
def create_ingestion_job(
    request: CreateJobRequest,
    authorization: str = Header(default=""),
    x_api_key: str = Header(default="", alias="x-api-key"),
) -> Dict[str, Any]:
    mapping_spec = request.mapping.dict()
    target_schema = mapping_spec.get("targetSchema")

    schema_id = None
    partner_id = None
    if authorization:
        claims = require_auth(authorization)
        partner_id = str(claims.get("partner_id"))
    elif x_api_key:
        schema = get_schema_by_api_key(x_api_key)
        if schema:
            partner_id = schema.partner_id
            schema_id = schema.id
            if not target_schema:
                target_schema = schema.schema_definition
    else:
        raise HTTPException(status_code=401, detail="Missing authentication")

    if not partner_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    mapping_agent = request.mappingAgent.dict() if request.mappingAgent else None
    roaster_mapping = _prepare_roaster_mapping(
        mapping_spec,
        request.data,
        target_schema,
        mapping_agent=mapping_agent,
    )

    flattened = _extract_target_paths(target_schema)
    target_paths = [
        _normalize_target_path(path)
        for path in flattened.keys()
        if isinstance(path, str) and ".items[]" in path
    ]

    roaster_mapping, _ = repair_mapping_spec(roaster_mapping, allowed_targets=set(target_paths))
    if not roaster_mapping:
        raise HTTPException(status_code=400, detail="Unable to build mapping spec.")
    validation_errors = validate_mapping_spec(roaster_mapping)
    if validation_errors:
        raise HTTPException(status_code=400, detail="; ".join(validation_errors))

    executor = MappingExecutor(roaster_mapping, canonical_schema_paths=target_paths)
    result = executor.execute(request.data)

    record = create_job(
        name=request.name,
        source_type=request.sourceType,
        partner_id=partner_id,
        data=request.data,
        mapping=roaster_mapping,
        target_schema=target_schema,
        schema_id=schema_id,
    )
    update_job(record.id, partner_id=partner_id, status="completed", result=result)

    return {
        "job": {
            "id": record.id,
            "name": record.name,
            "sourceType": record.source_type,
            "status": "completed",
            "createdAt": record.created_at,
        },
        "result": result,
    }


@app.get("/jobs")
def get_jobs(claims: Dict[str, Any] = Depends(require_auth)) -> Dict[str, Any]:
    partner_id = str(claims.get("partner_id"))
    return {
        "jobs": [
            {
                "id": job.id,
                "name": job.name,
                "sourceType": job.source_type,
                "status": job.status,
                "createdAt": job.created_at,
            "schemaId": job.schema_id,
            }
            for job in list_jobs(partner_id)
        ]
    }


@app.get("/jobs/{job_id}")
def get_job_detail(
    job_id: str, claims: Dict[str, Any] = Depends(require_auth)
) -> Dict[str, Any]:
    partner_id = str(claims.get("partner_id"))
    job = get_job(job_id, partner_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "id": job.id,
        "name": job.name,
        "sourceType": job.source_type,
        "status": job.status,
        "createdAt": job.created_at,
        "schemaId": job.schema_id,
    }


@app.get("/jobs/{job_id}/results")
def get_job_results(
    job_id: str, claims: Dict[str, Any] = Depends(require_auth)
) -> Dict[str, Any]:
    partner_id = str(claims.get("partner_id"))
    job = get_job(job_id, partner_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if not job.result:
        raise HTTPException(status_code=404, detail="Results not ready")
    return {"jobId": job.id, "result": job.result}
