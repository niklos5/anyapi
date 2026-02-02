from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional
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


class CreateJobRequest(BaseModel):
    name: str
    sourceType: str
    data: Any
    mapping: MappingSpec


class DeploySchemaRequest(BaseModel):
    name: str
    schemaDefinition: Optional[Any] = None
    schemaSample: Optional[Any] = None
    defaultMapping: Optional[MappingSpec] = None


class UpdateSchemaRequest(BaseModel):
    name: Optional[str] = None
    schemaDefinition: Optional[Any] = None
    defaultMapping: Optional[MappingSpec] = None


class IngestSchemaRequest(BaseModel):
    data: Any
    name: Optional[str] = None
    sourceType: Optional[str] = None
    mapping: Optional[MappingSpec] = None


app = FastAPI(title="AnyApi Roaster Service", version="0.1.0")

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
        api_key=api_key,
    )
    return {
        "schema": {
            "id": record.id,
            "name": record.name,
            "schemaDefinition": record.schema_definition,
            "defaultMapping": record.default_mapping,
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
    record = update_schema(schema_id, partner_id, **payload)
    if not record:
        raise HTTPException(status_code=404, detail="Schema not found")
    return {
        "schema": {
            "id": record.id,
            "name": record.name,
            "schemaDefinition": record.schema_definition,
            "defaultMapping": record.default_mapping,
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
    roaster_mapping = _prepare_roaster_mapping(mapping_spec, request.data, target_schema)

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
    roaster_mapping = _prepare_roaster_mapping(mapping_spec, request.data, target_schema)

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
