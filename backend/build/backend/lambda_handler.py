import base64
import json
import logging
import os
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from backend.lambdas.auth.common import get_bearer_token, verify_access_token

from backend import storage
from backend.mapping_service import analyze_payload, execute_mapping

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_LAMBDA_CLIENT: Optional[Any] = None


def _lambda_client() -> Any:
    global _LAMBDA_CLIENT
    if _LAMBDA_CLIENT is None:
        _LAMBDA_CLIENT = boto3.client("lambda")
    return _LAMBDA_CLIENT


# ---------- HTTP helpers ----------


def _get_origin(event: Dict[str, Any]) -> Optional[str]:
    headers = event.get("headers") or {}
    origin = headers.get("origin") or headers.get("Origin")
    if not origin:
        return None
    allowed = os.environ.get("ALLOWED_ORIGINS")
    if not allowed:
        return origin
    allowed_list = [item.strip() for item in allowed.split(",") if item.strip()]
    return origin if origin in allowed_list else None


def _response(
    status_code: int,
    body: Dict[str, Any],
    *,
    origin: Optional[str] = None,
) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
    }
    if origin:
        headers.update(
            {
                "Access-Control-Allow-Origin": origin,
                "Access-Control-Allow-Credentials": "true",
                "Access-Control-Allow-Headers": "Authorization,Content-Type,X-Api-Key,Idempotency-Key",
                "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
                "Access-Control-Max-Age": "0",
                "Vary": "Origin",
            }
        )
    return {
        "statusCode": status_code,
        "headers": headers,
        "body": json.dumps(body),
        "isBase64Encoded": False,
    }


def _parse_body(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        try:
            body = base64.b64decode(body).decode()
        except Exception:
            return None
    if not body:
        return {}
    try:
        return json.loads(body)
    except Exception:
        return None


def _header(event: Dict[str, Any], name: str) -> Optional[str]:
    headers = event.get("headers") or {}
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


def _normalize_path(event: Dict[str, Any]) -> str:
    raw_path = event.get("rawPath") or event.get("path") or ""
    stage = (event.get("requestContext") or {}).get("stage")
    if stage:
        prefix = f"/{stage}"
        if raw_path.startswith(prefix + "/"):
            return raw_path[len(prefix):]
    return raw_path


# ---------- Auth helpers ----------


def _require_auth(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    token = get_bearer_token(event)
    if not token:
        return None
    secret = os.environ.get("JWT_SECRET")
    if not secret:
        return None
    claims = verify_access_token(token, secret)
    if not claims:
        return None
    if not claims.get("partner_id"):
        return None
    return claims


# ---------- Handlers ----------


def _handle_analyze(event: Dict[str, Any], origin: Optional[str]) -> Dict[str, Any]:
    claims = _require_auth(event)
    if not claims:
        return _response(401, {"error": "Unauthorized"}, origin=origin)
    payload = _parse_body(event)
    if payload is None:
        return _response(400, {"error": "Invalid JSON"}, origin=origin)
    if "data" not in payload:
        return _response(400, {"error": "Missing data"}, origin=origin)
    result = analyze_payload(payload["data"])
    return _response(200, result, origin=origin)


def _schema_to_dict(record: storage.SchemaRecord) -> Dict[str, Any]:
    return {
        "id": record.id,
        "name": record.name,
        "schemaDefinition": record.schema_definition,
        "defaultMapping": record.default_mapping,
        "metadata": record.metadata,
        "createdAt": record.created_at,
        "updatedAt": record.updated_at,
        "version": record.version,
    }


def _job_to_dict(record: storage.JobRecord) -> Dict[str, Any]:
    return {
        "id": record.id,
        "name": record.name,
        "sourceType": record.source_type,
        "status": record.status,
        "createdAt": record.created_at,
        "schemaId": record.mapping_id,
    }


def _handle_list_schemas(event: Dict[str, Any], origin: Optional[str]) -> Dict[str, Any]:
    claims = _require_auth(event)
    if not claims:
        return _response(401, {"error": "Unauthorized"}, origin=origin)
    partner_internal_id = claims.get("partner_internal_id")
    if partner_internal_id is None:
        return _response(401, {"error": "Missing tenant"}, origin=origin)
    records = storage.list_schemas(int(partner_internal_id))
    return _response(200, {"schemas": [_schema_to_dict(r) for r in records]}, origin=origin)


def _handle_create_schema(event: Dict[str, Any], origin: Optional[str]) -> Dict[str, Any]:
    claims = _require_auth(event)
    if not claims:
        return _response(401, {"error": "Unauthorized"}, origin=origin)
    partner_internal_id = claims.get("partner_internal_id")
    if partner_internal_id is None:
        return _response(401, {"error": "Missing tenant"}, origin=origin)
    payload = _parse_body(event)
    if payload is None:
        return _response(400, {"error": "Invalid JSON"}, origin=origin)
    name = payload.get("name")
    schema_definition = payload.get("schemaDefinition")
    schema_sample = payload.get("schemaSample")
    default_mapping = payload.get("defaultMapping")
    metadata = payload.get("metadata")
    if not name:
        return _response(400, {"error": "Missing name"}, origin=origin)
    if schema_definition is None and schema_sample is not None:
        schema_definition = analyze_payload(schema_sample)["schema"]
    if schema_definition is None:
        return _response(400, {"error": "schemaDefinition or schemaSample required"}, origin=origin)
    record = storage.create_schema(
        name=name,
        partner_internal_id=int(partner_internal_id),
        schema_definition=schema_definition,
        default_mapping=default_mapping,
        metadata=metadata,
    )
    return _response(200, {"schema": _schema_to_dict(record), "apiKey": record.api_key}, origin=origin)


def _handle_get_schema(event: Dict[str, Any], origin: Optional[str], schema_id: str) -> Dict[str, Any]:
    claims = _require_auth(event)
    if not claims:
        return _response(401, {"error": "Unauthorized"}, origin=origin)
    partner_internal_id = claims.get("partner_internal_id")
    if partner_internal_id is None:
        return _response(401, {"error": "Missing tenant"}, origin=origin)
    record = storage.get_schema(schema_id=schema_id, partner_internal_id=int(partner_internal_id))
    if not record:
        return _response(404, {"error": "Schema not found"}, origin=origin)
    return _response(200, {"schema": _schema_to_dict(record)}, origin=origin)


def _handle_update_schema(event: Dict[str, Any], origin: Optional[str], schema_id: str) -> Dict[str, Any]:
    claims = _require_auth(event)
    if not claims:
        return _response(401, {"error": "Unauthorized"}, origin=origin)
    partner_internal_id = claims.get("partner_internal_id")
    if partner_internal_id is None:
        return _response(401, {"error": "Missing tenant"}, origin=origin)
    payload = _parse_body(event)
    if payload is None:
        return _response(400, {"error": "Invalid JSON"}, origin=origin)
    record = storage.update_schema(
        schema_id=schema_id,
        partner_internal_id=int(partner_internal_id),
        name=payload.get("name"),
        schema_definition=payload.get("schemaDefinition"),
        default_mapping=payload.get("defaultMapping"),
        metadata=payload.get("metadata"),
    )
    if not record:
        return _response(404, {"error": "Schema not found"}, origin=origin)
    return _response(200, {"schema": _schema_to_dict(record)}, origin=origin)


def _handle_delete_schema(event: Dict[str, Any], origin: Optional[str], schema_id: str) -> Dict[str, Any]:
    claims = _require_auth(event)
    if not claims:
        return _response(401, {"error": "Unauthorized"}, origin=origin)
    partner_internal_id = claims.get("partner_internal_id")
    if partner_internal_id is None:
        return _response(401, {"error": "Missing tenant"}, origin=origin)
    deleted = storage.delete_schema(schema_id=schema_id, partner_internal_id=int(partner_internal_id))
    if not deleted:
        return _response(404, {"error": "Schema not found"}, origin=origin)
    return _response(200, {"deleted": True}, origin=origin)


def _handle_ingest_schema(event: Dict[str, Any], origin: Optional[str], schema_id: str) -> Dict[str, Any]:
    claims = _require_auth(event)
    schema = None
    partner_internal_id = None
    if claims:
        partner_internal_id = claims.get("partner_internal_id")
        if partner_internal_id is not None:
            schema = storage.get_schema(schema_id=schema_id, partner_internal_id=int(partner_internal_id))
    if not schema:
        api_key = _header(event, "x-api-key")
        if api_key:
            schema = storage.get_schema_by_api_key(api_key)
            if schema and schema.id != schema_id:
                schema = None
            if schema:
                partner_internal_id = schema.partner_internal_id
    if not schema or partner_internal_id is None:
        return _response(401, {"error": "Unauthorized"}, origin=origin)

    payload = _parse_body(event)
    if payload is None:
        return _response(400, {"error": "Invalid JSON"}, origin=origin)
    data = payload.get("data")
    if data is None:
        return _response(400, {"error": "Missing data"}, origin=origin)

    idempotency_key = _header(event, "Idempotency-Key")
    if idempotency_key:
        existing_job_id = storage.get_idempotency_job_id(
            partner_internal_id=int(partner_internal_id),
            mapping_id=schema.id,
            idempotency_key=idempotency_key,
        )
        if existing_job_id:
            job = storage.get_job(existing_job_id, int(partner_internal_id))
            if job:
                result = storage.get_job_result(job)
                return _response(
                    200,
                    {"job": _job_to_dict(job), "result": result},
                    origin=origin,
                )

    mapping_spec = payload.get("mapping")
    if mapping_spec is None and schema.default_mapping:
        mapping_spec = schema.default_mapping
    mapping_agent = payload.get("mappingAgent")
    target_schema = (mapping_spec or {}).get("targetSchema") if isinstance(mapping_spec, dict) else None
    if target_schema is None:
        target_schema = schema.schema_definition

    input_key, input_checksum = storage.store_input_payload(
        partner_internal_id=int(partner_internal_id),
        mapping_id=schema.id,
        payload=data,
    )

    job_created = storage.create_job(
        name=payload.get("name") or f"Ingest {schema.name}",
        partner_internal_id=int(partner_internal_id),
        mapping_id=schema.id,
        source_type=payload.get("sourceType") or "api",
        status="processing",
        input_s3_key=input_key,
        input_checksum=input_checksum,
        metrics={
            "mapping_spec": mapping_spec,
            "target_schema": target_schema,
            "mapping_agent": mapping_agent if isinstance(mapping_agent, dict) else None,
        },
    )

    if idempotency_key:
        storage.store_idempotency_key(
            partner_internal_id=int(partner_internal_id),
            mapping_id=schema.id,
            idempotency_key=idempotency_key,
            job_id=job_created.id,
        )

    try:
        _invoke_async_job(job_created.id, int(partner_internal_id))
    except (BotoCoreError, ClientError, RuntimeError) as exc:
        storage.update_job(
            job_id=job_created.id,
            partner_internal_id=int(partner_internal_id),
            status="failed",
            issues=[{"field": "async", "message": str(exc)}],
        )
        return _response(500, {"error": str(exc)}, origin=origin)

    return _response(
        202,
        {"job": _job_to_dict(job_created)},
        origin=origin,
    )


def _handle_create_job(event: Dict[str, Any], origin: Optional[str]) -> Dict[str, Any]:
    claims = _require_auth(event)
    if not claims:
        return _response(401, {"error": "Unauthorized"}, origin=origin)
    partner_internal_id = claims.get("partner_internal_id")
    if partner_internal_id is None:
        return _response(401, {"error": "Missing tenant"}, origin=origin)
    payload = _parse_body(event)
    if payload is None:
        return _response(400, {"error": "Invalid JSON"}, origin=origin)
    mapping_spec = payload.get("mapping")
    mapping_agent = payload.get("mappingAgent")
    target_schema = (mapping_spec or {}).get("targetSchema") if isinstance(mapping_spec, dict) else None
    data = payload.get("data")
    if data is None:
        return _response(400, {"error": "Missing data"}, origin=origin)

    mapping_id = payload.get("mappingId")
    if not mapping_id:
        return _response(400, {"error": "Missing mappingId"}, origin=origin)

    if target_schema is None:
        schema = storage.get_schema(schema_id=mapping_id, partner_internal_id=int(partner_internal_id))
        if schema:
            target_schema = schema.schema_definition

    input_key, input_checksum = storage.store_input_payload(
        partner_internal_id=int(partner_internal_id),
        mapping_id=mapping_id,
        payload=data,
    )
    job = storage.create_job(
        name=payload.get("name") or "Ingestion job",
        partner_internal_id=int(partner_internal_id),
        mapping_id=mapping_id,
        source_type=payload.get("sourceType") or "api",
        status="processing",
        input_s3_key=input_key,
        input_checksum=input_checksum,
        metrics={
            "mapping_spec": mapping_spec,
            "target_schema": target_schema,
            "mapping_agent": mapping_agent if isinstance(mapping_agent, dict) else None,
        },
    )
    try:
        _invoke_async_job(job.id, int(partner_internal_id))
    except (BotoCoreError, ClientError, RuntimeError) as exc:
        storage.update_job(
            job_id=job.id,
            partner_internal_id=int(partner_internal_id),
            status="failed",
            issues=[{"field": "async", "message": str(exc)}],
        )
        return _response(500, {"error": str(exc)}, origin=origin)

    return _response(202, {"job": _job_to_dict(job)}, origin=origin)


def _handle_list_jobs(event: Dict[str, Any], origin: Optional[str]) -> Dict[str, Any]:
    claims = _require_auth(event)
    if not claims:
        return _response(401, {"error": "Unauthorized"}, origin=origin)
    partner_internal_id = claims.get("partner_internal_id")
    if partner_internal_id is None:
        return _response(401, {"error": "Missing tenant"}, origin=origin)
    jobs = storage.list_jobs(int(partner_internal_id))
    return _response(200, {"jobs": [_job_to_dict(job) for job in jobs]}, origin=origin)


def _handle_get_job(event: Dict[str, Any], origin: Optional[str], job_id: str) -> Dict[str, Any]:
    claims = _require_auth(event)
    if not claims:
        return _response(401, {"error": "Unauthorized"}, origin=origin)
    partner_internal_id = claims.get("partner_internal_id")
    if partner_internal_id is None:
        return _response(401, {"error": "Missing tenant"}, origin=origin)
    job = storage.get_job(job_id, int(partner_internal_id))
    if not job:
        return _response(404, {"error": "Job not found"}, origin=origin)
    return _response(200, _job_to_dict(job), origin=origin)


def _handle_get_job_results(event: Dict[str, Any], origin: Optional[str], job_id: str) -> Dict[str, Any]:
    claims = _require_auth(event)
    if not claims:
        return _response(401, {"error": "Unauthorized"}, origin=origin)
    partner_internal_id = claims.get("partner_internal_id")
    if partner_internal_id is None:
        return _response(401, {"error": "Missing tenant"}, origin=origin)
    job = storage.get_job(job_id, int(partner_internal_id))
    if not job:
        return _response(404, {"error": "Job not found"}, origin=origin)
    result = storage.get_job_result(job)
    if result is None:
        return _response(404, {"error": "Results not ready"}, origin=origin)
    return _response(200, {"jobId": job.id, "result": result}, origin=origin)


def _invoke_async_job(job_id: str, partner_internal_id: int) -> None:
    function_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
    if not function_name:
        raise RuntimeError("AWS_LAMBDA_FUNCTION_NAME not configured")
    payload = {
        "action": "process_job",
        "jobId": job_id,
        "partnerInternalId": partner_internal_id,
    }
    _lambda_client().invoke(
        FunctionName=function_name,
        InvocationType="Event",
        Payload=json.dumps(payload).encode("utf-8"),
    )


def _process_job(job_id: str, partner_internal_id: int) -> None:
    job = storage.get_job(job_id, partner_internal_id)
    if not job:
        raise RuntimeError("Job not found")
    schema = storage.get_schema(schema_id=job.mapping_id, partner_internal_id=partner_internal_id)
    if not schema:
        raise RuntimeError("Schema not found")
    if not job.input_s3_key:
        raise RuntimeError("Job input missing")

    metrics = job.metrics or {}
    mapping_spec = metrics.get("mapping_spec")
    target_schema = metrics.get("target_schema") or schema.schema_definition
    mapping_agent = metrics.get("mapping_agent")

    payload = storage.load_payload_from_s3(job.input_s3_key)
    result = execute_mapping(
        payload,
        mapping_spec,
        target_schema,
        mapping_agent=mapping_agent if isinstance(mapping_agent, dict) else None,
    )
    result_key, result_checksum = storage.store_result_payload(
        partner_internal_id=partner_internal_id,
        mapping_id=job.mapping_id,
        payload=result,
    )
    storage.update_job(
        job_id=job.id,
        partner_internal_id=partner_internal_id,
        status="completed",
        result_s3_key=result_key,
        result_checksum=result_checksum,
    )


# ---------- Lambda entry ----------


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    origin = _get_origin(event)
    try:
        if event.get("action") == "process_job":
            job_id = event.get("jobId")
            partner_internal_id = event.get("partnerInternalId")
            if not job_id or partner_internal_id is None:
                raise RuntimeError("Missing jobId or partnerInternalId")
            _process_job(str(job_id), int(partner_internal_id))
            return {"statusCode": 200, "body": "ok"}
        path = _normalize_path(event)
        method = (event.get("requestContext") or {}).get("http", {}).get("method")
        if not method:
            method = event.get("httpMethod")
        method = (method or "GET").upper()

        request_id = (event.get("requestContext") or {}).get("requestId")
        logger.info(
            "request",
            extra={
                "request_id": request_id,
                "path": path,
                "method": method,
            },
        )

        if method == "OPTIONS":
            return _response(204, {}, origin=origin)

        segments = [segment for segment in path.strip("/").split("/") if segment]

        if not segments:
            return _response(404, {"error": "Not found"}, origin=origin)

        if segments[0] == "analyze" and method == "POST":
            return _handle_analyze(event, origin)

        if segments[0] == "schemas":
            if len(segments) == 1:
                if method == "GET":
                    return _handle_list_schemas(event, origin)
                if method == "POST":
                    return _handle_create_schema(event, origin)
            if len(segments) == 2:
                schema_id = segments[1]
                if method == "GET":
                    return _handle_get_schema(event, origin, schema_id)
                if method == "PUT":
                    return _handle_update_schema(event, origin, schema_id)
                if method == "DELETE":
                    return _handle_delete_schema(event, origin, schema_id)
            if len(segments) == 3 and segments[2] == "ingest" and method == "POST":
                schema_id = segments[1]
                return _handle_ingest_schema(event, origin, schema_id)

        if segments[0] == "jobs":
            if len(segments) == 1:
                if method == "GET":
                    return _handle_list_jobs(event, origin)
                if method == "POST":
                    return _handle_create_job(event, origin)
            if len(segments) == 2 and method == "GET":
                return _handle_get_job(event, origin, segments[1])
            if len(segments) == 3 and segments[2] == "results" and method == "GET":
                return _handle_get_job_results(event, origin, segments[1])

        return _response(404, {"error": "Not found"}, origin=origin)
    except Exception as exc:
        logger.exception("Unhandled error in lambda handler")
        return _response(
            500,
            {"error": "Internal server error", "message": str(exc)},
            origin=origin,
        )
