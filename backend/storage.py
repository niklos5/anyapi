from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError


S3_BUCKET = os.environ.get("ANYAPI_S3_BUCKET", "")


@dataclass
class SchemaRecord:
    id: str
    name: str
    partner_id: str
    schema_definition: Any
    default_mapping: Optional[Dict[str, Any]]
    created_at: str
    updated_at: str
    version: int = 1
    api_key: Optional[str] = None


@dataclass
class JobRecord:
    id: str
    name: str
    source_type: str
    partner_id: str
    status: str
    created_at: str
    data: Any
    mapping: Dict[str, Any]
    target_schema: Any
    schema_id: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    issues: List[Dict[str, Any]] = field(default_factory=list)


_JOBS: Dict[str, JobRecord] = {}
_SCHEMAS: Dict[str, SchemaRecord] = {}
_S3_CLIENT: Optional[Any] = None


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M")


def _s3_enabled() -> bool:
    return bool(S3_BUCKET)


def _s3_client() -> Any:
    global _S3_CLIENT
    if _S3_CLIENT is None:
        _S3_CLIENT = boto3.client("s3")
    return _S3_CLIENT


def _schema_key(partner_id: str, schema_id: str) -> str:
    return f"schemas/{partner_id}/{schema_id}.json"


def _schema_index_key(schema_id: str) -> str:
    return f"schemas_by_id/{schema_id}.json"


def _schema_api_key_key(api_key: str) -> str:
    return f"schema_api_keys/{api_key}.json"


def _job_key(partner_id: str, job_id: str) -> str:
    return f"jobs/{partner_id}/{job_id}.json"


def _put_json(key: str, payload: Dict[str, Any]) -> None:
    if not _s3_enabled():
        return
    _s3_client().put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )


def _get_json(key: str) -> Optional[Dict[str, Any]]:
    if not _s3_enabled():
        return None
    try:
        response = _s3_client().get_object(Bucket=S3_BUCKET, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return None
        raise
    raw = response["Body"].read().decode("utf-8")
    return json.loads(raw)


def _delete_key(key: str) -> None:
    if not _s3_enabled():
        return
    _s3_client().delete_object(Bucket=S3_BUCKET, Key=key)


def _list_keys(prefix: str) -> List[str]:
    if not _s3_enabled():
        return []
    client = _s3_client()
    keys: List[str] = []
    continuation: Optional[str] = None
    while True:
        params = {"Bucket": S3_BUCKET, "Prefix": prefix}
        if continuation:
            params["ContinuationToken"] = continuation
        response = client.list_objects_v2(**params)
        for item in response.get("Contents", []):
            key = item.get("Key")
            if key:
                keys.append(key)
        if not response.get("IsTruncated"):
            break
        continuation = response.get("NextContinuationToken")
    return keys


def _schema_to_payload(record: SchemaRecord) -> Dict[str, Any]:
    return {
        "id": record.id,
        "name": record.name,
        "partner_id": record.partner_id,
        "schema_definition": record.schema_definition,
        "default_mapping": record.default_mapping,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "version": record.version,
        "api_key": record.api_key,
    }


def _payload_to_schema(payload: Dict[str, Any]) -> SchemaRecord:
    return SchemaRecord(
        id=str(payload["id"]),
        name=str(payload["name"]),
        partner_id=str(payload["partner_id"]),
        schema_definition=payload.get("schema_definition"),
        default_mapping=payload.get("default_mapping"),
        created_at=str(payload.get("created_at", _utc_now())),
        updated_at=str(payload.get("updated_at", _utc_now())),
        version=int(payload.get("version", 1)),
        api_key=payload.get("api_key"),
    )


def _job_to_payload(record: JobRecord) -> Dict[str, Any]:
    return {
        "id": record.id,
        "name": record.name,
        "source_type": record.source_type,
        "partner_id": record.partner_id,
        "status": record.status,
        "created_at": record.created_at,
        "data": record.data,
        "mapping": record.mapping,
        "target_schema": record.target_schema,
        "schema_id": record.schema_id,
        "result": record.result,
        "issues": record.issues,
    }


def _payload_to_job(payload: Dict[str, Any]) -> JobRecord:
    return JobRecord(
        id=str(payload["id"]),
        name=str(payload["name"]),
        source_type=str(payload["source_type"]),
        partner_id=str(payload["partner_id"]),
        status=str(payload["status"]),
        created_at=str(payload.get("created_at", _utc_now())),
        data=payload.get("data"),
        mapping=payload.get("mapping") or {},
        target_schema=payload.get("target_schema"),
        schema_id=payload.get("schema_id"),
        result=payload.get("result"),
        issues=payload.get("issues") or [],
    )


def create_schema(
    name: str,
    partner_id: str,
    schema_definition: Any,
    default_mapping: Optional[Dict[str, Any]] = None,
    api_key: Optional[str] = None,
) -> SchemaRecord:
    schema_id = f"schema_{uuid4().hex[:6]}"
    now = _utc_now()
    record = SchemaRecord(
        id=schema_id,
        name=name,
        partner_id=partner_id,
        schema_definition=schema_definition,
        default_mapping=default_mapping,
        created_at=now,
        updated_at=now,
        api_key=api_key,
    )
    _SCHEMAS[schema_id] = record
    _put_json(_schema_key(partner_id, schema_id), _schema_to_payload(record))
    _put_json(
        _schema_index_key(schema_id),
        {"partner_id": partner_id, "schema_id": schema_id, "api_key": api_key},
    )
    if api_key:
        _put_json(
            _schema_api_key_key(api_key),
            {"partner_id": partner_id, "schema_id": schema_id, "api_key": api_key},
        )
    return record


def update_schema(schema_id: str, partner_id: str, **kwargs: Any) -> Optional[SchemaRecord]:
    record = get_schema(schema_id, partner_id)
    if not record:
        return None
    for key, value in kwargs.items():
        setattr(record, key, value)
    record.updated_at = _utc_now()
    record.version += 1
    _SCHEMAS[schema_id] = record
    _put_json(_schema_key(partner_id, schema_id), _schema_to_payload(record))
    return record


def delete_schema(schema_id: str, partner_id: str) -> bool:
    record = get_schema(schema_id, partner_id)
    if not record:
        return False
    _SCHEMAS.pop(schema_id, None)
    _delete_key(_schema_key(partner_id, schema_id))
    _delete_key(_schema_index_key(schema_id))
    if record.api_key:
        _delete_key(_schema_api_key_key(record.api_key))
    return True


def get_schema(schema_id: str, partner_id: str) -> Optional[SchemaRecord]:
    record = _SCHEMAS.get(schema_id)
    if record and record.partner_id == partner_id:
        return record
    payload = _get_json(_schema_key(partner_id, schema_id))
    if not payload:
        return None
    record = _payload_to_schema(payload)
    _SCHEMAS[schema_id] = record
    return record


def get_schema_by_id(schema_id: str) -> Optional[SchemaRecord]:
    payload = _get_json(_schema_index_key(schema_id))
    if not payload:
        return None
    partner_id = payload.get("partner_id")
    if not isinstance(partner_id, str):
        return None
    return get_schema(schema_id, partner_id)


def get_schema_by_api_key(api_key: str) -> Optional[SchemaRecord]:
    payload = _get_json(_schema_api_key_key(api_key))
    if not payload:
        return None
    schema_id = payload.get("schema_id")
    partner_id = payload.get("partner_id")
    if not isinstance(schema_id, str) or not isinstance(partner_id, str):
        return None
    return get_schema(schema_id, partner_id)


def list_schemas(partner_id: str) -> List[SchemaRecord]:
    records: List[SchemaRecord] = []
    if _s3_enabled():
        keys = _list_keys(f"schemas/{partner_id}/")
        for key in keys:
            payload = _get_json(key)
            if payload:
                record = _payload_to_schema(payload)
                _SCHEMAS[record.id] = record
                records.append(record)
    else:
        records = [schema for schema in _SCHEMAS.values() if schema.partner_id == partner_id]
    return sorted(records, key=lambda item: item.updated_at, reverse=True)


def create_job(
    name: str,
    source_type: str,
    partner_id: str,
    data: Any,
    mapping: Dict[str, Any],
    target_schema: Any,
    schema_id: Optional[str] = None,
) -> JobRecord:
    job_id = f"job_{uuid4().hex[:6]}"
    record = JobRecord(
        id=job_id,
        name=name,
        source_type=source_type,
        partner_id=partner_id,
        status="processing",
        created_at=_utc_now(),
        data=data,
        mapping=mapping,
        target_schema=target_schema,
        schema_id=schema_id,
    )
    _JOBS[job_id] = record
    _put_json(_job_key(partner_id, job_id), _job_to_payload(record))
    return record


def update_job(job_id: str, partner_id: Optional[str] = None, **kwargs: Any) -> Optional[JobRecord]:
    record = _JOBS.get(job_id)
    if not record and partner_id:
        record = get_job(job_id, partner_id)
    if not record:
        return None
    for key, value in kwargs.items():
        setattr(record, key, value)
    _JOBS[job_id] = record
    _put_json(_job_key(record.partner_id, job_id), _job_to_payload(record))
    return record


def list_jobs(partner_id: str) -> List[JobRecord]:
    records: List[JobRecord] = []
    if _s3_enabled():
        keys = _list_keys(f"jobs/{partner_id}/")
        for key in keys:
            payload = _get_json(key)
            if payload:
                record = _payload_to_job(payload)
                _JOBS[record.id] = record
                records.append(record)
    else:
        records = [job for job in _JOBS.values() if job.partner_id == partner_id]
    return sorted(records, key=lambda item: item.created_at, reverse=True)


def get_job(job_id: str, partner_id: str) -> Optional[JobRecord]:
    job = _JOBS.get(job_id)
    if job and job.partner_id == partner_id:
        return job
    payload = _get_json(_job_key(partner_id, job_id))
    if not payload:
        return None
    job = _payload_to_job(payload)
    _JOBS[job_id] = job
    return job
