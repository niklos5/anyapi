from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import RealDictCursor, Json  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None
    RealDictCursor = None
    Json = None

try:
    import pg8000  # type: ignore
except ImportError:  # pragma: no cover
    pg8000 = None

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("ANYAPI_S3_BUCKET", "")
S3_KMS_KEY_ID = os.environ.get("S3_KMS_KEY_ID")
S3_SSE = os.environ.get("S3_SSE", "AES256")

_DB_CONN = None
_S3_CLIENT: Optional[Any] = None


@dataclass
class SchemaRecord:
    id: str
    name: str
    partner_internal_id: int
    schema_definition: Any
    default_mapping: Optional[Dict[str, Any]]
    metadata: Optional[Dict[str, Any]]
    created_at: str
    updated_at: str
    version: int
    api_key: Optional[str]


@dataclass
class JobRecord:
    id: str
    name: str
    partner_internal_id: int
    mapping_id: str
    source_type: str
    status: str
    input_s3_key: Optional[str]
    input_checksum: Optional[str]
    result_s3_key: Optional[str]
    result_checksum: Optional[str]
    issues: List[Dict[str, Any]]
    metrics: Optional[Dict[str, Any]]
    created_at: str
    updated_at: str


# ---------- DB helpers ----------


def _db_connection():
    global _DB_CONN
    if _DB_CONN is not None:
        try:
            if psycopg2 is not None and _DB_CONN.closed == 0:
                return _DB_CONN
        except Exception:
            _DB_CONN = None

    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        if psycopg2 is not None:
            _DB_CONN = psycopg2.connect(database_url)
        elif pg8000 is not None:
            _DB_CONN = pg8000.connect(database_url)
        else:
            raise RuntimeError("Missing database client (psycopg2 or pg8000).")
    else:
        host = os.environ.get("DB_HOST")
        user = os.environ.get("DB_USER")
        password = os.environ.get("DB_PASSWORD")
        db_name = os.environ.get("DB_NAME")
        port = int(os.environ.get("DB_PORT", "5432"))
        if not all([host, user, password, db_name]):
            raise RuntimeError("Missing DB_* environment variables.")
        if psycopg2 is not None:
            _DB_CONN = psycopg2.connect(
                host=host, user=user, password=password, port=port, dbname=db_name
            )
        elif pg8000 is not None:
            _DB_CONN = pg8000.connect(
                host=host, user=user, password=password, port=port, database=db_name
            )
        else:
            raise RuntimeError("Missing database client (psycopg2 or pg8000).")

    try:
        _DB_CONN.autocommit = True
    except Exception:
        pass
    return _DB_CONN


def _json_param(value: Any):
    if Json is not None:
        return Json(value)
    return json.dumps(value)


def _fetch_one(sql: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    conn = _db_connection()
    if psycopg2 is not None and RealDictCursor is not None:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
            return dict(row) if row else None
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        row = cursor.fetchone()
        if not row:
            return None
        columns = [desc[0] for desc in cursor.description]
        return {col: value for col, value in zip(columns, row)}


def _fetch_all(sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    conn = _db_connection()
    if psycopg2 is not None and RealDictCursor is not None:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall() or []
            return [dict(row) for row in rows]
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall() or []
        columns = [desc[0] for desc in cursor.description]
        return [{col: value for col, value in zip(columns, row)} for row in rows]


# ---------- S3 helpers ----------


def _s3_enabled() -> bool:
    return bool(S3_BUCKET)


def _s3_client() -> Any:
    global _S3_CLIENT
    if _S3_CLIENT is None:
        _S3_CLIENT = boto3.client("s3")
    return _S3_CLIENT


def _checksum(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _s3_put_json(prefix: str, payload: Any) -> Tuple[str, str]:
    if not _s3_enabled():
        raise RuntimeError("ANYAPI_S3_BUCKET not configured")
    key = f"{prefix}/{uuid4().hex}.json"
    raw = json.dumps(payload).encode("utf-8")
    checksum = _checksum(raw)
    params: Dict[str, Any] = {
        "Bucket": S3_BUCKET,
        "Key": key,
        "Body": raw,
        "ContentType": "application/json",
    }
    if S3_KMS_KEY_ID:
        params["ServerSideEncryption"] = "aws:kms"
        params["SSEKMSKeyId"] = S3_KMS_KEY_ID
    elif S3_SSE:
        params["ServerSideEncryption"] = S3_SSE
    _s3_client().put_object(**params)
    return key, checksum


def _s3_get_json(key: str) -> Any:
    if not _s3_enabled():
        raise RuntimeError("ANYAPI_S3_BUCKET not configured")
    response = _s3_client().get_object(Bucket=S3_BUCKET, Key=key)
    raw = response["Body"].read().decode("utf-8")
    return json.loads(raw)


# ---------- Row mapping ----------


def _schema_from_row(row: Dict[str, Any]) -> SchemaRecord:
    return SchemaRecord(
        id=str(row["id"]),
        name=str(row["name"]),
        partner_internal_id=int(row["partner_internal_id"]),
        schema_definition=row.get("schema_definition"),
        default_mapping=row.get("default_mapping"),
        metadata=row.get("metadata"),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        version=int(row["version"]),
        api_key=row.get("api_key"),
    )


def _job_from_row(row: Dict[str, Any]) -> JobRecord:
    return JobRecord(
        id=str(row["id"]),
        name=str(row["name"]),
        partner_internal_id=int(row["partner_internal_id"]),
        mapping_id=str(row["mapping_id"]),
        source_type=str(row["source_type"]),
        status=str(row["status"]),
        input_s3_key=row.get("input_s3_key"),
        input_checksum=row.get("input_checksum"),
        result_s3_key=row.get("result_s3_key"),
        result_checksum=row.get("result_checksum"),
        issues=row.get("issues") or [],
        metrics=row.get("metrics"),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


# ---------- Schema CRUD ----------


def create_schema(
    *,
    name: str,
    partner_internal_id: int,
    schema_definition: Any,
    default_mapping: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    api_key: Optional[str] = None,
) -> SchemaRecord:
    api_key = api_key or f"api_{uuid4().hex}"
    row = _fetch_one(
        """
        INSERT INTO anyapi_app.mappings (
            partner_internal_id,
            name,
            schema_definition,
            default_mapping,
            metadata,
            api_key
        ) VALUES (
            %(partner_internal_id)s,
            %(name)s,
            %(schema_definition)s,
            %(default_mapping)s,
            %(metadata)s,
            %(api_key)s
        )
        RETURNING *
        """,
        {
            "partner_internal_id": partner_internal_id,
            "name": name,
            "schema_definition": _json_param(schema_definition),
            "default_mapping": _json_param(default_mapping) if default_mapping else None,
            "metadata": _json_param(metadata) if metadata else None,
            "api_key": api_key,
        },
    )
    if not row:
        raise RuntimeError("Failed to create mapping")
    return _schema_from_row(row)


def update_schema(
    *,
    schema_id: str,
    partner_internal_id: int,
    name: Optional[str] = None,
    schema_definition: Optional[Any] = None,
    default_mapping: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[SchemaRecord]:
    row = _fetch_one(
        """
        UPDATE anyapi_app.mappings
        SET
            name = COALESCE(%(name)s, name),
            schema_definition = COALESCE(%(schema_definition)s, schema_definition),
            default_mapping = COALESCE(%(default_mapping)s, default_mapping),
            metadata = COALESCE(%(metadata)s, metadata),
            updated_at = NOW(),
            version = version + 1
        WHERE id = %(schema_id)s AND partner_internal_id = %(partner_internal_id)s
        RETURNING *
        """,
        {
            "schema_id": schema_id,
            "partner_internal_id": partner_internal_id,
            "name": name,
            "schema_definition": _json_param(schema_definition) if schema_definition is not None else None,
            "default_mapping": _json_param(default_mapping) if default_mapping is not None else None,
            "metadata": _json_param(metadata) if metadata is not None else None,
        },
    )
    return _schema_from_row(row) if row else None


def delete_schema(*, schema_id: str, partner_internal_id: int) -> bool:
    row = _fetch_one(
        """
        DELETE FROM anyapi_app.mappings
        WHERE id = %(schema_id)s AND partner_internal_id = %(partner_internal_id)s
        RETURNING id
        """,
        {"schema_id": schema_id, "partner_internal_id": partner_internal_id},
    )
    return bool(row)


def get_schema(*, schema_id: str, partner_internal_id: int) -> Optional[SchemaRecord]:
    row = _fetch_one(
        """
        SELECT * FROM anyapi_app.mappings
        WHERE id = %(schema_id)s AND partner_internal_id = %(partner_internal_id)s
        """,
        {"schema_id": schema_id, "partner_internal_id": partner_internal_id},
    )
    return _schema_from_row(row) if row else None


def get_schema_by_api_key(api_key: str) -> Optional[SchemaRecord]:
    row = _fetch_one(
        """
        SELECT * FROM anyapi_app.mappings
        WHERE api_key = %(api_key)s
        """,
        {"api_key": api_key},
    )
    return _schema_from_row(row) if row else None


def list_schemas(partner_internal_id: int) -> List[SchemaRecord]:
    rows = _fetch_all(
        """
        SELECT * FROM anyapi_app.mappings
        WHERE partner_internal_id = %(partner_internal_id)s
        ORDER BY updated_at DESC
        """,
        {"partner_internal_id": partner_internal_id},
    )
    return [_schema_from_row(row) for row in rows]


# ---------- Jobs ----------


def create_job(
    *,
    name: str,
    partner_internal_id: int,
    mapping_id: str,
    source_type: str,
    status: str = "processing",
    input_s3_key: Optional[str] = None,
    input_checksum: Optional[str] = None,
    result_s3_key: Optional[str] = None,
    result_checksum: Optional[str] = None,
    issues: Optional[List[Dict[str, Any]]] = None,
    metrics: Optional[Dict[str, Any]] = None,
) -> JobRecord:
    row = _fetch_one(
        """
        INSERT INTO anyapi_app.jobs (
            partner_internal_id,
            mapping_id,
            name,
            source_type,
            status,
            input_s3_key,
            input_checksum,
            result_s3_key,
            result_checksum,
            issues,
            metrics
        ) VALUES (
            %(partner_internal_id)s,
            %(mapping_id)s,
            %(name)s,
            %(source_type)s,
            %(status)s,
            %(input_s3_key)s,
            %(input_checksum)s,
            %(result_s3_key)s,
            %(result_checksum)s,
            %(issues)s,
            %(metrics)s
        )
        RETURNING *
        """,
        {
            "partner_internal_id": partner_internal_id,
            "mapping_id": mapping_id,
            "name": name,
            "source_type": source_type,
            "status": status,
            "input_s3_key": input_s3_key,
            "input_checksum": input_checksum,
            "result_s3_key": result_s3_key,
            "result_checksum": result_checksum,
            "issues": _json_param(issues or []),
            "metrics": _json_param(metrics) if metrics is not None else None,
        },
    )
    if not row:
        raise RuntimeError("Failed to create job")
    return _job_from_row(row)


def update_job(
    *,
    job_id: str,
    partner_internal_id: int,
    status: Optional[str] = None,
    result_s3_key: Optional[str] = None,
    result_checksum: Optional[str] = None,
    issues: Optional[List[Dict[str, Any]]] = None,
    metrics: Optional[Dict[str, Any]] = None,
) -> Optional[JobRecord]:
    row = _fetch_one(
        """
        UPDATE anyapi_app.jobs
        SET
            status = COALESCE(%(status)s, status),
            result_s3_key = COALESCE(%(result_s3_key)s, result_s3_key),
            result_checksum = COALESCE(%(result_checksum)s, result_checksum),
            issues = COALESCE(%(issues)s, issues),
            metrics = COALESCE(%(metrics)s, metrics),
            updated_at = NOW()
        WHERE id = %(job_id)s AND partner_internal_id = %(partner_internal_id)s
        RETURNING *
        """,
        {
            "job_id": job_id,
            "partner_internal_id": partner_internal_id,
            "status": status,
            "result_s3_key": result_s3_key,
            "result_checksum": result_checksum,
            "issues": _json_param(issues) if issues is not None else None,
            "metrics": _json_param(metrics) if metrics is not None else None,
        },
    )
    return _job_from_row(row) if row else None


def list_jobs(partner_internal_id: int) -> List[JobRecord]:
    rows = _fetch_all(
        """
        SELECT * FROM anyapi_app.jobs
        WHERE partner_internal_id = %(partner_internal_id)s
        ORDER BY created_at DESC
        """,
        {"partner_internal_id": partner_internal_id},
    )
    return [_job_from_row(row) for row in rows]


def get_job(job_id: str, partner_internal_id: int) -> Optional[JobRecord]:
    row = _fetch_one(
        """
        SELECT * FROM anyapi_app.jobs
        WHERE id = %(job_id)s AND partner_internal_id = %(partner_internal_id)s
        """,
        {"job_id": job_id, "partner_internal_id": partner_internal_id},
    )
    return _job_from_row(row) if row else None


def get_job_result(job: JobRecord) -> Optional[Any]:
    if not job.result_s3_key:
        return None
    return _s3_get_json(job.result_s3_key)


# ---------- Idempotency ----------


def get_idempotency_job_id(
    *,
    partner_internal_id: int,
    mapping_id: str,
    idempotency_key: str,
) -> Optional[str]:
    row = _fetch_one(
        """
        SELECT job_id FROM anyapi_app.idempotency_keys
        WHERE partner_internal_id = %(partner_internal_id)s
          AND mapping_id = %(mapping_id)s
          AND idempotency_key = %(idempotency_key)s
        """,
        {
            "partner_internal_id": partner_internal_id,
            "mapping_id": mapping_id,
            "idempotency_key": idempotency_key,
        },
    )
    return str(row["job_id"]) if row else None


def store_idempotency_key(
    *,
    partner_internal_id: int,
    mapping_id: str,
    idempotency_key: str,
    job_id: str,
) -> None:
    _fetch_one(
        """
        INSERT INTO anyapi_app.idempotency_keys (
            partner_internal_id,
            mapping_id,
            idempotency_key,
            job_id
        ) VALUES (
            %(partner_internal_id)s,
            %(mapping_id)s,
            %(idempotency_key)s,
            %(job_id)s
        )
        ON CONFLICT DO NOTHING
        RETURNING id
        """,
        {
            "partner_internal_id": partner_internal_id,
            "mapping_id": mapping_id,
            "idempotency_key": idempotency_key,
            "job_id": job_id,
        },
    )


# ---------- S3 payload helpers ----------


def store_input_payload(partner_internal_id: int, mapping_id: str, payload: Any) -> Tuple[str, str]:
    prefix = f"inputs/{partner_internal_id}/{mapping_id}"
    return _s3_put_json(prefix, payload)


def store_result_payload(partner_internal_id: int, mapping_id: str, payload: Any) -> Tuple[str, str]:
    prefix = f"results/{partner_internal_id}/{mapping_id}"
    return _s3_put_json(prefix, payload)


def load_payload_from_s3(key: str) -> Any:
    return _s3_get_json(key)


def utc_now() -> str:
    return datetime.utcnow().isoformat()
