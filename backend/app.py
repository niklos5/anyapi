from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from auth import require_auth
from mapping_executor import MappingExecutor, flatten_target_schema
from schema_fingerprint import SchemaStructureExtractor
from storage import create_job, get_job, list_jobs, update_job


class AnalyzeRequest(BaseModel):
    data: Any


class MappingSpec(BaseModel):
    targetSchema: Optional[Any] = None
    mappings: List[Dict[str, Any]] = Field(default_factory=list)
    defaults: Optional[Dict[str, Any]] = None


class CreateJobRequest(BaseModel):
    name: str
    sourceType: str
    data: Any
    mapping: MappingSpec


app = FastAPI(title="AnyApi Roaster Service", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _normalize_target_path(path: str) -> str:
    normalized = path
    if normalized.startswith("$."):
        normalized = normalized[2:]
    elif normalized.startswith("$"):
        normalized = normalized[1:]
    normalized = normalized.replace("[*]", "")
    normalized = normalized.replace("[]", "")
    return normalized


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


@app.post("/jobs")
def create_ingestion_job(
    request: CreateJobRequest, claims: Dict[str, Any] = Depends(require_auth)
) -> Dict[str, Any]:
    mapping_spec = request.mapping.dict()
    if not mapping_spec.get("mappings"):
        raise HTTPException(status_code=400, detail="Mapping spec is required.")

    target_schema = mapping_spec.get("targetSchema")
    target_paths: List[str] = []
    if isinstance(target_schema, (dict, list)):
        flattened = flatten_target_schema(target_schema)
        target_paths = [_normalize_target_path(path) for path in flattened.keys()]

    executor = MappingExecutor(mapping_spec, target_paths=target_paths)
    result = executor.execute(request.data)

    partner_id = str(claims.get("partner_id"))
    record = create_job(
        name=request.name,
        source_type=request.sourceType,
        partner_id=partner_id,
        data=request.data,
        mapping=mapping_spec,
        target_schema=target_schema,
    )
    update_job(record.id, status="completed", result=result)

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
