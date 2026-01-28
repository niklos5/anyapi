from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4


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
    result: Optional[Dict[str, Any]] = None
    issues: List[Dict[str, Any]] = field(default_factory=list)


_JOBS: Dict[str, JobRecord] = {}


def create_job(
    name: str,
    source_type: str,
    partner_id: str,
    data: Any,
    mapping: Dict[str, Any],
    target_schema: Any,
) -> JobRecord:
    job_id = f"job_{uuid4().hex[:6]}"
    record = JobRecord(
        id=job_id,
        name=name,
        source_type=source_type,
        partner_id=partner_id,
        status="processing",
        created_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
        data=data,
        mapping=mapping,
        target_schema=target_schema,
    )
    _JOBS[job_id] = record
    return record


def update_job(job_id: str, **kwargs: Any) -> Optional[JobRecord]:
    record = _JOBS.get(job_id)
    if not record:
        return None
    for key, value in kwargs.items():
        setattr(record, key, value)
    return record


def list_jobs(partner_id: str) -> List[JobRecord]:
    jobs = [job for job in _JOBS.values() if job.partner_id == partner_id]
    return sorted(jobs, key=lambda item: item.created_at, reverse=True)


def get_job(job_id: str, partner_id: str) -> Optional[JobRecord]:
    job = _JOBS.get(job_id)
    if not job or job.partner_id != partner_id:
        return None
    return job
