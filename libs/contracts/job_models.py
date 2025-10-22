from __future__ import annotations
from datetime import datetime, date, timezone
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


# ---- helpers ----
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def make_idempotency_key(job_type: "JobType", symbol: str, asof: date) -> str:
    sym = (symbol or "").upper().strip()
    return f"{job_type.value}:{sym}:{asof.isoformat()}"


# ---- domain enums ----
class JobType(str, Enum):
    daily_pipeline = "daily_pipeline"


class JobStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    dead_letter = "dead_letter"


# ---- queue message ----
class JobMessage(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    job_id: str
    job_type: JobType
    symbol: str
    asof: date
    requested_by: str = Field(default="api")
    idempotency_key: str
    enqueued_at: datetime = Field(default_factory=utcnow)


# ---- repo record (state machine) ----
class JobRecord(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    job_id: str
    job_type: JobType
    symbol: str
    asof: date
    requested_by: str = "api"
    idempotency_key: str

    status: JobStatus = JobStatus.queued
    attempts: int = 0

    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    error_code: Optional[str] = None
    error_message: Optional[str] = None

    def set_running(self) -> None:
        if self.status not in {JobStatus.queued, JobStatus.failed}:
            raise ValueError(f"Invalid transition {self.status} -> running")
        self.status = JobStatus.running
        self.attempts += 1
        self.started_at = self.started_at or utcnow()
        self.updated_at = utcnow()

    def set_succeeded(self) -> None:
        if self.status != JobStatus.running:
            raise ValueError(f"Invalid transition {self.status} -> succeeded")
        self.status = JobStatus.succeeded
        self.finished_at = utcnow()
        self.updated_at = utcnow()
        self.error_code = None
        self.error_message = None

    def set_failed(self, error_code: str, error_message: str) -> None:
        if self.status not in {JobStatus.running, JobStatus.queued}:
            raise ValueError(f"Invalid transition {self.status} -> failed")
        self.status = JobStatus.failed
        self.updated_at = utcnow()
        self.error_code = error_code
        self.error_message = error_message

    def to_message(self) -> JobMessage:
        return JobMessage(
            job_id=self.job_id,
            job_type=self.job_type,
            symbol=self.symbol,
            asof=self.asof,
            requested_by=self.requested_by,
            idempotency_key=self.idempotency_key,
        )