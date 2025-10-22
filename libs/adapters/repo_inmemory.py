from __future__ import annotations
from typing import Optional, Dict
from datetime import date
from uuid import uuid4
from libs.contracts.job_models import (
    JobRecord,
    JobType,
    JobStatus,
    make_idempotency_key,
)
from .errors import IdempotencyConflict, NotFound


class InMemoryJobRepo:
    def __init__(self) -> None:
        self._by_id: Dict[str, JobRecord] = {}
        self._by_key: Dict[str, str] = {}

    # ---- reads ----
    def get_by_id(self, job_id: str) -> JobRecord:
        rec = self._by_id.get(job_id)
        if not rec:
            raise NotFound(job_id)
        return rec

    def get_by_idempotency_key(self, key: str) -> Optional[JobRecord]:
        jid = self._by_key.get(key)
        return self._by_id.get(jid) if jid else None

    # ---- writes ----
    def create_queued(
        self,
        *,
        job_type: JobType,
        symbol: str,
        asof: date,
        requested_by: str,
    ) -> JobRecord:
        key = make_idempotency_key(job_type, symbol, asof)
        existing = self.get_by_idempotency_key(key)
        if existing and existing.status in {JobStatus.queued, JobStatus.running}:
            raise IdempotencyConflict(key)
        job_id = str(uuid4())
        rec = JobRecord(
            job_id=job_id,
            job_type=job_type,
            symbol=symbol,
            asof=asof,
            requested_by=requested_by,
            idempotency_key=key,
            status=JobStatus.queued,
        )
        self._by_id[job_id] = rec
        self._by_key[key] = job_id
        return rec

    def set_running(self, job_id: str) -> JobRecord:
        rec = self.get_by_id(job_id)
        rec.set_running()
        self._by_id[job_id] = rec
        return rec

    def set_succeeded(self, job_id: str) -> JobRecord:
        rec = self.get_by_id(job_id)
        rec.set_succeeded()
        self._by_id[job_id] = rec
        return rec

    def set_failed(self, job_id: str, error_code: str, error_message: str) -> JobRecord:
        rec = self.get_by_id(job_id)
        rec.set_failed(error_code, error_message)
        self._by_id[job_id] = rec
        return rec

    def ping(self) -> bool:
        return True