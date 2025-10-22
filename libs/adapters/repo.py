from __future__ import annotations
from typing import Protocol, Optional
from datetime import date
from libs.contracts.job_models import JobRecord, JobStatus, JobType, make_idempotency_key
from libs.contracts.prices_daily import PriceDaily

class PricesRepoAdapter(Protocol):
    def upsert_prices(self, rows: list[PriceDaily]) -> tuple[int, int]:
        """Insert or update price rows."""
        ...

    def get_prices(self, symbol: str, limit: int = 30) -> list[PriceDaily]:
        """Read last N records."""
        ...
