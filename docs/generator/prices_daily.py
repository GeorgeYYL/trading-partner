# This file is generated from libs/contracts/prices_daily.yaml
# Do NOT edit manually — update the YAML instead.

from __future__ import annotations
from enum import Enum
from datetime import date, datetime
from pydantic import BaseModel, Field, conint, confloat


class JobStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"


class PricesDaily(BaseModel):
    """Daily stock OHLCV data fetched from Alpaca/Yahoo and validated by Prefect flow."""

    symbol: str = Field(..., description="Stock ticker")
    date: date
    open: confloat(ge=0)
    high: confloat(ge=0)
    low: confloat(ge=0)
    close: confloat(ge=0)
    volume: conint(ge=0)
    job_status: JobStatus
    ingested_at: datetime
