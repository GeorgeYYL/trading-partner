# libs/contracts/storage_meta.py
from __future__ import annotations
from enum import StrEnum
from uuid import uuid4
from datetime import datetime, timezone
from typing import List
from pydantic import BaseModel, Field, field_validator, model_validator
from libs.contracts.job_models import StorageEngine, DataSource, WriteMode

# -------- repo contract --------
class StorageReport(BaseModel):
    """存储回执契约：描述一次存储写入的结构与统计"""
    run_id: str = Field(default_factory=lambda: uuid4().hex)
    idempotency_key: str | None = None
    engine: StorageEngine
    source: DataSource
    location: str = Field(..., min_length=1)
    symbol: str = Field(..., min_length=1)
    primary_key: List[str] = Field(default_factory=lambda: ["symbol", "date"])
    layout_version: int = 1
    write_mode: WriteMode = WriteMode.UPSERT
    rows: int = Field(..., ge=0)
    inserted: int = Field(0, ge=0)
    updated: int = Field(0, ge=0)
    skipped: int = Field(0, ge=0)
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # -------- validator --------
    @field_validator("symbol", mode="before")
    @classmethod
    def norm_symbol(cls, v: str) -> str:
        """统一symbol为大写，去空格"""
        s = str(v).strip().upper()
        if not s:
            raise ValueError("symbol 不能为空")
        return s

    @field_validator("ts", mode="after")
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        """utc timezone"""
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError("ts 必须为带时区的 UTC 时间")
        if v.utcoffset().total_seconds() != 0:
            raise ValueError("ts 必须为 UTC 时区")
        return v

    # -------- model validator --------
    @model_validator(mode="after")
    def check_counts(self):
        """严格计数关系: rows == inserted + updated + skipped"""
        total = self.inserted + self.updated + self.skipped
        if self.rows != total:
            raise ValueError(f"rows({self.rows}) 必须等于 inserted+updated+skipped({total})")
        return self



