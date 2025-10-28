# ✅ JobSpec / JobRun · L1（最小可运行版）
from __future__ import annotations
from enum import StrEnum
from datetime import date, datetime, timezone
from typing import List, Optional, Dict, Any
from hashlib import sha256
from uuid import uuid4
import json
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict

# ---- 枚举：与 storage_meta 对齐 ----
class StorageEngine(StrEnum): PARQUET="parquet"; CLICKHOUSE="clickhouse"; POSTGRES="postgres"
class DataSource(StrEnum): YFINANCE="yfinance"; ALPACA="alpaca"
class WriteMode(StrEnum): UPSERT="upsert"; APPEND="append"; INSERT_OVERWRITE="insert_overwrite"
class RunStatus(StrEnum): PENDING="PENDING"; RUNNING="RUNNING"; SUCCEEDED="SUCCEEDED"; FAILED="FAILED"

# ---- JobSpec：一次“逻辑批次”的规格（用于生成幂等键）----
class JobSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    source: DataSource                                  # 数据来源
    engine: StorageEngine                               # 存储引擎
    location: str = Field(min_length=1)                 # 存储位置（文件/表）
    symbols: List[str]                                  # 目标标的（可多）
    date_from: date                                     # 窗口起（含）
    date_to: date                                       # 窗口止（含/闭区间）
    write_mode: WriteMode = WriteMode.UPSERT            # 写入语义
    primary_key: List[str] = Field(default_factory=lambda: ["symbol","date"])  # 主键
    layout_version: int = 1                             # 布局版本
    options: Dict[str, Any] = Field(default_factory=dict)  # 额外参数（可选）

    @field_validator("symbols", mode="before")
    @classmethod
    def _norm_symbols(cls, v):                          # 统一大写去空白，并去重排序
        vals = [str(s).strip().upper() for s in (v or []) if str(s).strip()]
        return sorted(list(dict.fromkeys(vals)))

    @model_validator(mode="after")
    def _check_window(self):                            # 窗口有效性与主键规范
        if self.date_from > self.date_to: raise ValueError("date_from 不能晚于 date_to")
        if sorted(self.primary_key) != ["date","symbol"]:
            raise ValueError("primary_key 必须为 ['symbol','date']")
        return self

    def idempotency_key(self) -> str:                   # 生成稳定幂等键（规范化→JSON→SHA256前32）
        payload = {
            "source": self.source.value,
            "engine": self.engine.value,
            "location": self.location,                  # 保持原样，避免路径大小写丢失
            "symbols": self.symbols,                    # 已统一大写+排序
            "date_from": self.date_from.isoformat(),
            "date_to": self.date_to.isoformat(),
            "write_mode": self.write_mode.value,
            "primary_key": self.primary_key,            # 已校验
            "layout_version": self.layout_version,
            "options": self.options,                    # 参与口径，确保真正幂等
        }
        s = json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
        return sha256(s.encode("utf-8")).hexdigest()[:32]

# ---- JobRun：一次“实际执行”的记录（用于追踪/审计）----
class JobRun(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_id: str = Field(default_factory=lambda: uuid4().hex)     # 本次执行ID
    status: RunStatus = RunStatus.PENDING                        # 状态
    spec_key: str                                                # 关联 JobSpec 的幂等键
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))  # UTC
    finished_at: Optional[datetime] = None                       # 结束时间（UTC）
    error_message: Optional[str] = None                          # 错误信息（失败时）

    @field_validator("started_at", "finished_at", mode="after")
    @classmethod
    def _ensure_utc(cls, v: Optional[datetime]):                 # 时间必须为 UTC 或 None
        if v is None: return v
        if v.tzinfo is None or v.utcoffset() is None: raise ValueError("时间戳必须带UTC时区")
        if v.utcoffset().total_seconds() != 0: raise ValueError("必须是UTC时区")
        return v

    @model_validator(mode="after")
    def _check_times(self):                                      # 结束时间不得早于开始时间
        if self.finished_at and self.finished_at < self.started_at:
            raise ValueError("finished_at 不能早于 started_at")
        return self