from __future__ import annotations  # 兼容前向注解
from pydantic import BaseModel, Field, field_validator  # 轻量数据校验
from datetime import datetime, timezone  # 统一UTC时间

class StorageReport(BaseModel):
    engine: str = Field(..., min_length=1)              # 存储引擎：parquet/clickhouse/postgres
    location: str = Field(..., min_length=1)            # 存储位置：文件路径或表名
    source: str = Field(..., min_length=1)              # 数据来源：如 yfinance/alpaca
    symbol: str = Field(..., min_length=1)              # 主维度：本批处理的股票代码
    rows: int = Field(..., ge=0)                        # 本次处理的记录数
    inserted: int = Field(0, ge=0)                      # 插入条数
    updated: int = Field(0, ge=0)                       # 更新条数
    ts: datetime = Field(default_factory=lambda:         # 回执生成时间（UTC）
                         datetime.now(timezone.utc))

    @field_validator("symbol")
    @classmethod
    def norm_symbol(cls, v: str) -> str:                # 统一symbol为大写
        return v.upper()

    @field_validator("updated")
    @classmethod
    def check_counts(cls, v: int, info):                # 计数关系校验：inserted+updated<=rows
        rows = info.data.get("rows", 0)
        ins = info.data.get("inserted", 0)
        if ins + v > rows:
            raise ValueError("inserted + updated 不能大于 rows")
        return v
