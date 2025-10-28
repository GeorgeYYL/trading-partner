# libs/contracts/prices_daily.py
from datetime import date
from typing import Iterable, Mapping, List, Union
from pydantic import BaseModel, Field, field_validator, ConfigDict

REQUIRED_COLS = {"date","symbol","open","high","low","close","adj_close","volume"}

class PriceRow(BaseModel):
    # 行级事实数据契约（主键：symbol+date）
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    date: date                              # 交易日（UTC 语义，使用 date）
    symbol: str                             # 股票代码（统一大写）
    open: float  = Field(ge=0)              # 开盘价
    high: float  = Field(ge=0)              # 最高价
    low: float   = Field(ge=0)              # 最低价
    close: float = Field(ge=0)              # 收盘价
    adj_close: float = Field(ge=0)          # 复权收盘价
    volume: int = Field(ge=0)               # 成交量（非负整数）

    @field_validator("symbol", mode="before")
    @classmethod
    def _norm_symbol(cls, v: str) -> str:   # 统一大写并去空格
        s = str(v).strip().upper()
        if not s: raise ValueError("symbol 不能为空")
        return s

    def business_check(self) -> None:       # 业务规则校验
        eps = 1e-9
        if self.low > self.high: raise ValueError("low > high")
        lo, hi = self.low, self.high
        if not (lo-eps <= self.close   <= hi+eps): raise ValueError("close 越界")
        if not (lo-eps <= self.open    <= hi+eps): raise ValueError("open 越界")
        if self.adj_close < 0: raise ValueError("adj_close 不能为负")

def validate_prices_batch(rows: Iterable[Union[Mapping, PriceRow]]) -> List[PriceRow]:
    # 批量校验：dict -> PriceRow；PriceRow 复用；附带业务规则
    out: List[PriceRow] = []
    for r in rows:
        if isinstance(r, Mapping):
            missing = REQUIRED_COLS - set(r.keys())
            if missing: raise ValueError(f"缺少必需列: {sorted(missing)}")
            item = PriceRow(**r)
        else:
            item = r
        item.business_check()
        out.append(item)
    return out