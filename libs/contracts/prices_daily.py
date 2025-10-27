# libs/contracts/prices_daily.py
# ✅ price contrace and validation （L1）
from datetime import date
from typing import Iterable, Mapping, List, Union
from pydantic import BaseModel, Field, ValidationError

class PriceRow(BaseModel):
    # a stander recorder(for all layer)
    date: date                       # trading day in UTC   
    symbol: str                      # Stock code
    open: float  = Field(ge=0)       # Open
    high: float  = Field(ge=0)       # High
    low: float   = Field(ge=0)       # Low
    close: float = Field(ge=0)       # Close
    adj_close: float = Field(ge=0)   # adj_close
    volume: int = Field(ge=0)        # Volume

    def business_check(self) -> None:
        # Business rules：low<=high and close in [min(open,high,low,close), max(...)] 
        if self.low > self.high:
            raise ValueError("low > high")
        lo = min(self.open, self.high, self.low, self.close)
        hi = max(self.open, self.high, self.low, self.close)
        if not (lo <= self.close <= hi):
            raise ValueError("close out of range")

def validate_prices_batch(rows: Iterable[Union[Mapping, PriceRow]]) -> List[PriceRow]:
    # validate batch：dict -> PriceRow；PriceRow -> reuse with rules
    out: List[PriceRow] = []
    for r in rows:
        item = r if isinstance(r, PriceRow) else PriceRow(**r)  # structure validation
        item.symbol = item.symbol.upper()                       # symblo normalization
        item.business_check()                                   # business rules
        out.append(item)
    return out
