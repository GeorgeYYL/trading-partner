# libs/contracts/prices_daily.py
from datetime import date
from pydantic import BaseModel, Field, confloat, conint
from typing import List
import pandas as pd

class PriceDaily(BaseModel):
    date: date
    symbol: str = Field(min_length=1)
    open: confloat(ge=0)
    high: confloat(ge=0)
    low: confloat(ge=0)
    close: confloat(ge=0)
    adj_close: confloat(ge=0)
    volume: conint(ge=0)

def validate_prices_batch(df: pd.DataFrame) -> List[PriceDaily]:
    records = []
    for _, row in df.iterrows():
        record = PriceDaily(
            date=row["date"],
            symbol=row["symbol"],
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            adj_close=row["adj_close"],
            volume=int(row["volume"]),
        )
        records.append(record)
    return records