from __future__ import annotations
from typing import Protocol, runtime_checkable
from datetime import date
import pandas as pd

class PricesFetcher(Protocol):
    def fetch_last_1y_daily(self, symbol: str) -> pd.DataFrame: ...


@runtime_checkable
class FetcherPort(Protocol):
    """
    Contract for price-data fetchers.

    Requirements (must be satisfied by implementers):
    - `source_name`: a short identifier for lineage/logging, e.g. "yfinance", "alpaca".
    - `fetch_daily(symbol, date_from, date_to)`: returns a pandas.DataFrame with columns:
        ['date','open','high','low','close','adj_close','volume','symbol']
      Types:
        - date: datetime.date or pandas datetime (will be normalized upstream)
        - open/high/low/close/adj_close: numeric
        - volume: integer-like
        - symbol: UPPERCASE str for every row
    """

    # attribute used by logging and lineage
    source_name: str

    # fetch daily bars; date range is optional; implementers may ignore it if unsupported
    def fetch_daily(
        self,
        symbol: str,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> pd.DataFrame:
        ...