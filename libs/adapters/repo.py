from __future__ import annotations
from typing import Protocol, List
from libs.contracts.storage_meta import StorageReport  
from libs.contracts.prices_daily import PriceRow

class PricesRepoAdapter(Protocol):
    def upsert_prices(self, rows: List[PriceRow]) -> StorageReport:
        """Insert or update price rows."""
        ...

    def get_prices(self, symbol: str, limit: int = 30) -> List[PriceRow]:
        """Read last N records."""
        ...
