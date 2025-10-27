from pathlib import Path
import pandas as pd
from libs.contracts.prices_daily import PriceRow
from libs.adapters.repo import PricesRepoAdapter

class PricesRepoParquet(PricesRepoAdapter):
    def __init__(self, path: str | Path = "data/prices_daily.parquet"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def upsert_prices(self, rows: list[PriceRow]) -> tuple[int, int]:
        df_new = pd.DataFrame([r.model_dump() for r in rows])
        if not self.path.exists():
            df_new.to_parquet(self.path, index=False)
            return (len(df_new), 0)

        df_old = pd.read_parquet(self.path)
        merged = (
            pd.concat([df_old, df_new])
              .drop_duplicates(subset=["symbol", "date"], keep="last")
        )
        inserted = len(merged) - len(df_old)
        merged.to_parquet(self.path, index=False)
        return (inserted, 0)

    def get_prices(self, symbol: str, limit: int = 30) -> list[PriceRow]:
        if not self.path.exists():
            return []
        df = pd.read_parquet(self.path)
        df = df[df["symbol"] == symbol].sort_values("date", ascending=False).head(limit)
        return [PriceRow(**row) for row in df.to_dict(orient="records")]