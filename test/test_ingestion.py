# test/test_ingestion.py
import pandas as pd
from datetime import date
from libs.adapters.repo_parquet_partitioned import PricesRepoParquetPartitioned
from apps.api.services.prices_ingestion import PricesIngestionService

def fake_fetch(_symbol: str) -> pd.DataFrame:
    return pd.DataFrame([
        {"date": date(2024,1,1), "symbol":"AAPL", "open":1, "high":2, "low":0.5, "close":1.5, "adj_close":1.5, "volume":100},
        {"date": date(2024,1,2), "symbol":"AAPL", "open":1.1, "high":2.1, "low":0.6, "close":1.6, "adj_close":1.6, "volume":200},
    ])

def test_ingest_idempotency(tmp_path):
    # Use partitioned repo with temporary directory
    repo = PricesRepoParquetPartitioned(layer="silver", base_dir=tmp_path)
    svc = PricesIngestionService(repo=repo, fetch_fn=fake_fetch)

    r1 = svc.ingest_last_1y("AAPL")
    r2 = svc.ingest_last_1y("AAPL")

    assert r1["rows"] == 2 and r1["inserted"] == 2
    assert r2["rows"] == 2 and r2["inserted"] == 0  # 再跑不应重复插入
