# apps/api/deps.py
from __future__ import annotations
from functools import lru_cache
from time import perf_counter
import structlog

from libs.connectors.registry import get_fetcher
from libs.connectors.base import FetcherPort
from libs.adapters.repo import PricesRepoAdapter
from libs.adapters.repo_parquet_partitioned import PricesRepoParquetPartitioned
from apps.api.services.prices_ingestion import PricesIngestionService

def _select_repo(engine: str = "parquet_partitioned") -> PricesRepoAdapter:
    if engine == "parquet_partitioned":
        return PricesRepoParquetPartitioned(layer="silver")
    raise ValueError(f"Unknown engine: {engine}")

@lru_cache
def get_ingestion_service(source: str = "yfinance", engine: str = "parquet_partitioned") -> PricesIngestionService:
    """
    Wire up ingestion service (DI):
      - repo: by engine (parquet_partitioned for now)
      - fetcher: by registry (default yfinance)
      - logger: structlog
      - clock : perf_counter
    """
    repo: PricesRepoAdapter = _select_repo(engine)
    fetcher: FetcherPort = get_fetcher(source)
    logger = structlog.get_logger()
    return PricesIngestionService(repo=repo, fetcher=fetcher, logger=logger, clock=perf_counter)


