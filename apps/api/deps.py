# apps/api/deps.py
from __future__ import annotations
from functools import lru_cache
from time import perf_counter
import structlog

from libs.connectors.registry import get_fetcher
from libs.connectors.base import FetcherPort
from libs.adapters.repo import PricesRepoAdapter
from libs.adapters.repo_parquet import PricesRepoParquet
from apps.api.services.prices_ingestion import PricesIngestionService

def _select_repo(engine: str) -> PricesRepoAdapter:
    """
    Very small factory: choose repo by engine.
    暂时只支持 parquet，其它引擎后续补充。
    """
    if engine == "parquet":
        return PricesRepoParquet()
    # elif engine == "clickhouse": return PricesRepoClickHouse(...)
    # elif engine == "postgres":   return PricesRepoPostgres(...)
    raise ValueError(f"Unknown repo engine: {engine}")

@lru_cache
def get_ingestion_service(source: str = "yfinance", engine: str = "parquet") -> PricesIngestionService:
    """
    Wire up ingestion service (DI):
      - repo: by engine (parquet for now)
      - fetcher: by registry (default yfinance)
      - logger: structlog
      - clock : perf_counter
    """
    repo: PricesRepoAdapter = _select_repo(engine)
    fetcher: FetcherPort = get_fetcher(source)
    logger = structlog.get_logger()
    return PricesIngestionService(repo=repo, fetcher=fetcher, logger=logger, clock=perf_counter)


