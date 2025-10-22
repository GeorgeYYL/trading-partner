# apps/api/deps.py
from __future__ import annotations

from time import perf_counter
import structlog

from libs.connectors.registry import get_fetcher              # <- 从 registry 选 fetcher
from libs.adapters.repo_parquet import PricesRepoParquet # <- 选 parquet 仓储
from apps.api.services.prices_ingestion import PricesIngestionService


def get_ingestion_service() -> PricesIngestionService:
    """
    Wire up a minimal ingestion service with DI:
      - repo: parquet
      - fetcher: yfinance (via registry)
      - logger: structlog
      - clock: perf_counter
    """
    repo = PricesRepoParquet()
    fetcher = get_fetcher("yfinance")
    logger = structlog.get_logger()
    clock = perf_counter
    return PricesIngestionService(repo=repo, fetcher=fetcher, logger=logger, clock=clock)
