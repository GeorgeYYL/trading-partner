# apps/api/services/prices_ingestion.py
from __future__ import annotations

from time import perf_counter
import structlog

from libs.connectors.base import FetcherPort           # ✅ 依赖接口，不依赖具体 yfinance
from libs.adapters.repo import PricesRepoAdapter     # ✅ 依赖仓储接口
from libs.contracts.prices_daily import validate_prices_batch


class PricesIngestionService:
    def __init__(
        self,
        repo: PricesRepoAdapter,
        fetcher: FetcherPort,
        logger=None,
        clock=perf_counter,
    ):
        """
        repo    : 仓储接口实现（Parquet/ClickHouse/Postgres 皆可）
        fetcher : 抓取接口实现（yfinance/alpaca/...）
        logger  : 结构化日志（默认 structlog）
        clock   : 计时函数（默认 perf_counter；便于测试替换）
        """
        self.repo = repo
        self.fetcher = fetcher
        self.log = logger or structlog.get_logger()
        self.clock = clock

    def ingest_last_1y(self, symbol: str) -> dict:
        """拉取近 1 年日线 → 校验 → upsert；返回摘要结果"""
        sym = symbol.upper().strip()
        t0 = self.clock()

        df = self.fetcher.fetch_daily(sym)   # ✅ 不关心具体来源，由 fetcher 实现
        if df.empty:
            duration_ms = int((self.clock() - t0) * 1000)
            self.log.info(
                "ingest.empty",
                symbol=sym,
                source=self.fetcher.source_name,
                rows=0, inserted=0, updated=0,
                duration_ms=duration_ms,
            )
            return {"symbol": sym, "rows": 0, "inserted": 0, "updated": 0}

        rows = validate_prices_batch(df)     # 契约校验失败 → 让异常冒泡给上层
        inserted, updated = self.repo.upsert_prices(rows)

        duration_ms = int((self.clock() - t0) * 1000)
        self.log.info(
            "ingest.done",
            symbol=sym,
            source=self.fetcher.source_name,
            rows=len(rows), inserted=inserted, updated=updated,
            duration_ms=duration_ms,
        )
        return {"symbol": sym, "rows": len(rows), "inserted": inserted, "updated": updated}
