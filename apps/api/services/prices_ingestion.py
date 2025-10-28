# apps/api/services/prices_ingestion.py
from __future__ import annotations

from datetime import date, timedelta
from time import perf_counter
from typing import Iterable, List, Optional
from uuid import uuid4

import structlog

from libs.connectors.base import FetcherPort
from libs.adapters.repo import PricesRepoAdapter
from libs.contracts.prices_daily import PriceRow, validate_prices_batch
from libs.contracts.job_models import JobSpec, StorageEngine, DataSource, WriteMode
from libs.contracts.storage_meta import StorageReport


class PricesIngestionService:
    """
    编排服务：负责
      - 依赖注入（repo/fetcher/logger/clock）
      - 生成 run_id
      - 构造 JobSpec 并计算 idempotency_key
      - fetch -> validate -> repo.upsert
      - 返回 StorageReport（统一契约）
    """

    def __init__(
        self,
        repo: PricesRepoAdapter,
        fetcher: FetcherPort,
        logger=None,
        clock=perf_counter,
        default_write_mode: WriteMode = WriteMode.UPSERT,
        default_layout_version: int = 1,
        default_primary_key: Optional[List[str]] = None,
    ):
        """
        repo    : 仓储接口实现（Parquet/ClickHouse/Postgres 皆可）
        fetcher : 抓取接口实现（yfinance/alpaca/...）
        logger  : 结构化日志（默认 structlog）
        clock   : 计时函数（默认 perf_counter；便于测试替换）
        defaults: 写入语义/布局版本/主键
        """
        self.repo = repo
        self.fetcher = fetcher
        self.log = logger or structlog.get_logger()
        self.clock = clock
        self.default_write_mode = default_write_mode
        self.default_layout_version = default_layout_version
        self.default_primary_key = default_primary_key or ["symbol", "date"]

    # Perf: O(n) w.r.t. rows; validation 和 upsert 主成本在 repo 层
    # Eng : 纯编排，不含具体存储/拉取实现；幂等/回执由契约驱动
    def ingest_last_1y(self, symbol: str, *, on_date: Optional[date] = None) -> StorageReport:
        """
        便捷方法：以 on_date（默认今天）为止的近 365 天闭区间
        """
        today = on_date or date.today()
        return self.ingest_window(symbol=symbol, date_from=today - timedelta(days=365), date_to=today)

    # Perf: O(n); 主要耗时在 fetch 和 repo.upsert
    # Eng : 服务契约主入口；生成 run_id & idempotency_key；空数据也返回合法 StorageReport
    def ingest_window(self, symbol: str, *, date_from: date, date_to: date) -> StorageReport:
        sym = symbol.strip().upper()
        t0 = self.clock()

        # ---- 1) 构造 JobSpec（服务契约核心），生成幂等键 ----
        source_enum = self._resolve_source_enum(self.fetcher)
        engine_enum = self._resolve_engine_enum(self.repo)
        location = self._resolve_location(self.repo)

        spec = JobSpec(
            source=source_enum,
            engine=engine_enum,
            location=location,
            symbols=[sym],
            date_from=date_from,
            date_to=date_to,
            write_mode=self.default_write_mode,
            primary_key=list(self.default_primary_key),
            layout_version=self.default_layout_version,
            options={},  # 如需扩展（分区策略、压缩、schema_version...）
        )
        idem = spec.idempotency_key()
        run_id = uuid4().hex  # 如果你要 uuid7/ulid，可在此替换

        # ---- 2) fetch（按窗口）----
        rows_raw: Iterable[dict] = self.fetcher.fetch_daily(sym, date_from, date_to)
        rows_list: List[PriceRow] = validate_prices_batch(rows_raw) if rows_raw else []

        # ---- 3) 空数据短路：仍返回合法 StorageReport（rows=0）----
        if not rows_list:
            duration_ms = int((self.clock() - t0) * 1000)
            report = StorageReport(
                run_id=run_id,
                idempotency_key=idem,
                engine=engine_enum,
                source=source_enum,
                location=location,
                symbol=sym,
                primary_key=list(self.default_primary_key),
                layout_version=self.default_layout_version,
                write_mode=self.default_write_mode,
                rows=0,
                inserted=0,
                updated=0,
                skipped=0,
            )
            self.log.info(
                "ingest.empty",
                run_id=run_id,
                idempotency_key=idem,
                source=source_enum.value,
                engine=engine_enum.value,
                location=location,
                symbol=sym,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                rows=0,
                inserted=0,
                updated=0,
                skipped=0,
                duration_ms=duration_ms,
            )
            return report

        # ---- 4) upsert（幂等由 repo 实现；回执为 StorageReport）----
        report: StorageReport = self.repo.upsert_prices(
            rows_list,
            primary_key=list(self.default_primary_key),
            write_mode=self.default_write_mode,
            layout_version=self.default_layout_version,
            idempotency_key=idem,
            run_id=run_id,
            engine_opts=None,
        )

        # ---- 5) 结构化日志（含 run_id/idempotency_key 与 KPI）----
        duration_ms = int((self.clock() - t0) * 1000)
        self.log.info(
            "ingest.done",
            run_id=run_id,
            idempotency_key=idem,
            source=source_enum.value,
            engine=engine_enum.value,
            location=location,
            symbol=sym,
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
            rows=report.rows,
            inserted=report.inserted,
            updated=report.updated,
            skipped=report.skipped,
            duration_ms=duration_ms,
        )
        return report

    # ---------- helpers ----------

    @staticmethod
    def _resolve_source_enum(fetcher: FetcherPort) -> DataSource:
        """
        将 fetcher.source_name → DataSource 枚举；若未知，抛出更明确错误，避免悄悄降级为字符串。
        """
        name = getattr(fetcher, "source_name", None)
        if not name:
            raise ValueError("Fetcher 缺少 source_name 属性，无法映射 DataSource")
        try:
            return DataSource(name)
        except Exception as e:
            raise ValueError(f"未知的数据源 '{name}'，请在 DataSource 中登记") from e

    @staticmethod
    def _resolve_engine_enum(repo: PricesRepoAdapter) -> StorageEngine:
        """
        从 repo 暴露的 engine 名称映射到 StorageEngine；若未提供则默认 parquet。
        - 推荐你的 repo 实现上暴露: repo.engine -> 'parquet'/'clickhouse'/'postgres'
        """
        name = getattr(repo, "engine", "parquet")
        try:
            return StorageEngine(name)
        except Exception as e:
            raise ValueError(f"未知的存储引擎 '{name}'，请在 StorageEngine 中登记") from e

    @staticmethod
    def _resolve_location(repo: PricesRepoAdapter) -> str:
        """
        获取 repo 的 location（文件路径或 schema.table）。如果未提供，返回 'unknown'。
        - 建议在具体 repo 实现中定义 self.location 以提升可观测性与回执质量。
        """
        return getattr(repo, "location", "unknown")
