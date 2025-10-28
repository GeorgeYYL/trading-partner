# libs/adapters/repo.py
from __future__ import annotations
from typing import Protocol, List, Optional, Dict, Any
from libs.contracts.prices_daily import PriceRow
from libs.contracts.storage_meta import StorageReport, WriteMode
from libs.contracts.job_models import DataSource  # 仅用于标注来源（回执 lineage）

class PricesRepoAdapter(Protocol):
    """
    仓储接口（Storage 抽象）
    - 所有实现（parquet/clickhouse/postgres）必须遵守相同签名与语义
    - 返回 StorageReport（统一回执契约）
    """

    # Engine/Location 用于上层观测：实现类应提供以下属性
    engine: str              # 如 "parquet" / "clickhouse" / "postgres"
    location: str            # 文件路径或 "schema.table"

    def upsert_prices(
        self,
        rows: List[PriceRow],
        *,
        primary_key: List[str],
        write_mode: WriteMode,
        layout_version: int,
        idempotency_key: str,
        run_id: str,
        engine_opts: Optional[Dict[str, Any]] = None,
        source: Optional[DataSource] = None,  # 可由 service 传入，用于回执 lineage
        symbol: Optional[str] = None,         # 可由 service 传入；否则由 rows 推断
    ) -> StorageReport:
        """
        写入价格数据（幂等 + 回执）
        语义：
          - 幂等：idempotency_key 命中 → 直接返回历史回执（不得重复写）
          - 计数：严格 rows == inserted + updated + skipped
          - UPSERT：PK 冲突行若值有变化 → updated；无变化 → skipped
          - APPEND：仅新增 PK（与 UPSERT 类似，但不统计 updated）
          - INSERT_OVERWRITE：删除同窗口/同 PK 的旧行后全量写入（MVP：按 PK 覆盖）
        """
        ...

    def get_prices(self, symbol: str, limit: int = 30) -> List[PriceRow]:
        """
        读取最近 N 条（按 date 降序）
        """
        ...
