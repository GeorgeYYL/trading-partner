# libs/adapters/repo_parquet_partitioned.py
from __future__ import annotations
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import date
import pandas as pd
import json

from libs.contracts.prices_daily import PriceRow
from libs.contracts.storage_meta import StorageReport
from libs.contracts.job_models import DataSource, StorageEngine, WriteMode
from libs.adapters.repo import PricesRepoAdapter
from libs.storage.lakehouse_paths import LakehousePaths, Layer

class PricesRepoParquetPartitioned(PricesRepoAdapter):
    """
    Partitioned Parquet implementation with Hive-style partitioning.
    
    Structure:
        data/silver/prices_daily/symbol=AAPL/year=2024/month=01/data.parquet
    
    Benefits:
        - Partition pruning for faster queries
        - Incremental updates (append new partitions)
        - Scalable to millions of rows
    """
    engine = StorageEngine.PARQUET.value
    
    def __init__(
        self,
        layer: Layer = "silver",
        dataset: str = "prices_daily",
        base_dir: str | Path = "data",
    ):
        self.lakehouse = LakehousePaths(base_dir)
        self.layer = layer
        self.dataset = dataset
        self.location = f"{base_dir}/{layer}/{dataset}"
    
    def upsert_prices(
        self,
        rows: List[PriceRow],
        *,
        primary_key: List[str],
        write_mode: WriteMode,
        layout_version: int,
        idempotency_key: str,
        run_id: str,
        engine_opts: Optional[Dict[str, Any]] = None,  # Reserved for future: compression, encoding, etc.
        source: Optional[DataSource] = None,
    ) -> StorageReport:
        """
        Write prices to partitioned parquet files.

        Strategy:
            1. Group rows by (symbol, year, month)
            2. For each partition:
                - Read existing data (if exists)
                - Merge with new data (upsert/append/overwrite)
                - Write back to partition

        Args:
            rows: List of price records to write
            primary_key: Columns that form the primary key (typically ["symbol", "date"])
            write_mode: UPSERT, APPEND, or INSERT_OVERWRITE
            layout_version: Schema version for future compatibility
            idempotency_key: Unique key to prevent duplicate writes
            run_id: Unique identifier for this execution
            engine_opts: Reserved for future parquet options (compression, encoding, etc.)
            source: Data source (e.g., YFINANCE, ALPACA)

        Returns:
            StorageReport with statistics (inserted, updated, skipped counts)
        """
        # Check idempotency
        manifest_path = self.lakehouse.get_manifest_path(self.layer, self.dataset)
        found = self._manifest_lookup(manifest_path, idempotency_key)
        if found:
            return self._report_from_manifest(found)

        # Convert to DataFrame using helper (ensures proper normalization)
        df_new = self._rows_to_df(rows)

        # Validate primary key columns exist
        for col in primary_key:
            if col not in df_new.columns:
                raise ValueError(f"缺少主键列: {col}")

        # Group by partition keys
        df_new["_year"] = pd.to_datetime(df_new["date"]).dt.year
        df_new["_month"] = pd.to_datetime(df_new["date"]).dt.month

        total_inserted = 0
        total_updated = 0
        total_skipped = 0

        # Process each partition
        for (symbol, year, month), partition_df in df_new.groupby(["symbol", "_year", "_month"]):
            partition_df = partition_df.drop(columns=["_year", "_month"])

            # Get partition file path (convert numpy int64 to Python int)
            partition_date = date(int(year), int(month), 1)
            file_path = self.lakehouse.get_data_file(
                self.layer,
                self.dataset,
                symbol,
                partition_date,
                source=source.value if source else None,
            )
            
            # Read existing data
            if file_path.exists():
                df_old = pd.read_parquet(file_path)
            else:
                df_old = pd.DataFrame()
            
            # Merge based on write mode
            if write_mode == WriteMode.UPSERT:
                merged, ins, upd, skip = self._upsert(df_old, partition_df, primary_key)
            elif write_mode == WriteMode.APPEND:
                merged, ins, upd, skip = self._append(df_old, partition_df, primary_key)
            elif write_mode == WriteMode.INSERT_OVERWRITE:
                merged, ins, upd, skip = self._insert_overwrite(df_old, partition_df, primary_key)
            else:
                raise ValueError(f"Unknown write mode: {write_mode}")
            
            # Write partition
            merged.to_parquet(file_path, index=False)
            
            total_inserted += ins
            total_updated += upd
            total_skipped += skip

        # Create report (infer symbol from data)
        symbol_str = self._infer_symbol(df_new)
        report = StorageReport(
            run_id=run_id,
            idempotency_key=idempotency_key,
            engine=StorageEngine.PARQUET,
            source=source or DataSource.YFINANCE,
            location=self.location,
            symbol=symbol_str,
            primary_key=list(primary_key),
            layout_version=layout_version,
            write_mode=write_mode,
            rows=len(df_new),
            inserted=total_inserted,
            updated=total_updated,
            skipped=total_skipped,
        )

        # Write manifest
        self._manifest_write(manifest_path, idempotency_key, report)

        return report
    
    def get_prices(
        self,
        symbol: str,
        limit: int = 30,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> List[PriceRow]:
        """
        Read prices with partition pruning.
        
        Strategy:
            1. List all partitions for symbol
            2. Filter partitions by date range (if provided)
            3. Read only relevant partitions
            4. Combine and sort
        """
        partitions = self.lakehouse.list_partitions(self.layer, self.dataset, symbol)
        
        if not partitions:
            return []
        
        # Read all partitions
        dfs = []
        for partition_file in partitions:
            df = pd.read_parquet(partition_file)
            
            # Filter by date range
            if date_from:
                df = df[pd.to_datetime(df["date"]) >= pd.Timestamp(date_from)]
            if date_to:
                df = df[pd.to_datetime(df["date"]) <= pd.Timestamp(date_to)]
            
            if not df.empty:
                dfs.append(df)
        
        if not dfs:
            return []
        
        # Combine and sort
        df_combined = pd.concat(dfs, ignore_index=True)
        df_combined = df_combined.sort_values("date", ascending=False).head(limit)
        
        # Convert to PriceRow
        rows = []
        for record in df_combined.to_dict(orient="records"):
            if hasattr(record["date"], "date"):
                record["date"] = record["date"].date()
            rows.append(PriceRow(**record))
        
        return rows

    # --------------- internal helpers ---------------

    def _rows_to_df(self, rows: List[PriceRow]) -> pd.DataFrame:
        """Convert PriceRow objects to DataFrame with proper normalization."""
        df = pd.DataFrame([r.model_dump() for r in rows])
        # 确保 symbol 标准化（保险起见；PriceRow 已做过）
        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
        # 确保 date 是日期（非 tz datetime）
        if "date" in df.columns:
            # 如果是 datetime64，转为日期
            if pd.api.types.is_datetime64_any_dtype(df["date"]):
                df["date"] = df["date"].dt.date
        return df

    def _upsert(self, df_old: pd.DataFrame, df_new: pd.DataFrame, pk: List[str]):
        """
        UPSERT 语义：
          - 新 PK 不存在 → inserted
          - PK 存在：
              值有变化 → updated（取新值）
              值无变化 → skipped
        """
        if df_old.empty:
            merged = df_new.copy()
            inserted = len(df_new)
            updated = 0
            skipped = 0
            return merged, inserted, updated, skipped

        # 标记：新数据中的 PK
        pk_cols = pk
        key_new = df_new[pk_cols].astype(str).agg("||".join, axis=1)
        key_old = df_old[pk_cols].astype(str).agg("||".join, axis=1)

        set_new = set(key_new)
        set_old = set(key_old)

        # 插入：新有旧无
        key_insert = set_new - set_old
        mask_insert = key_new.isin(key_insert)
        df_ins = df_new[mask_insert]

        # PK 交集（考虑 updated / skipped）
        key_inter = set_new & set_old
        mask_inter_new = key_new.isin(key_inter)

        # 对旧数据去除交集，再拼接（再把交集用新值覆盖）
        df_old_keep = df_old[~key_old.isin(key_inter)]
        df_inter_new = df_new[mask_inter_new]

        # 计算 updated：比较非主键列是否变化
        non_pk_cols = [c for c in df_new.columns if c not in pk_cols]
        updated = 0
        if not df_inter_new.empty:
            # 从旧数据取出同 PK 的行，对齐到新数据顺序
            df_old_inter = df_old[df_old[pk_cols].astype(str).agg("||".join, axis=1).isin(key_inter)]
            # 按 PK 合并便于比较
            comp_new = df_inter_new.set_index(pk_cols)[non_pk_cols].sort_index()
            comp_old = df_old_inter.set_index(pk_cols)[non_pk_cols].sort_index()
            # 对齐列（防止旧数据缺列）
            for col in comp_new.columns:
                if col not in comp_old.columns:
                    comp_old[col] = pd.NA
            comp_old = comp_old[comp_new.columns]
            diffs = (comp_new != comp_old) & ~(comp_new.isna() & comp_old.isna())
            updated = int(diffs.any(axis=1).sum())

        skipped = int(len(df_new) - len(df_ins) - updated)

        merged = pd.concat([df_old_keep, df_inter_new, df_ins], ignore_index=True)
        # 去重保障（双保险）
        merged = merged.drop_duplicates(subset=pk_cols, keep="last")
        return merged, int(len(df_ins)), int(updated), int(skipped)

    def _append(self, df_old: pd.DataFrame, df_new: pd.DataFrame, pk: List[str]):
        """
        APPEND 语义（MVP 简化）：
          - 仅新增 PK（避免重复），不更新既有行
          - 插入数 = 新 PK - 旧 PK；updated=0
          - 若完全重复 → 全部计入 skipped
        """
        if df_old.empty:
            merged = df_new.copy()
            inserted = len(df_new)
            updated = 0
            skipped = 0
            return merged, inserted, updated, skipped

        pk_cols = pk
        key_new = df_new[pk_cols].astype(str).agg("||".join, axis=1)
        key_old = df_old[pk_cols].astype(str).agg("||".join, axis=1)

        key_insert = set(key_new) - set(key_old)
        mask_insert = key_new.isin(key_insert)

        df_ins = df_new[mask_insert]
        merged = pd.concat([df_old, df_ins], ignore_index=True)
        merged = merged.drop_duplicates(subset=pk_cols, keep="first")

        inserted = int(len(df_ins))
        updated = 0
        skipped = int(len(df_new) - inserted)
        return merged, inserted, updated, skipped

    def _insert_overwrite(self, df_old: pd.DataFrame, df_new: pd.DataFrame, pk: List[str]):
        """
        INSERT_OVERWRITE 语义（MVP 简化）：
          - 删除旧数据中与 df_new 冲突的 PK（可被视为一个“窗口覆盖”）
          - 再把新数据写入
          - inserted=len(df_new), updated=0, skipped=0
        """
        if df_old.empty:
            merged = df_new.copy()
            return merged, len(df_new), 0, 0

        pk_cols = pk
        key_new = df_new[pk_cols].astype(str).agg("||".join, axis=1)
        key_old = df_old[pk_cols].astype(str).agg("||".join, axis=1)

        df_old_keep = df_old[~key_old.isin(set(key_new))]
        merged = pd.concat([df_old_keep, df_new], ignore_index=True)
        merged = merged.drop_duplicates(subset=pk_cols, keep="last")
        inserted = int(len(df_new))
        updated = 0
        skipped = 0
        return merged, inserted, updated, skipped

    # ---- manifest 管理：idempotency_key -> report_json ----

    def _manifest_lookup(self, manifest_path: Path, key: str) -> Optional[dict]:
        if not manifest_path.exists():
            return None
        try:
            with manifest_path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    if obj.get("idempotency_key") == key:
                        return obj
        except FileNotFoundError:
            return None
        return None

    def _manifest_write(self, manifest_path: Path, key: str, report: StorageReport) -> None:
        rec = report.model_dump(mode="json")
        # Check if already exists to avoid duplicates
        prev = self._manifest_lookup(manifest_path, key)
        if prev is not None:
            return
        
        # Ensure parent directory exists
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        
        with manifest_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def _report_from_manifest(self, obj: dict) -> StorageReport:
        """
        从 manifest 记录还原 StorageReport
        （StorageReport 的字段若升级了，pydantic 会对缺失/多余字段做容错，只要不 forbid）
        """
        return StorageReport(**obj)

    # ---- minor helpers ----

    @staticmethod
    def _infer_symbol(df_new: pd.DataFrame) -> str:
        if "symbol" not in df_new.columns or df_new.empty:
            return "UNKNOWN"
        syms = list(pd.unique(df_new["symbol"]))
        if len(syms) == 1:
            return str(syms[0])
        # 多 symbol 的批次：回执使用 'MULTI'
        return "MULTI"

    @staticmethod
    def _safe_source_default() -> str:
        # 如果 service 没有传入 source，就用一个兜底字符串；更推荐由 service 传。
        return DataSource.YFINANCE.value
