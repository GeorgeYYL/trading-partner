# libs/adapters/repo_parquet.py
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Dict, Any, Iterable
import json
import os
import tempfile

import pandas as pd

from libs.contracts.prices_daily import PriceRow
from libs.contracts.storage_meta import StorageReport
from libs.contracts.job_models import DataSource, StorageEngine, WriteMode
from libs.adapters.repo import PricesRepoAdapter


class PricesRepoParquet(PricesRepoAdapter):
    """
    Parquet 单文件实现（MVP）：
      - 文件：data/prices_daily.parquet
      - 幂等：data/_manifests/prices_ingestion.jsonl
    设计说明：
      - 为简化：单文件 + 基于 PK 的 UPSERT/APPEND/OVERWRITE
      - 生产化可演进为分区目录（symbol=.../date=...）+ manifest per key
    """
    engine = StorageEngine.PARQUET.value

    def __init__(self, path: str | Path = "data/prices_daily.parquet", manifest_path: str | Path = "data/_manifests/prices_ingestion.jsonl"):
        self.path = Path(path)
        self.location = str(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

        self.manifest_path = Path(manifest_path)
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)

    # --------------- public API ---------------

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
        source: Optional[DataSource] = None,
        symbol: Optional[str] = None,
    ) -> StorageReport:
        # 幂等短路：命中 manifest 直接返回历史回执
        found = self._manifest_lookup(idempotency_key)
        if found is not None:
            return self._report_from_manifest(found)

        # 准备输入 DataFrame
        df_new = self._rows_to_df(rows)

        # 主键校验
        for col in primary_key:
            if col not in df_new.columns:
                raise ValueError(f"缺少主键列: {col}")

        # 推断 symbol（若未显式传入）
        sym = symbol or self._infer_symbol(df_new)

        # 读取旧数据
        df_old = self._read_parquet()

        # 根据模式执行合并/覆盖
        if write_mode == WriteMode.UPSERT:
            merged, inserted, updated, skipped = self._upsert(df_old, df_new, primary_key)
        elif write_mode == WriteMode.APPEND:
            merged, inserted, updated, skipped = self._append(df_old, df_new, primary_key)
        elif write_mode == WriteMode.INSERT_OVERWRITE:
            merged, inserted, updated, skipped = self._insert_overwrite(df_old, df_new, primary_key)
        else:
            raise ValueError(f"未知写入模式: {write_mode}")

        # 原子写回
        self._atomic_write_parquet(merged)

        # 组装回执（严格三段式计数）
        report = StorageReport(
            run_id=run_id,
            idempotency_key=idempotency_key,
            engine=StorageEngine.PARQUET,           # 本实现固定为 parquet
            source=source or DataSource(self._safe_source_default()),  # 若 service 未传，容错但建议传
            location=self.location,
            symbol=sym,
            primary_key=list(primary_key),
            layout_version=layout_version,
            write_mode=write_mode,
            rows=int(len(df_new)),
            inserted=int(inserted),
            updated=int(updated),
            skipped=int(skipped),
        )
        # 写入 manifest（完成标记）
        self._manifest_write(idempotency_key, report)

        return report

    def get_prices(self, symbol: str, limit: int = 30) -> List[PriceRow]:
        if not self.path.exists():
            return []
        df = pd.read_parquet(self.path)
        if "symbol" not in df.columns or "date" not in df.columns:
            return []
        df = df[df["symbol"] == symbol.strip().upper()].sort_values("date", ascending=False).head(limit)
        # pandas 读 parquet 后 date 可能是 Timestamp，需转回 date
        recs = df.to_dict(orient="records")
        out: List[PriceRow] = []
        for r in recs:
            if hasattr(r["date"], "date"):  # pandas.Timestamp
                r["date"] = r["date"].date()
            out.append(PriceRow(**r))
        return out

    # --------------- internal helpers ---------------

    def _rows_to_df(self, rows: List[PriceRow]) -> pd.DataFrame:
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

    def _read_parquet(self) -> pd.DataFrame:
        if not self.path.exists():
            return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "adj_close", "volume"])
        return pd.read_parquet(self.path)

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

    def _atomic_write_parquet(self, df: pd.DataFrame):
        # 原子替换写：先写到临时文件，再移动覆盖
        tmp_dir = self.path.parent
        with tempfile.NamedTemporaryFile(dir=tmp_dir, prefix=".tmp_", suffix=".parquet", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            df.to_parquet(tmp_path, index=False)
            os.replace(tmp_path, self.path)
        finally:
            try:
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

    # ---- manifest 管理：idempotency_key -> report_json ----

    def _manifest_lookup(self, key: str) -> Optional[dict]:
        if not self.manifest_path.exists():
            return None
        # 简易 JSONL 扫描（MVP；数据量大可建索引）
        try:
            with self.manifest_path.open("r", encoding="utf-8") as f:
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

    def _manifest_write(self, key: str, report: StorageReport) -> None:
        rec = report.model_dump(mode="json")
        # 为避免重复写：若已有记录且相同，直接跳过
        prev = self._manifest_lookup(key)
        if prev is not None:
            return
        with self.manifest_path.open("a", encoding="utf-8") as f:
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
