# connectors/yfinance_fetcher.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional, List
import pandas as pd
from pandas.api.types import is_numeric_dtype, is_float_dtype
import yfinance as yf
from libs.contracts.prices_daily import PriceRow, validate_prices_batch
from .base import FetcherPort


@dataclass(slots=True)
class YFinanceFetcher(FetcherPort):
    """
    YFinance 实现的日线抓取器（满足 FetcherPort 契约）

    - source_name: 用于血缘/日志
    - fetch_daily(symbol, date_from, date_to) -> DataFrame
      返回列: ['date','open','high','low','close','adj_close','volume','symbol']
      语义: 闭区间过滤，date 为 Python date（UTC），volume 为可空整型(Int64)

    可配置参数:
    - auto_adjust: 是否让 yfinance 自动复权（默认 False；我们返回 adj_close 字段）
    - default_period_days: 当未显式给出 date_from/date_to 时，默认拉取的天数（默认 365）
    """

    auto_adjust: bool = False
    default_period_days: int = 365

    # ---- Port 属性 ----
    source_name: str = "yfinance"

    # ---- 主入口 ----
    def fetch_daily(
        self,
        symbol: str,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
    ) -> List[PriceRow]:
        sym = symbol.upper().strip()
        if not sym:
            raise ValueError("symbol must be non-empty")

        # 计算查询窗口（闭区间）。未给出时用默认最近 N 天。
        if date_to is None:
            now_utc = datetime.now(timezone.utc).date()
            date_to = now_utc
        if date_from is None:
            date_from = date_to - timedelta(days=self.default_period_days)

        if date_from > date_to:
            raise ValueError("date_from must be <= date_to")

        # yfinance 支持 start/end，end 为“半开区间”的结束时刻；我们传 end+1 日确保闭区间
        yf_start = datetime.combine(date_from, datetime.min.time()).astimezone(timezone.utc)
        yf_end_exclusive = datetime.combine(date_to + timedelta(days=1), datetime.min.time()).astimezone(timezone.utc)

        df = yf.download(
            tickers=sym,
            start=yf_start,
            end=yf_end_exclusive,
            interval="1d",
            auto_adjust=self.auto_adjust,
            threads=False,     # 这里关掉多线程，稳定一些
            progress=False,
        )

        if df is None or df.empty:
            # 统一抛错，上层 Service 负责打日志并映射
            raise RuntimeError(f"No data returned for {sym!r} from yfinance")

        df = self._flatten_columns(df)
        df = self._normalize_index_to_utc_date(df)
        df = self._rename_and_align_columns(df)
        df = self._slice_closed_range(df, date_from, date_to)
        df = self._coerce_dtypes(df)

        # 添加 symbol，并保证输出列顺序
        df["symbol"] = sym
        cols = ["date", "symbol", "open", "high", "low", "close", "adj_close", "volume"]
        # 确保所有列都存在（缺失填 NA），并按日期升序
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[cols].sort_values("date").reset_index(drop=True)

        # 契约验证（df -> list[dict] -> DTO 校验）
        rows_as_dict = [
            {
                "date": r.date,
                "symbol": r.symbol,
                "open": float(r.open) if pd.notna(r.open) else 0.0,        # 这里决定 NA 策略：兜底或报错
                "high": float(r.high) if pd.notna(r.high) else 0.0,
                "low":  float(r.low)  if pd.notna(r.low)  else 0.0,
                "close": float(r.close) if pd.notna(r.close) else 0.0,
                "adj_close": float(r.adj_close) if pd.notna(r.adj_close) else float(r.close or 0.0),
                "volume": int(r.volume) if pd.notna(r.volume) else 0,
            }
            for r in df.itertuples(index=False)
        ]
        validated = validate_prices_batch(rows_as_dict)

        return validated

    # ---- 辅助函数：保持小而清晰 ----
    @staticmethod
    def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
        """拍平 MultiIndex 列；若有多 ticker，优先选择字段层次，保留单层字段名。"""
        if isinstance(df.columns, pd.MultiIndex):
            # yfinance 多 ticker 常见列形如 ('Open','AAPL')；我们保留字段层 level=0
            try:
                df = df.copy()
                df.columns = df.columns.get_level_values(0)
            except Exception:
                # 兜底：拼接列名
                df = df.copy()
                df.columns = ["_".join([str(x) for x in tup if x not in (None, "")]) for tup in df.columns.to_list()]
        return df

    @staticmethod
    def _normalize_index_to_utc_date(df: pd.DataFrame) -> pd.DataFrame:
        """将索引转为 UTC Datetime，再规范化为当日 00:00:00（用于取出 Python date）。"""
        out = df.copy()
        idx = pd.to_datetime(out.index, utc=True, errors="coerce")
        mask = idx.notna()
        out = out.loc[mask]
        out.index = idx[mask].normalize()  # 置 00:00:00+00:00
        # 同时保留一个日期列（后续会覆盖/重建）
        out["date"] = out.index.date
        return out

    @staticmethod
    def _rename_and_align_columns(df: pd.DataFrame) -> pd.DataFrame:
        """把列名统一成契约字段，并保证这些列存在。"""
        out = df.copy()
        rename_map = {
            "Open": "open", "open": "open",
            "High": "high", "high": "high",
            "Low": "low", "low": "low",
            "Close": "close", "close": "close",
            "Adj Close": "adj_close", "AdjClose": "adj_close",
            "adj close": "adj_close", "adj_close": "adj_close",
            "Volume": "volume", "volume": "volume",
        }
        out.rename(columns={c: rename_map.get(c, c) for c in out.columns}, inplace=True)

        # volume 如果是二维，取第一列；若无则补列
        if "volume" in out.columns and isinstance(out["volume"], pd.DataFrame):
            out["volume"] = out["volume"].iloc[:, 0]
        if "volume" not in out.columns:
            out["volume"] = pd.NA

        # 确保关键价格列存在
        for c in ["open", "high", "low", "close", "adj_close"]:
            if c not in out.columns:
                out[c] = pd.NA

        # 确保 date 列存在（若之前没建）
        if "date" not in out.columns:
            out["date"] = pd.to_datetime(out.index, utc=True, errors="coerce").date

        return out

    @staticmethod
    def _slice_closed_range(df: pd.DataFrame, date_from: date, date_to: date) -> pd.DataFrame:
        """按闭区间裁剪（包含端点）。"""
        out = df.copy()
        # 这里的 df['date'] 已经是 Python date
        mask = (out["date"] >= date_from) & (out["date"] <= date_to)
        return out.loc[mask]

    @staticmethod
    def _coerce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
        """价格列转 float，成交量转可空 Int64，date 保持 Python date。"""
        out = df.copy()
        for c in ["open", "high", "low", "close", "adj_close"]:
            if not is_numeric_dtype(out[c]):
                out[c] = pd.to_numeric(out[c], errors="coerce")
            # 如果是字符串或混合，coerce 成 float
            # 如果需要统一到 float（哪怕原本是 int），再兜底一次
            if not is_float_dtype(out[c]):
                out[c] = out[c].astype("float64")
        out["volume"] = pd.to_numeric(out["volume"], errors="coerce").astype("Int64")
        # date 已经是 Python date（在 _normalize_index_to_utc_date 中处理）
        return out
