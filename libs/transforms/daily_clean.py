# daily_clean.py
from __future__ import annotations

import pandas as pd


def clean_daily(df: pd.DataFrame, *, prefer_adj_close: bool = False) -> pd.DataFrame:
    """
    Standardize daily OHLCV dataframe.

    Output columns (in order):
        ['symbol'(optional), 'timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']

    Parameters
    ----------
    prefer_adj_close : bool
        If True and 'Adj Close' exists, use it as 'Close'.
    """
    if df is None or df.empty:
        # 返回规范但空的框架
        cols = ["timestamp", "Open", "High", "Low", "Close", "Volume"]
        if "symbol" in df.columns if df is not None else []:
            cols = ["symbol"] + cols
        return pd.DataFrame(columns=cols)

    df = df.copy()

    # ---- 1) 统一时间列：优先 timestamp，其次 Date/date/Timestamp ----
    ts_col = None
    for cand in ["timestamp", "Timestamp", "Date", "date", "datetime", "Datetime"]:
        if cand in df.columns:
            ts_col = cand
            break
    if ts_col is None:
        raise ValueError(f"clean_daily: missing timestamp/date column in {list(df.columns)}")

    # 标准名为 'timestamp'
    if ts_col != "timestamp":
        df = df.rename(columns={ts_col: "timestamp"})

    # 解析为 UTC
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    if df["timestamp"].isna().all():
        raise ValueError("clean_daily: all timestamps failed to parse")

    # ---- 2) 统一价格与成交量列名 ----
    # 若选择优先使用复权收盘价
    rename_map = {}
    if prefer_adj_close and "Adj Close" in df.columns:
        rename_map["Adj Close"] = "Close"

    # 常见别名兼容（小写或不同大小写）
    alt_map = {
        "open": "Open", "high": "High", "low": "Low", "close": "Close",
        "volume": "Volume", "adj close": "Close", "adj_close": "Close",
    }
    for c in list(df.columns):
        lc = c.lower()
        if lc in alt_map and alt_map[lc] not in df.columns:
            rename_map[c] = alt_map[lc]

    if rename_map:
        df = df.rename(columns=rename_map)

    required_price_cols = ["Open", "High", "Low", "Close"]
    missing_price = [c for c in required_price_cols if c not in df.columns]
    if missing_price:
        raise ValueError(f"clean_daily: missing required price columns: {missing_price}")

    # Volume 可选，缺失则补 0
    if "Volume" not in df.columns:
        df["Volume"] = 0

    # ---- 3) 丢弃关键列缺失的行、类型与取值清洗 ----
    df = df.dropna(subset=["timestamp"] + required_price_cols)

    # 数值列强制为数值
    for c in required_price_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)

    # 去掉价格仍为空的行
    df = df.dropna(subset=required_price_cols)

    # Volume 用可空整型，避免大数溢出与缺失问题
    # 先确保非负
    df["Volume"] = df["Volume"].clip(lower=0).astype("Int64")

    # ---- 4) 排序与列顺序 ----
    df = df.sort_values("timestamp").reset_index(drop=True)

    out_cols = ["timestamp", "Open", "High", "Low", "Close", "Volume"]
    if "symbol" in df.columns:
        # 保持 symbol 在最前
        out_cols = ["symbol"] + out_cols

    return df[out_cols]
