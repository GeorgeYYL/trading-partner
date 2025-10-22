import os
from typing import Optional, Union
import pandas as pd
import requests
from datetime import datetime, date, timezone

ALPACA_BASE = "https://data.alpaca.markets/v2"


def _to_iso8601_z(dt: Union[str, datetime, date, None]) -> Optional[str]:
    """Convert str/date/datetime -> RFC3339/ISO8601 Zulu (e.g. 2024-01-01T00:00:00Z)."""
    if dt is None:
        return None
    if isinstance(dt, str):
        # 若已是 ISO8601，直接返回；否则尽力解析
        try:
            # 优先尝试 fromisoformat（允许 “YYYY-MM-DD”）
            if "T" in dt or " " in dt:
                # 含时间的字符串
                parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
            else:
                # 仅日期
                parsed = datetime.fromisoformat(dt)  # 可能是 YYYY-MM-DD
                parsed = datetime(parsed.year, parsed.month, parsed.day)
        except Exception:
            # 兜底用 pandas 解析
            parsed = pd.to_datetime(dt, utc=False).to_pydatetime()
    elif isinstance(dt, date) and not isinstance(dt, datetime):
        parsed = datetime(dt.year, dt.month, dt.day)
    else:
        parsed = dt

    # 统一设为 UTC 再转 Z 结尾
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)

    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_daily_alpaca(
    symbol: str,
    start: Union[str, datetime, date, None] = None,
    end: Union[str, datetime, date, None] = None,
    *,
    limit_per_page: int = 1000,
    timeout_sec: int = 30,
) -> pd.DataFrame:
    """
    Fetch daily bars from Alpaca Data v2.

    Returns a DataFrame with columns: ['timestamp','Open','High','Low','Close','Volume'].
    Sorted by timestamp ascending. Empty DataFrame if no data.
    """
    key = os.getenv("ALPACA_KEY")
    secret = os.getenv("ALPACA_SECRET")
    if not key or not secret:
        raise RuntimeError("Missing ALPACA_KEY/ALPACA_SECRET; use yfinance fallback")

    params = {
        "timeframe": "1Day",
        "limit": int(limit_per_page),
        "adjustment": "raw",     # 可选: 'raw' / 'all' / 'split' / 'dividend'
        # "feed": "sip",         # 若你的订阅允许，可显式指定
    }

    s = _to_iso8601_z(start)
    e = _to_iso8601_z(end)
    if s:
        params["start"] = s
    if e:
        params["end"] = e

    headers = {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
        "Accept": "application/json",
    }

    all_rows = []
    page_token = None

    while True:
        if page_token:
            params["page_token"] = page_token
        else:
            params.pop("page_token", None)

        url = f"{ALPACA_BASE}/stocks/{symbol}/bars"
        r = requests.get(url, headers=headers, params=params, timeout=timeout_sec)
        try:
            r.raise_for_status()
        except requests.HTTPError as exc:
            # 带上服务端返回的错误信息，便于定位
            raise RuntimeError(
                f"Alpaca request failed: {exc}; "
                f"status={r.status_code}, body={r.text[:500]}"
            ) from exc

        payload = r.json() or {}
        bars = payload.get("bars", [])
        if not bars:
            break

        all_rows.extend(bars)
        page_token = payload.get("next_page_token")
        if not page_token:
            break

    if not all_rows:
        # 返回一个带有目标列但为空的 DataFrame，更方便下游处理
        return pd.DataFrame(columns=["timestamp", "Open", "High", "Low", "Close", "Volume"])

    df = pd.DataFrame(all_rows)

    # Alpaca v2 字段：t(时间戳)、o/h/l/c/v、(可有 n,vw 等)
    rename_map = {
        "t": "timestamp",
        "o": "Open",
        "h": "High",
        "l": "Low",
        "c": "Close",
        "v": "Volume",
    }
    df = df.rename(columns=rename_map)

    # 仅保留我们关心的列（若存在）
    keep_cols = [c for c in ["timestamp", "Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    df = df[keep_cols]

    # 转换类型
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    for col in ["Open", "High", "Low", "Close"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").astype("Int64")

    # 按时间升序
    df = df.sort_values("timestamp").reset_index(drop=True)

    return df
