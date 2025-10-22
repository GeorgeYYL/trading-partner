# clickhouse.py
from __future__ import annotations

from typing import Optional
from clickhouse_connect import get_client, common
from clickhouse_connect.driver.client import Client
from pydantic_settings import BaseSettings, SettingsConfigDict


class CHSettings(BaseSettings):
    """
    环境变量示例：
      CLICKHOUSE_HOST=localhost
      CLICKHOUSE_PORT=8123
      CLICKHOUSE_USER=default
      CLICKHOUSE_PASSWORD=
      CLICKHOUSE_DATABASE=default
    """
    HOST: str = "localhost"
    PORT: int = 8123
    USER: str = "default"
    PASSWORD: str = ""
    DATABASE: str = "default"

    # pydantic-settings v2 风格
    model_config = SettingsConfigDict(env_prefix="CLICKHOUSE_")


def ch_client(cfg: Optional[CHSettings] = None, *, secure: bool = False, timeout: int = 15) -> Client:
    """
    创建 ClickHouse 连接。
    - secure=True 时用 https(9440)/TLS；False 用 http(8123)。
    - timeout：请求超时（秒）。
    """
    cfg = cfg or CHSettings()
    params = {
        "host": cfg.HOST,
        "port": cfg.PORT,
        "username": cfg.USER,
        "password": cfg.PASSWORD,
        "database": cfg.DATABASE,
        "secure": bool(secure),
        "connect_timeout": timeout,
        "send_receive_timeout": timeout,
        "compress": True,
    }
    # 避免无意义的 None 传入
    params = {k: v for k, v in params.items() if v is not None}
    return get_client(**params)


DDL = """
CREATE TABLE IF NOT EXISTS prices_daily
(
    symbol     LowCardinality(String),
    timestamp  DateTime64(3, 'UTC'),
    open       Float64,
    high       Float64,
    low        Float64,
    close      Float64,
    volume     UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, timestamp)
SETTINGS index_granularity = 8192
"""


def ensure_schema(client: Client) -> None:
    """创建所需表结构（幂等）。"""
    client.command(DDL)


def insert_prices_daily(client: Client, df) -> int:
    """
    将 DataFrame 直接写入 ClickHouse。
    期望列：['symbol','timestamp','Open','High','Low','Close','Volume']（大小写与前面抓取函数一致）。
    """
    if df is None or df.empty:
        return 0

    # 列映射到表结构（小写）
    cols = ["symbol", "timestamp", "open", "high", "low", "close", "volume"]
    # 统一重命名一份副本，避免就地修改
    pdf = df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    missing = [c for c in ["symbol", "timestamp", "open", "high", "low", "close", "volume"] if c not in pdf.columns]
    if missing:
        raise ValueError(f"insert_prices_daily: missing columns: {missing}")

    # pandas 的 Timestamp -> python 原生，clickhouse-connect 会自动转 DateTime64
    data = [tuple(row[c] for c in cols) for _, row in pdf.iterrows()]
    client.insert("prices_daily", data, column_names=cols)
    return len(data)
