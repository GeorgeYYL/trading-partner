# libs/connectors/registry.py
from typing import Callable, Literal,Dict
import pandas as pd
from . import yfinance_fetcher  # 你刚移动的脚本
from .base import FetcherPort
from .yfinance_fetcher import YFinanceFetcher

_REGISTRY = {
    "yfinance": YFinanceFetcher(),
    # "alpaca": AlpacaFetcher(...),
}

FetchSource = Literal["yfinance"]              # 以后加: "alpaca", "polygon" ...
FetcherFn = Callable[[str], list]

def get_prices_fetcher(source: FetchSource = "yfinance") -> FetcherFn:
    if source == "yfinance":
        return yfinance_fetcher.fetch_last_1y_daily
    raise ValueError(f"Unknown fetch source: {source}")

def get_fetcher(name: str) -> FetcherPort:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise ValueError(f"unknown fetcher: {name!r}")

def list_fetchers() -> list[str]:
    return list(_REGISTRY.keys())