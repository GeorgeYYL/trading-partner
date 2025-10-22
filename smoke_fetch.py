# scripts/test_ingest.py
"""
Smoke test for end-to-end ingestion:
- Build service via deps (DI: repo + fetcher + logger + clock)
- Call ingest_last_1y(symbol)
- Print structured result and some quick sanity info

Run:
    $ python scripts/test_ingest.py
    # or
    $ python scripts/test_ingest.py --symbol AAPL
"""

from __future__ import annotations

import os
import sys
import argparse
from typing import Any


# --- Step 1. Ensure project root is importable ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)


def main():
    # --- Step 2. CLI args ---
    parser = argparse.ArgumentParser(description="End-to-end smoke test for ingestion service.")
    parser.add_argument("--symbol", type=str, default=os.getenv("TEST_SYMBOL", "AAPL"),
                        help="Ticker symbol to ingest (default: AAPL or TEST_SYMBOL env).")
    args = parser.parse_args()

    # --- Step 3. Import deps & build service via DI ---
    try:
        # deps.get_ingestion_service() 应该内部完成：选择 repo 实现 + 从 registry 选择 fetcher
        from apps.api.deps import get_ingestion_service  # type: ignore
    except ModuleNotFoundError as e:
        print("❌ Cannot import apps.api.deps.get_ingestion_service")
        print("   确认你从项目根目录运行：`cd <project-root> && python scripts/test_ingest.py`")
        raise e

    svc = get_ingestion_service()

    # --- 可选：展示当前装配的实现（便于理解 DI 起作用了） ---
    fetcher_name = getattr(getattr(svc, "fetcher", None), "source_name", "?")
    repo_impl = type(getattr(svc, "repo", object())).__name__
    print("🔧 IngestionService wiring")
    print(f"   fetcher: {fetcher_name}")
    print(f"   repo   : {repo_impl}")
    print()

    # --- Step 4. Call ingest ---
    symbol = args.symbol.upper().strip()
    print(f"▶ Ingest last 1y for: {symbol}")
    try:
        result: dict[str, Any] = svc.ingest_last_1y(symbol)  # 触发：抓取→校验→upsert
        print("\n✅ Ingest finished. Result:")
        # 只打印关键字段，避免过长
        useful = {k: result.get(k) for k in ("symbol", "rows", "inserted", "updated")}
        print(useful)

        # 说明：结构化日志（structlog）会由 Service 自己打印到控制台
        print("\nℹ️  说明：如果已配置 structlog，你应在控制台看到类似：")
        print("    {\"event\":\"ingest.done\",\"symbol\":\"AAPL\", ... ,\"duration_ms\":xxx}")
    except Exception as e:
        print("\n❌ Ingest failed with error:")
        print(repr(e))
        print("   - 网络/数据源失败：检查网络或稍后再试")
        print("   - 校验失败：检查 fetcher 输出是否符合契约（8 列、类型正确）")
        print("   - 落库异常：检查 parquet 路径/写权限/主键幂等逻辑")
        raise


if __name__ == "__main__":
    main()
