# check.py
from __future__ import annotations

from typing import List
import pandas as pd

try:
    import great_expectations as gx
except Exception as e:
    raise ImportError(
        "Great Expectations is required. Install with: pip install great_expectations"
    ) from e


REQUIRED_COLS: List[str] = ["timestamp", "Open", "High", "Low", "Close", "Volume"]

def validate_daily(df: pd.DataFrame) -> None:
    """
    Validate daily OHLCV dataframe using Great Expectations (GX 0.18+ style).

    Expected columns (case-sensitive):
        ['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'] (+ optional 'symbol')
    Raises AssertionError if validation fails.
    """
    # 0) 先做最快速的结构校验，避免后面报错不明确
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise AssertionError(f"Missing required columns: {missing}. Got: {list(df.columns)}")

    # 1) 建立 ephemeral 上下文 & 从 DataFrame 构建 batch
    ctx = gx.get_context(mode="ephemeral")

    # 使用 pandas 源把 DataFrame 作为资产喂给 GX
    pandas_src = ctx.sources.add_or_update_pandas(name="in_memory_src")
    asset = pandas_src.add_dataframe_asset(name="daily_bars")
    batch_request = asset.build_batch_request(dataframe=df)

    # 2) 准备/加载期望集合与校验器（Validator）
    suite_name = "daily_suite"
    suite = ctx.add_or_update_expectation_suite(suite_name)
    validator = ctx.get_validator(batch_request=batch_request, expectation_suite=suite)

    # 3) 期望规则（基础质量）
    # 3.1 列集合（至少包含这些列）
    validator.expect_table_columns_to_match_set(column_set=set(REQUIRED_COLS), exact_match=False)

    # 3.2 非空检查
    validator.expect_column_values_to_not_be_null("timestamp")
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        validator.expect_column_values_to_not_be_null(col)

    # 3.3 类型/取值范围（对 pandas 兼容：float32/float64/Int64/int64 都可）
    float_types = ["float64", "float32", "float16"]
    int_types = ["int64", "Int64", "int32"]
    for col in ["Open", "High", "Low", "Close"]:
        validator.expect_column_values_to_be_in_type_list(col, type_list=float_types + int_types)
        validator.expect_column_values_to_be_greater_than_or_equal_to(col, 0)

    validator.expect_column_values_to_be_in_type_list("Volume", type_list=int_types + float_types)
    validator.expect_column_values_to_be_greater_than_or_equal_to("Volume", 0)

    # 3.4 价格关系：High >= Low
    validator.expect_column_pair_values_A_to_be_greater_than_or_equal_to_B("High", "Low")

    # 3.5 时间戳应递增（允许相等以容忍同日多条情况；如需严格递增设 strictly=True）
    validator.expect_column_values_to_be_increasing("timestamp", strictly=False)

    # 4) 执行验证并判断是否成功
    result = validator.validate()
    if not result.success:
        # 汇总失败的期望，给出可读信息
        failed = []
        for r in result.results:
            st = r.get("success")
            if st is False:
                exp_type = r.get("expectation_config", {}).get("expectation_type", "unknown")
                kwargs = r.get("expectation_config", {}).get("kwargs", {})
                failed.append(f"- {exp_type} {kwargs}")
        detail = "\n".join(failed[:15])  # 最多展示前15条，避免日志过长
        raise AssertionError(f"Data quality checks failed. Failed expectations:\n{detail}")
