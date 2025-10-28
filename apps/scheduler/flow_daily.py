# apps/workers/tasks/run_daily_pipeline.py 之类的位置
from prefect import flow, task
import io
import os
import pandas as pd  # 你用到了 pd.DataFrame，所以需要导入

# 这些函数/对象按你项目里的实际路径导入（保持不变）
from libs.connectors.market import fetch_daily_alpaca, fetch_daily_yf  # 示例路径
from libs.transforms.clean import clean_daily                          # 示例路径
from libs.storage.s3 import get_s3, S3Settings                         # 示例路径
from libs.storage.clickhouse import ch_client, CHSettings, DDL         # 示例路径


@task
def fetch(symbol: str) -> pd.DataFrame:
    try:
        return fetch_daily_alpaca(symbol)
    except Exception:
        return fetch_daily_yf(symbol)


@task
def transform(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    c = clean_daily(df)
    c.insert(0, "symbol", symbol)
    return c


@task
def quality(df: pd.DataFrame) -> None:
    # keep minimal: check non-empty
    if df.empty:
        raise ValueError("No data fetched")


@task
def write_raw(df: pd.DataFrame, symbol: str) -> None:
    s3 = get_s3(S3Settings())
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(
        Bucket=os.getenv("S3_BUCKET_RAW", "raw"),
        Key=f"prices_daily/{symbol}.parquet",
        Body=buf,
    )


@task
def write_clean(df: pd.DataFrame, symbol: str) -> None:
    s3 = get_s3(S3Settings())
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(
        Bucket=os.getenv("S3_BUCKET_CLEAN", "clean"),
        Key=f"prices_daily/{symbol}.parquet",
        Body=buf,
    )


@task
def upsert_clickhouse(df: pd.DataFrame) -> None:
    client = ch_client(CHSettings())
    client.command(DDL)
    client.insert_df("prices_daily", df)


@task
def export_csv(df: pd.DataFrame, symbol: str) -> str:
    # Use lakehouse gold layer for reports
    out = f"data/gold/reports/{symbol}_daily_report.csv"
    os.makedirs("data/gold/reports", exist_ok=True)
    df.to_csv(out, index=False)
    return out


@flow(name="daily-bars-pipeline")
def run(symbol: str = "AAPL") -> str:
    raw = fetch(symbol)
    clean = transform(raw, symbol)
    quality(clean)
    write_raw(raw, symbol)
    write_clean(clean, symbol)
    upsert_clickhouse(clean)
    path = export_csv(clean, symbol)
    return path


if __name__ == "__main__":
    print(run())
