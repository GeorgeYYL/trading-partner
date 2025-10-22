# s3_client.py
from __future__ import annotations

import boto3
from botocore.client import BaseClient
from pydantic_settings import BaseSettings, SettingsConfigDict


class S3Settings(BaseSettings):
    """
    环境变量示例：
      S3_ENDPOINT_URL=https://s3.ap-southeast-2.amazonaws.com
      S3_ACCESS_KEY=AKIAxxxx
      S3_SECRET_KEY=xxxx
      S3_BUCKET_RAW=trading-raw
      S3_BUCKET_CLEAN=trading-clean
      S3_REGION=ap-southeast-2
    """

    ENDPOINT_URL: str
    ACCESS_KEY: str
    SECRET_KEY: str
    BUCKET_RAW: str
    BUCKET_CLEAN: str
    REGION: str | None = None  # 可选：AWS/MinIO 区域

    # pydantic-settings v2 规范
    model_config = SettingsConfigDict(
        env_prefix="S3_",
        env_nested_delimiter="__",
    )


def get_s3(settings: S3Settings | None = None) -> BaseClient:
    """
    获取 boto3 S3 客户端。

    若不传 settings，会自动从环境变量读取（通过 S3Settings）。
    支持 AWS S3、MinIO、Supabase Storage 等兼容 S3 的服务。
    """
    settings = settings or S3Settings()

    session = boto3.session.Session()
    client = session.client(
        "s3",
        endpoint_url=settings.ENDPOINT_URL,
        aws_access_key_id=settings.ACCESS_KEY,
        aws_secret_access_key=settings.SECRET_KEY,
        region_name=settings.REGION,
        config=boto3.session.Config(s3={"addressing_style": "path"}),  # MinIO 兼容
    )
    return client


# ✅ 示例：上传 DataFrame CSV
def upload_bytes(client: BaseClient, bucket: str, key: str, data: bytes, content_type: str = "text/csv") -> None:
    """
    简单上传二进制数据到 S3。
    例：upload_bytes(s3, 'my-bucket', 'AAPL.csv', df.to_csv(index=False).encode())
    """
    client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)


# ✅ 示例：下载为字节串
def download_bytes(client: BaseClient, bucket: str, key: str) -> bytes:
    """
    从 S3 下载文件为字节串。
    """
    obj = client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()
