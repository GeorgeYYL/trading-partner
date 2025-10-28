# libs/storage/config.py
from pydantic_settings import BaseSettings
from typing import Literal

class LakehouseConfig(BaseSettings):
    """Lakehouse configuration."""
    
    # Storage backend
    STORAGE_BACKEND: Literal["local", "s3", "gcs"] = "local"
    
    # Local paths
    DATA_DIR: str = "data"
    
    # Partitioning strategy
    PARTITION_BY: list[str] = ["symbol", "year", "month"]
    
    # Retention policy
    BRONZE_RETENTION_DAYS: int = 365  # Keep raw data for 1 year
    SILVER_RETENTION_DAYS: int = 1825  # Keep clean data for 5 years
    
    # Compaction settings
    ENABLE_COMPACTION: bool = True
    COMPACTION_THRESHOLD_FILES: int = 10  # Compact when >10 small files
    
    class Config:
        env_prefix = "LAKEHOUSE_"
