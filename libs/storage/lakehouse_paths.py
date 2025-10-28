# libs/storage/lakehouse_paths.py
from pathlib import Path
from datetime import date
from typing import Literal

Layer = Literal["bronze", "silver", "gold"]
Dataset = Literal["prices_daily", "prices_monthly", "market_indicators"]

class LakehousePaths:
    """
    Centralized path management for lakehouse structure.
    
    Structure:
        data/{layer}/{dataset}/symbol={symbol}/year={year}/month={month}/data.parquet
    """
    
    def __init__(self, base_dir: str | Path = "data"):
        self.base = Path(base_dir)
        self.bronze = self.base / "bronze"
        self.silver = self.base / "silver"
        self.gold = self.base / "gold"
        self.metadata = self.base / "_metadata"
        
        # Ensure directories exist
        for layer in [self.bronze, self.silver, self.gold, self.metadata]:
            layer.mkdir(parents=True, exist_ok=True)
    
    def get_partition_path(
        self,
        layer: Layer,
        dataset: Dataset,
        symbol: str,
        dt: date,
        *,
        source: str | None = None,
    ) -> Path:
        """
        Get partitioned path for a specific symbol and date.
        
        Example:
            data/bronze/prices_daily/source=yfinance/symbol=AAPL/year=2024/month=01/
        """
        layer_dir = getattr(self, layer)
        path = layer_dir / dataset
        
        # Bronze layer includes source partition
        if layer == "bronze" and source:
            path = path / f"source={source}"
        
        path = path / f"symbol={symbol.upper()}"
        path = path / f"year={dt.year}"
        path = path / f"month={dt.month:02d}"
        
        return path
    
    def get_data_file(
        self,
        layer: Layer,
        dataset: Dataset,
        symbol: str,
        dt: date,
        *,
        source: str | None = None,
    ) -> Path:
        """Get full path to data file."""
        partition_path = self.get_partition_path(layer, dataset, symbol, dt, source=source)
        partition_path.mkdir(parents=True, exist_ok=True)
        return partition_path / "data.parquet"
    
    def get_manifest_path(self, layer: Layer, dataset: Dataset) -> Path:
        """Get manifest file path."""
        manifest_dir = self.metadata / "manifests"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        return manifest_dir / f"{layer}_{dataset}.jsonl"
    
    def list_partitions(
        self,
        layer: Layer,
        dataset: Dataset,
        symbol: str | None = None,
    ) -> list[Path]:
        """List all partition directories."""
        layer_dir = getattr(self, layer)
        base_path = layer_dir / dataset
        
        if symbol:
            base_path = base_path / f"symbol={symbol.upper()}"
        
        if not base_path.exists():
            return []
        
        # Find all data.parquet files
        return list(base_path.rglob("data.parquet"))