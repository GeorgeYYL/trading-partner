# apps/api/routers/prices.py
from datetime import date
from fastapi import APIRouter, Query, Depends
from apps.api.deps import get_ingestion_service
from apps.api.services.prices_ingestion import PricesIngestionService
from libs.adapters.repo_parquet_partitioned import PricesRepoParquetPartitioned

router = APIRouter(prefix="/prices", tags=["prices"])

@router.post("/jobs/daily/range")
def run_ingest_range(
    symbol: str = Query(..., min_length=1),
    date_from: date = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: date = Query(..., description="End date (YYYY-MM-DD)"),
    svc: PricesIngestionService = Depends(get_ingestion_service)
):
    """Fetch prices for a specific date range"""
    report = svc.ingest_window(symbol=symbol, date_from=date_from, date_to=date_to)
    return report.model_dump()

@router.post("/jobs/daily")
def run_ingest(
    symbol: str = Query(..., min_length=1),
    svc: PricesIngestionService = Depends(get_ingestion_service)
):
    """Fetch last 1 year of prices"""
    report = svc.ingest_last_1y(symbol)
    return report.model_dump()

@router.get("/daily")
def get_prices(symbol: str, limit: int = 30):
    repo = PricesRepoParquetPartitioned()
    rows = repo.get_prices(symbol, limit)
    return [r.model_dump() for r in rows]
