# apps/api/routers/prices.py
from fastapi import APIRouter, Query
from apps.api.services.prices_ingestion import PricesIngestionService
from libs.adapters.repo_parquet import PricesRepoParquet

router = APIRouter(prefix="/prices", tags=["prices"])

@router.post("/jobs/daily")
def run_ingest(symbol: str = Query(..., min_length=1)):
    svc = PricesIngestionService()
    result = svc.ingest_last_1y(symbol)
    return result

@router.get("/daily")
def get_prices(symbol: str, limit: int = 30):
    repo = PricesRepoParquet()
    rows = repo.get_prices(symbol, limit)
    return [r.model_dump() for r in rows]
