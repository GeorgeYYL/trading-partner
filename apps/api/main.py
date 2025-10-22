# apps/api/main.py
from fastapi import FastAPI
from libs.observability.logging import setup_logging
from apps.api.routers import health, prices
from fastapi.responses import HTMLResponse, RedirectResponse

app = FastAPI(title="trading-partner API")
setup_logging()

@app.get("/", response_class=HTMLResponse)
def root():
    return """
    <html><body>
      <h1>Trading Partner API</h1>
      <p>See <a href="/docs">/docs</a> for Swagger UI.</p>
    </body></html>
    """
@app.get("/healthz")
def healthz():
    return {"status": "ok"}

app.include_router(health.router)
app.include_router(prices.router)
