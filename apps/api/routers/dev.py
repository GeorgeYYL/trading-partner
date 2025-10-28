# apps/api/routers/dev.py
from fastapi import APIRouter, Request
from fastapi.openapi.utils import get_openapi

router = APIRouter(prefix="/_dev", tags=["_dev"])

@router.post("/reload-openapi")
def reload_openapi(req: Request):
    app = req.app
    app.openapi_schema = None
    # 立刻生成一次（可选）
    get_openapi(title=app.title, version=app.version, routes=app.routes)
    return {"ok": True, "routes": len(app.routes)}
