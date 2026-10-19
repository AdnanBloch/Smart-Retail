"""Merged OpenAPI document and Swagger UI. Never proxied, exempt from auth."""

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

router = APIRouter(include_in_schema=False)

# Swagger UI with auto-authorize on login and silent token refresh (see the page's script).
_SWAGGER_HTML = (Path(__file__).parent / "swagger.html").read_text(encoding="utf-8")


@router.get("/openapi.json")
async def get_openapi(request: Request) -> JSONResponse:
    return JSONResponse(await request.app.state.openapi.document())


@router.get("/docs")
async def get_docs() -> HTMLResponse:
    return HTMLResponse(_SWAGGER_HTML)
