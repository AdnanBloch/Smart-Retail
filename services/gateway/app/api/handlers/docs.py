"""Merged OpenAPI document and Swagger UI. Never proxied, exempt from auth."""

from fastapi import APIRouter, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse, JSONResponse

router = APIRouter(include_in_schema=False)


async def get_openapi(request: Request) -> JSONResponse:
    return JSONResponse(await request.app.state.openapi.document())


async def get_docs() -> HTMLResponse:
    return get_swagger_ui_html(openapi_url="/openapi.json", title="SmartRetail API")
