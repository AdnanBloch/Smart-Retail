"""Wiring every service applies to its FastAPI app."""

from fastapi import FastAPI

from smartretail_common.context import RequestContextMiddleware
from smartretail_common.errors import install_error_handlers


def install_platform(app: FastAPI, *, adopt_inbound_request_id: bool = True) -> None:
    install_error_handlers(app)
    app.add_middleware(RequestContextMiddleware, adopt_inbound=adopt_inbound_request_id)
