"""Builds the order repos, and hands out their collaborators, for a request."""

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.db import get_db
from app.core.dependencies import get_settings
from app.v1.orders.clients.catalog import CatalogClient
from app.v1.orders.idempotency_repo import IdempotencyRepo
from app.v1.orders.repo import OrderRepo
from app.v1.orders.shipment_repo import ShipmentRepo
from app.v1.orders.workflows.client import TemporalOrderWorkflows


def order_repo(
    session: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings)
) -> OrderRepo:
    return OrderRepo(session, settings)


def shipment_repo(
    session: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings)
) -> ShipmentRepo:
    return ShipmentRepo(session, settings)


def idempotency_repo(
    session: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings)
) -> IdempotencyRepo:
    return IdempotencyRepo(session, settings)


def catalog_client(request: Request) -> CatalogClient:
    client: CatalogClient = request.app.state.catalog
    return client


def order_workflows(request: Request) -> TemporalOrderWorkflows:
    workflows: TemporalOrderWorkflows = request.app.state.workflows
    return workflows
