"""JSON logging over ``smartretail_common.logging``."""

import logging

from app.core.config import GatewaySettings
from smartretail_common.logging import configure_logging

logger = logging.getLogger("gateway")


def init_logger(settings: GatewaySettings) -> None:
    configure_logging(settings.service_name, settings.log_level)
