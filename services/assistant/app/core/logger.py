"""JSON logging over ``smartretail_common.logging``."""

import logging

from app.core.config import Settings
from smartretail_common.logging import configure_logging

logger = logging.getLogger("assistant")


def init_logger(settings: Settings) -> None:
    configure_logging(settings.service_name, settings.log_level)
