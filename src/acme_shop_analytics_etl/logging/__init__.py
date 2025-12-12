"""
Logging module for AcmeShop Analytics ETL.

Provides both legacy and structured logging utilities.
"""
from acme_shop_analytics_etl.logging.structured_logging import (
    get_logger,
    configure_logging,
    LogContext,
)

__all__ = [
    "get_logger",
    "configure_logging",
    "LogContext",
]
