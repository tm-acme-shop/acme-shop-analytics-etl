"""
Configuration module for AcmeShop Analytics ETL.

Provides centralized configuration management.
"""
from acme_shop_analytics_etl.config.settings import Settings, get_settings

__all__ = [
    "Settings",
    "get_settings",
]
