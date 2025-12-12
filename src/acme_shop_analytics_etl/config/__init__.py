"""
Configuration module for AcmeShop Analytics ETL.

Provides centralized configuration management and feature flags.
"""
from acme_shop_analytics_etl.config.settings import Settings, get_settings
from acme_shop_analytics_etl.config.feature_flags import (
    is_legacy_etl_enabled,
    is_v1_schema_enabled,
    FeatureFlags,
)

__all__ = [
    "Settings",
    "get_settings",
    "is_legacy_etl_enabled",
    "is_v1_schema_enabled",
    "FeatureFlags",
]
