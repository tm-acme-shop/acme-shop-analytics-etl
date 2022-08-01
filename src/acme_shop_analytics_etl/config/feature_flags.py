"""
Feature Flags

Controls feature toggles for gradual rollout and migration between
legacy and new implementations.
"""
import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Callable


def _parse_bool(value: str) -> bool:
    """Parse a string value as a boolean."""
    return value.lower() in ("true", "1", "yes", "on")


@dataclass
class FeatureFlags:
    """
    Feature flag configuration for the ETL pipeline.
    
    These flags control the behavior of ETL jobs during the migration
    from legacy to v2 implementations.
    """
    
    enable_legacy_etl: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("ENABLE_LEGACY_ETL", "true")
        )
    )
    
    enable_v1_schema: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("ENABLE_V1_SCHEMA", "true")
        )
    )
    
    enable_legacy_payments: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("ENABLE_LEGACY_PAYMENTS", "false")
        )
    )
    
    enable_legacy_pii: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("ENABLE_LEGACY_PII", "true")
        )
    )


@lru_cache(maxsize=1)
def get_feature_flags() -> FeatureFlags:
    """
    Get the feature flags singleton.
    
    Uses lru_cache to ensure flags are only loaded once per process.
    
    Returns:
        FeatureFlags: The feature flags instance.
    """
    return FeatureFlags()


def is_legacy_etl_enabled() -> bool:
    """
    Check if legacy ETL pipelines are enabled.
    
    When enabled, ETL jobs will use the legacy extraction and transformation
    logic alongside or instead of v2 implementations.
    
    Returns:
        bool: True if legacy ETL is enabled.
    """
    return get_feature_flags().enable_legacy_etl


def is_v1_schema_enabled() -> bool:
    """
    Check if v1 data schema is enabled.
    
    When enabled, ETL jobs will read from and write to v1 schema tables.
    This flag controls the data model version used for transformations.
    
    Returns:
        bool: True if v1 schema is enabled.
    """
    return get_feature_flags().enable_v1_schema


def is_legacy_payments_enabled() -> bool:
    """
    Check if legacy payments integration is enabled.
    
    Controls whether the ETL uses the old payments client or the new
    payments service integration.
    
    Returns:
        bool: True if legacy payments is enabled.
    """
    return get_feature_flags().enable_legacy_payments


def is_legacy_pii_enabled() -> bool:
    """
    Check if legacy PII handling is enabled.
    
    When enabled, PII is processed using the legacy approach (masking).
    When disabled, PII uses the new tokenization approach.
    
    Returns:
        bool: True if legacy PII handling is enabled.
    """
    return get_feature_flags().enable_legacy_pii
