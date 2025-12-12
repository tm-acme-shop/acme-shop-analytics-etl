"""
Feature Flags

Controls feature toggles for gradual rollout and migration between
legacy and new implementations.

TODO(TEAM-PLATFORM): Consider migrating to a proper feature flag service
like LaunchDarkly or Unleash for production use.
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
    
    # TODO(TEAM-PLATFORM): Disable after v2 migration complete (Q2 2025)
    enable_legacy_etl: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("ENABLE_LEGACY_ETL", "true")
        )
    )
    
    # TODO(TEAM-API): Disable after schema migration complete
    enable_v1_schema: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("ENABLE_V1_SCHEMA", "true")
        )
    )
    
    # TODO(TEAM-PAYMENTS): Legacy payments integration
    enable_legacy_payments: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("ENABLE_LEGACY_PAYMENTS", "false")
        )
    )
    
    # TODO(TEAM-SEC): Disable legacy PII handling after migration
    enable_legacy_pii: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("ENABLE_LEGACY_PII", "true")
        )
    )
    
    # Enable experimental features
    enable_experimental_dedup: bool = field(
        default_factory=lambda: _parse_bool(
            os.getenv("ENABLE_EXPERIMENTAL_DEDUP", "false")
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
    
    TODO(TEAM-PLATFORM): Remove after migration complete
    
    Returns:
        bool: True if legacy ETL is enabled.
    """
    return get_feature_flags().enable_legacy_etl


def is_v1_schema_enabled() -> bool:
    """
    Check if v1 data schema is enabled.
    
    When enabled, ETL jobs will read from and write to v1 schema tables.
    This flag controls the data model version used for transformations.
    
    TODO(TEAM-API): Remove after schema migration complete
    
    Returns:
        bool: True if v1 schema is enabled.
    """
    return get_feature_flags().enable_v1_schema


def is_legacy_payments_enabled() -> bool:
    """
    Check if legacy payments integration is enabled.
    
    Controls whether the ETL uses the old payments client or the new
    payments service integration.
    
    TODO(TEAM-PAYMENTS): Remove after payments v2 rollout
    
    Returns:
        bool: True if legacy payments is enabled.
    """
    return get_feature_flags().enable_legacy_payments


def is_legacy_pii_enabled() -> bool:
    """
    Check if legacy PII handling is enabled.
    
    When enabled, PII is processed using the legacy approach (masking).
    When disabled, PII uses the new tokenization approach.
    
    TODO(TEAM-SEC): Remove after PII migration complete
    
    Returns:
        bool: True if legacy PII handling is enabled.
    """
    return get_feature_flags().enable_legacy_pii


def with_feature_flag(
    flag_checker: Callable[[], bool],
    enabled_func: Callable,
    disabled_func: Callable,
):
    """
    Execute different functions based on a feature flag.
    
    This is a utility for cleanly branching logic based on feature flags.
    
    Args:
        flag_checker: A function that returns True if the flag is enabled.
        enabled_func: Function to call when flag is enabled.
        disabled_func: Function to call when flag is disabled.
    
    Returns:
        A wrapper function that branches based on the flag.
    """
    def wrapper(*args, **kwargs):
        if flag_checker():
            return enabled_func(*args, **kwargs)
        return disabled_func(*args, **kwargs)
    return wrapper
