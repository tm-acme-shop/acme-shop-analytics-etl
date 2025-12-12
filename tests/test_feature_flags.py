"""
Feature Flags Tests

Tests for the feature flag system controlling legacy vs v2 implementations.
"""
import os
import pytest

from acme_shop_analytics_etl.config.feature_flags import (
    FeatureFlags,
    get_feature_flags,
    is_legacy_etl_enabled,
    is_v1_schema_enabled,
    is_legacy_payments_enabled,
    is_legacy_pii_enabled,
    with_feature_flag,
    _parse_bool,
)


class TestParseBool:
    """Tests for boolean parsing from environment strings."""
    
    def test_parse_true_values(self):
        """Test that various true-ish values parse as True."""
        assert _parse_bool("true") is True
        assert _parse_bool("True") is True
        assert _parse_bool("TRUE") is True
        assert _parse_bool("1") is True
        assert _parse_bool("yes") is True
        assert _parse_bool("YES") is True
        assert _parse_bool("on") is True
        assert _parse_bool("ON") is True
    
    def test_parse_false_values(self):
        """Test that various false-ish values parse as False."""
        assert _parse_bool("false") is False
        assert _parse_bool("False") is False
        assert _parse_bool("FALSE") is False
        assert _parse_bool("0") is False
        assert _parse_bool("no") is False
        assert _parse_bool("NO") is False
        assert _parse_bool("off") is False
        assert _parse_bool("") is False
        assert _parse_bool("random") is False


class TestFeatureFlags:
    """Tests for FeatureFlags dataclass."""
    
    def test_default_values(self, monkeypatch):
        """Test default flag values when no env vars are set."""
        monkeypatch.delenv("ENABLE_LEGACY_ETL", raising=False)
        monkeypatch.delenv("ENABLE_V1_SCHEMA", raising=False)
        monkeypatch.delenv("ENABLE_LEGACY_PAYMENTS", raising=False)
        monkeypatch.delenv("ENABLE_LEGACY_PII", raising=False)
        monkeypatch.delenv("ENABLE_EXPERIMENTAL_DEDUP", raising=False)
        
        flags = FeatureFlags()
        
        assert flags.enable_legacy_etl is True
        assert flags.enable_v1_schema is True
        assert flags.enable_legacy_payments is False
        assert flags.enable_legacy_pii is True
        assert flags.enable_experimental_dedup is False
    
    def test_env_var_override(self, monkeypatch):
        """Test that environment variables override defaults."""
        monkeypatch.setenv("ENABLE_LEGACY_ETL", "false")
        monkeypatch.setenv("ENABLE_V1_SCHEMA", "false")
        monkeypatch.setenv("ENABLE_LEGACY_PAYMENTS", "true")
        monkeypatch.setenv("ENABLE_LEGACY_PII", "false")
        monkeypatch.setenv("ENABLE_EXPERIMENTAL_DEDUP", "true")
        
        flags = FeatureFlags()
        
        assert flags.enable_legacy_etl is False
        assert flags.enable_v1_schema is False
        assert flags.enable_legacy_payments is True
        assert flags.enable_legacy_pii is False
        assert flags.enable_experimental_dedup is True


class TestGetFeatureFlags:
    """Tests for the singleton feature flags getter."""
    
    def test_returns_feature_flags_instance(self):
        """Test that get_feature_flags returns a FeatureFlags instance."""
        flags = get_feature_flags()
        assert isinstance(flags, FeatureFlags)
    
    def test_caching(self):
        """Test that get_feature_flags returns cached instance."""
        flags1 = get_feature_flags()
        flags2 = get_feature_flags()
        assert flags1 is flags2
    
    def test_cache_clear_returns_fresh_instance(self, monkeypatch):
        """Test that cache_clear allows fresh flag loading."""
        monkeypatch.setenv("ENABLE_LEGACY_ETL", "false")
        get_feature_flags.cache_clear()
        flags = get_feature_flags()
        
        assert flags.enable_legacy_etl is False


class TestIsLegacyEtlEnabled:
    """Tests for is_legacy_etl_enabled helper."""
    
    def test_returns_true_when_enabled(self, monkeypatch):
        """Test returns True when ENABLE_LEGACY_ETL is true."""
        monkeypatch.setenv("ENABLE_LEGACY_ETL", "true")
        get_feature_flags.cache_clear()
        
        assert is_legacy_etl_enabled() is True
    
    def test_returns_false_when_disabled(self, monkeypatch):
        """Test returns False when ENABLE_LEGACY_ETL is false."""
        monkeypatch.setenv("ENABLE_LEGACY_ETL", "false")
        get_feature_flags.cache_clear()
        
        assert is_legacy_etl_enabled() is False


class TestIsV1SchemaEnabled:
    """Tests for is_v1_schema_enabled helper."""
    
    def test_returns_true_when_enabled(self, monkeypatch):
        """Test returns True when ENABLE_V1_SCHEMA is true."""
        monkeypatch.setenv("ENABLE_V1_SCHEMA", "true")
        get_feature_flags.cache_clear()
        
        assert is_v1_schema_enabled() is True
    
    def test_returns_false_when_disabled(self, monkeypatch):
        """Test returns False when ENABLE_V1_SCHEMA is false."""
        monkeypatch.setenv("ENABLE_V1_SCHEMA", "false")
        get_feature_flags.cache_clear()
        
        assert is_v1_schema_enabled() is False


class TestIsLegacyPaymentsEnabled:
    """Tests for is_legacy_payments_enabled helper."""
    
    def test_returns_false_by_default(self, monkeypatch):
        """Test returns False by default (payments v2 is preferred)."""
        monkeypatch.delenv("ENABLE_LEGACY_PAYMENTS", raising=False)
        get_feature_flags.cache_clear()
        
        assert is_legacy_payments_enabled() is False
    
    def test_returns_true_when_enabled(self, monkeypatch):
        """Test returns True when explicitly enabled."""
        monkeypatch.setenv("ENABLE_LEGACY_PAYMENTS", "true")
        get_feature_flags.cache_clear()
        
        assert is_legacy_payments_enabled() is True


class TestIsLegacyPiiEnabled:
    """Tests for is_legacy_pii_enabled helper."""
    
    def test_returns_true_when_enabled(self, monkeypatch):
        """Test returns True when ENABLE_LEGACY_PII is true."""
        monkeypatch.setenv("ENABLE_LEGACY_PII", "true")
        get_feature_flags.cache_clear()
        
        assert is_legacy_pii_enabled() is True
    
    def test_returns_false_when_disabled(self, monkeypatch):
        """Test returns False when ENABLE_LEGACY_PII is false."""
        monkeypatch.setenv("ENABLE_LEGACY_PII", "false")
        get_feature_flags.cache_clear()
        
        assert is_legacy_pii_enabled() is False


class TestWithFeatureFlag:
    """Tests for the with_feature_flag utility."""
    
    def test_calls_enabled_func_when_flag_true(self):
        """Test calls enabled function when flag returns True."""
        enabled_func = lambda x: f"enabled: {x}"
        disabled_func = lambda x: f"disabled: {x}"
        
        wrapper = with_feature_flag(
            flag_checker=lambda: True,
            enabled_func=enabled_func,
            disabled_func=disabled_func,
        )
        
        result = wrapper("test")
        assert result == "enabled: test"
    
    def test_calls_disabled_func_when_flag_false(self):
        """Test calls disabled function when flag returns False."""
        enabled_func = lambda x: f"enabled: {x}"
        disabled_func = lambda x: f"disabled: {x}"
        
        wrapper = with_feature_flag(
            flag_checker=lambda: False,
            enabled_func=enabled_func,
            disabled_func=disabled_func,
        )
        
        result = wrapper("test")
        assert result == "disabled: test"
    
    def test_passes_multiple_args_and_kwargs(self):
        """Test that args and kwargs are passed through correctly."""
        def enabled_func(a, b, c=None):
            return f"enabled: {a}, {b}, {c}"
        
        def disabled_func(a, b, c=None):
            return f"disabled: {a}, {b}, {c}"
        
        wrapper = with_feature_flag(
            flag_checker=lambda: True,
            enabled_func=enabled_func,
            disabled_func=disabled_func,
        )
        
        result = wrapper("x", "y", c="z")
        assert result == "enabled: x, y, z"
    
    def test_with_real_feature_flag(self, monkeypatch):
        """Test with actual feature flag function."""
        monkeypatch.setenv("ENABLE_LEGACY_ETL", "true")
        get_feature_flags.cache_clear()
        
        legacy_transform = lambda data: {"version": "v1", "data": data}
        v2_transform = lambda data: {"version": "v2", "data": data}
        
        wrapper = with_feature_flag(
            flag_checker=is_legacy_etl_enabled,
            enabled_func=legacy_transform,
            disabled_func=v2_transform,
        )
        
        result = wrapper({"id": 1})
        assert result["version"] == "v1"
        
        monkeypatch.setenv("ENABLE_LEGACY_ETL", "false")
        get_feature_flags.cache_clear()
        
        wrapper = with_feature_flag(
            flag_checker=is_legacy_etl_enabled,
            enabled_func=legacy_transform,
            disabled_func=v2_transform,
        )
        
        result = wrapper({"id": 1})
        assert result["version"] == "v2"


class TestFeatureFlagIntegration:
    """Integration tests for feature flags with fixtures."""
    
    def test_enable_legacy_flags_fixture(self, enable_legacy_flags):
        """Test that enable_legacy_flags fixture works."""
        assert is_legacy_etl_enabled() is True
        assert is_v1_schema_enabled() is True
        assert is_legacy_payments_enabled() is True
        assert is_legacy_pii_enabled() is True
    
    def test_disable_legacy_flags_fixture(self, disable_legacy_flags):
        """Test that disable_legacy_flags fixture works."""
        assert is_legacy_etl_enabled() is False
        assert is_v1_schema_enabled() is False
        assert is_legacy_payments_enabled() is False
        assert is_legacy_pii_enabled() is False
