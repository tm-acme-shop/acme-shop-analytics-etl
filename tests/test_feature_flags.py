"""
Feature Flags Tests

Tests for the feature flag configuration.
"""
import pytest

from acme_shop_analytics_etl.config.feature_flags import (
    get_feature_flags,
    is_legacy_etl_enabled,
    is_v1_schema_enabled,
    is_legacy_payments_enabled,
    is_legacy_pii_enabled,
    FeatureFlags,
)


class TestFeatureFlags:
    """Tests for FeatureFlags configuration."""
    
    def test_defaults(self, monkeypatch):
        """Test default flag values."""
        monkeypatch.delenv("ENABLE_LEGACY_ETL", raising=False)
        monkeypatch.delenv("ENABLE_V1_SCHEMA", raising=False)
        monkeypatch.delenv("ENABLE_LEGACY_PAYMENTS", raising=False)
        monkeypatch.delenv("ENABLE_LEGACY_PII", raising=False)
        get_feature_flags.cache_clear()
        
        flags = get_feature_flags()
        
        assert flags.enable_legacy_etl is True
        assert flags.enable_v1_schema is True
        assert flags.enable_legacy_payments is False
        assert flags.enable_legacy_pii is True
    
    def test_legacy_etl_enabled(self, monkeypatch):
        """Test ENABLE_LEGACY_ETL=true."""
        monkeypatch.setenv("ENABLE_LEGACY_ETL", "true")
        get_feature_flags.cache_clear()
        
        assert is_legacy_etl_enabled() is True
    
    def test_legacy_etl_disabled(self, monkeypatch):
        """Test ENABLE_LEGACY_ETL=false."""
        monkeypatch.setenv("ENABLE_LEGACY_ETL", "false")
        get_feature_flags.cache_clear()
        
        assert is_legacy_etl_enabled() is False
    
    def test_v1_schema_enabled(self, monkeypatch):
        """Test ENABLE_V1_SCHEMA=true."""
        monkeypatch.setenv("ENABLE_V1_SCHEMA", "true")
        get_feature_flags.cache_clear()
        
        assert is_v1_schema_enabled() is True
    
    def test_v1_schema_disabled(self, monkeypatch):
        """Test ENABLE_V1_SCHEMA=false."""
        monkeypatch.setenv("ENABLE_V1_SCHEMA", "false")
        get_feature_flags.cache_clear()
        
        assert is_v1_schema_enabled() is False
    
    def test_parse_bool_variations(self, monkeypatch):
        """Test that various boolean string values are parsed correctly."""
        for true_val in ["true", "1", "yes", "on", "TRUE", "True"]:
            monkeypatch.setenv("ENABLE_LEGACY_ETL", true_val)
            get_feature_flags.cache_clear()
            assert is_legacy_etl_enabled() is True
        
        for false_val in ["false", "0", "no", "off", "FALSE", "False"]:
            monkeypatch.setenv("ENABLE_LEGACY_ETL", false_val)
            get_feature_flags.cache_clear()
            assert is_legacy_etl_enabled() is False
