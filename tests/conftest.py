"""
Pytest Configuration and Fixtures

Provides common test fixtures and configuration for ETL tests.
"""
import os
import pytest
from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch


@pytest.fixture(autouse=True)
def reset_feature_flags():
    """
    Reset feature flags before each test.
    
    Clears the lru_cache on get_feature_flags to ensure tests
    get fresh flag values based on environment variables.
    """
    from acme_shop_analytics_etl.config.feature_flags import get_feature_flags
    get_feature_flags.cache_clear()
    yield
    get_feature_flags.cache_clear()


@pytest.fixture
def enable_legacy_flags(monkeypatch):
    """Enable all legacy feature flags for testing legacy code paths."""
    monkeypatch.setenv("ENABLE_LEGACY_ETL", "true")
    monkeypatch.setenv("ENABLE_V1_SCHEMA", "true")
    monkeypatch.setenv("ENABLE_LEGACY_PAYMENTS", "true")
    monkeypatch.setenv("ENABLE_LEGACY_PII", "true")
    
    from acme_shop_analytics_etl.config.feature_flags import get_feature_flags
    get_feature_flags.cache_clear()
    
    yield
    
    get_feature_flags.cache_clear()


@pytest.fixture
def disable_legacy_flags(monkeypatch):
    """Disable all legacy feature flags for testing v2 code paths."""
    monkeypatch.setenv("ENABLE_LEGACY_ETL", "false")
    monkeypatch.setenv("ENABLE_V1_SCHEMA", "false")
    monkeypatch.setenv("ENABLE_LEGACY_PAYMENTS", "false")
    monkeypatch.setenv("ENABLE_LEGACY_PII", "false")
    
    from acme_shop_analytics_etl.config.feature_flags import get_feature_flags
    get_feature_flags.cache_clear()
    
    yield
    
    get_feature_flags.cache_clear()


@pytest.fixture
def sample_user_records() -> List[Dict[str, Any]]:
    """Sample user records for testing."""
    return [
        {
            "id": 1,
            "email": "john.doe@example.com",
            "name": "John Doe",
            "phone": "+1-555-123-4567",
            "created_at": datetime(2024, 1, 15, 10, 30, 0),
            "status": "active",
            "subscription_type": "premium",
            "email_verified": True,
            "last_login_at": datetime(2024, 6, 1, 9, 0, 0),
            "country_code": "US",
            "signup_source": "web",
        },
        {
            "id": 2,
            "email": "jane.smith@example.com",
            "name": "Jane Smith",
            "phone": "+44-20-1234-5678",
            "created_at": datetime(2024, 2, 20, 14, 0, 0),
            "status": "active",
            "subscription_type": "basic",
            "email_verified": True,
            "last_login_at": datetime(2024, 5, 15, 12, 0, 0),
            "country_code": "GB",
            "signup_source": "mobile",
        },
    ]


@pytest.fixture
def mock_db_connection():
    """Mock database connection for testing."""
    with patch("acme_shop_analytics_etl.db.connection.get_connection") as mock:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock.return_value.__enter__ = lambda self: mock_conn
        mock.return_value.__exit__ = lambda self, *args: None
        yield mock_conn


@pytest.fixture
def date_range():
    """Standard date range for testing."""
    end_date = datetime(2024, 6, 15)
    start_date = datetime(2024, 6, 1)
    return start_date, end_date


@pytest.fixture
def execution_date():
    """Standard execution date for testing."""
    return datetime(2024, 6, 15)
