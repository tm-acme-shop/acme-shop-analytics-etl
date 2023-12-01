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
            "user_token": "usr_a1b2c3d4e5f6g7h8",
            "email": "john.doe@example.com",
            "name": "John Doe",
            "phone": "+1-555-123-4567",
            "created_at": datetime(2024, 1, 15, 10, 30, 0),
            "status": "active",
            "subscription_type": "premium",
            "subscription_tier": "gold",
            "email_verified": True,
            "email_verified_at": datetime(2024, 1, 15, 11, 0, 0),
            "last_login_at": datetime(2024, 6, 1, 9, 0, 0),
            "last_activity_at": datetime(2024, 6, 1, 9, 30, 0),
            "country_code": "US",
            "signup_source": "web",
        },
        {
            "id": 2,
            "user_token": "usr_b2c3d4e5f6g7h8i9",
            "email": "jane.smith@example.com",
            "name": "Jane Smith",
            "phone": "+44-20-1234-5678",
            "created_at": datetime(2024, 2, 20, 14, 0, 0),
            "status": "active",
            "subscription_type": "basic",
            "subscription_tier": "silver",
            "email_verified": True,
            "email_verified_at": datetime(2024, 2, 20, 15, 0, 0),
            "last_login_at": datetime(2024, 5, 15, 12, 0, 0),
            "last_activity_at": datetime(2024, 5, 15, 13, 0, 0),
            "country_code": "GB",
            "signup_source": "mobile",
        },
        {
            "id": 3,
            "user_token": "usr_c3d4e5f6g7h8i9j0",
            "email": "bob.wilson@example.com",
            "name": "Bob Wilson",
            "phone": "+1-555-987-6543",
            "created_at": datetime(2024, 3, 10, 8, 0, 0),
            "status": "inactive",
            "subscription_type": "free",
            "subscription_tier": "bronze",
            "email_verified": False,
            "email_verified_at": None,
            "last_login_at": datetime(2024, 3, 15, 10, 0, 0),
            "last_activity_at": None,
            "country_code": "US",
            "signup_source": "referral",
        },
    ]


@pytest.fixture
def sample_order_records() -> List[Dict[str, Any]]:
    """Sample order records for testing."""
    return [
        {
            "id": "order-001",
            "order_date": datetime(2024, 6, 1),
            "status": "completed",
            "order_count": 150,
            "total_revenue": "45000.00",
            "avg_order_value": "300.00",
        },
        {
            "id": "order-002",
            "order_date": datetime(2024, 6, 2),
            "status": "completed",
            "order_count": 175,
            "total_revenue": "52500.00",
            "avg_order_value": "300.00",
        },
    ]


@pytest.fixture
def sample_payment_records() -> List[Dict[str, Any]]:
    """Sample payment records for testing."""
    return [
        {
            "id": "pay-001",
            "payment_date": datetime(2024, 6, 1),
            "payment_method": "credit_card",
            "card_number": "4111111111111111",
            "cardholder_name": "John Doe",
            "billing_address": "123 Main St, City, State 12345",
            "transaction_count": 500,
            "total_amount": "50000.00",
            "successful": 485,
            "failed": 15,
            "avg_processing_time": 250,
        },
        {
            "id": "pay-002",
            "payment_date": datetime(2024, 6, 1),
            "payment_method": "paypal",
            "transaction_count": 200,
            "total_amount": "18000.00",
            "successful": 195,
            "failed": 5,
            "avg_processing_time": 180,
        },
    ]


@pytest.fixture
def sample_notification_records() -> List[Dict[str, Any]]:
    """Sample notification records for testing."""
    return [
        {
            "id": "notif-001",
            "notification_date": datetime(2024, 6, 1),
            "channel": "email",
            "notification_type": "marketing",
            "total_sent": 10000,
            "delivered": 9500,
            "opened": 2500,
            "clicked": 500,
            "bounced": 300,
            "failed": 200,
        },
        {
            "id": "notif-002",
            "notification_date": datetime(2024, 6, 1),
            "channel": "sms",
            "notification_type": "transactional",
            "total_sent": 5000,
            "delivered": 4800,
            "opened": 4000,
            "clicked": 800,
            "bounced": 50,
            "failed": 150,
        },
        {
            "id": "notif-003",
            "notification_date": datetime(2024, 6, 1),
            "channel": "push",
            "notification_type": "alert",
            "total_sent": 8000,
            "delivered": 7200,
            "opened": 3600,
            "clicked": 720,
            "bounced": 100,
            "failed": 700,
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
