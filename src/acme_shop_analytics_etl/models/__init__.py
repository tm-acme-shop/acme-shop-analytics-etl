# DATA-175: Add PII masking to ETL pipeline (2024-01)
"""
Data Models for AcmeShop Analytics ETL.

Contains both v1 (legacy) and v2 (modern) data models.
"""
from acme_shop_analytics_etl.config.feature_flags import is_v1_schema_enabled

from acme_shop_analytics_etl.models.v1 import (
    UserV1,
    OrderV1,
    PaymentV1,
    NotificationV1,
)
from acme_shop_analytics_etl.models.v2 import (
    User,
    Order,
    Payment,
    Notification,
)


def get_user_model():
    """
    Get the appropriate user model based on feature flags.
    
    TODO(TEAM-API): Remove v1 model support after migration
    
    Returns:
        UserV1 if v1 schema is enabled, otherwise User.
    """
    if is_v1_schema_enabled():
        return UserV1
    return User


def get_order_model():
    """
    Get the appropriate order model based on feature flags.
    
    TODO(TEAM-API): Remove v1 model support after migration
    
    Returns:
        OrderV1 if v1 schema is enabled, otherwise Order.
    """
    if is_v1_schema_enabled():
        return OrderV1
    return Order


def get_payment_model():
    """
    Get the appropriate payment model based on feature flags.
    
    TODO(TEAM-PAYMENTS): Remove v1 model support after migration
    
    Returns:
        PaymentV1 if v1 schema is enabled, otherwise Payment.
    """
    if is_v1_schema_enabled():
        return PaymentV1
    return Payment


def get_notification_model():
    """
    Get the appropriate notification model based on feature flags.
    
    Returns:
        NotificationV1 if v1 schema is enabled, otherwise Notification.
    """
    if is_v1_schema_enabled():
        return NotificationV1
    return Notification


__all__ = [
    # V1 models (deprecated)
    "UserV1",
    "OrderV1",
    "PaymentV1",
    "NotificationV1",
    # V2 models (current)
    "User",
    "Order",
    "Payment",
    "Notification",
    # Model selectors
    "get_user_model",
    "get_order_model",
    "get_payment_model",
    "get_notification_model",
]
