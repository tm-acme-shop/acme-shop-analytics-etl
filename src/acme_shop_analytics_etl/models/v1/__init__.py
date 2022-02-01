"""
V1 Data Models (Legacy)

DEPRECATED: These models contain anti-patterns and should not be used for new code.
TODO(TEAM-API): Complete migration to v2 models and remove this package.
"""
from acme_shop_analytics_etl.models.v1.user import UserV1, UserActivityV1
from acme_shop_analytics_etl.models.v1.order import OrderV1, OrderItemV1
from acme_shop_analytics_etl.models.v1.payment import PaymentV1, RefundV1
from acme_shop_analytics_etl.models.v1.notification import NotificationV1

__all__ = [
    "UserV1",
    "UserActivityV1",
    "OrderV1",
    "OrderItemV1",
    "PaymentV1",
    "RefundV1",
    "NotificationV1",
]
