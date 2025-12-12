"""
ETL Jobs module for AcmeShop Analytics.

Contains ETL job implementations for each analytics domain.
"""
from acme_shop_analytics_etl.etl.user_analytics_job import (
    run_user_analytics_etl,
    extract_user_data,
    transform_user_metrics,
    load_user_analytics,
)
from acme_shop_analytics_etl.etl.order_analytics_job import (
    run_order_analytics_etl,
    extract_order_data,
    transform_order_metrics,
    load_order_analytics,
)
from acme_shop_analytics_etl.etl.payment_analytics_job import (
    run_payment_analytics_etl,
    extract_payment_data,
    transform_payment_metrics,
    load_payment_analytics,
)
from acme_shop_analytics_etl.etl.notification_analytics_job import (
    run_notification_analytics_etl,
    extract_notification_data,
    transform_notification_metrics,
    load_notification_analytics,
)

__all__ = [
    # User analytics
    "run_user_analytics_etl",
    "extract_user_data",
    "transform_user_metrics",
    "load_user_analytics",
    # Order analytics
    "run_order_analytics_etl",
    "extract_order_data",
    "transform_order_metrics",
    "load_order_analytics",
    # Payment analytics
    "run_payment_analytics_etl",
    "extract_payment_data",
    "transform_payment_metrics",
    "load_payment_analytics",
    # Notification analytics
    "run_notification_analytics_etl",
    "extract_notification_data",
    "transform_notification_metrics",
    "load_notification_analytics",
]
