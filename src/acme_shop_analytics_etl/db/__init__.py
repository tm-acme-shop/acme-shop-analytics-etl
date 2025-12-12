"""
Database module for AcmeShop Analytics ETL.

Provides database connection management and query utilities.
"""
from acme_shop_analytics_etl.db.connection import (
    get_connection,
    get_source_connection,
    DatabaseConnection,
)
from acme_shop_analytics_etl.db.queries import (
    execute_parameterized_query,
    fetch_user_analytics,
    fetch_order_analytics,
)

__all__ = [
    "get_connection",
    "get_source_connection",
    "DatabaseConnection",
    "execute_parameterized_query",
    "fetch_user_analytics",
    "fetch_order_analytics",
]
