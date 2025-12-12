"""
Database Queries (V2)

Contains properly parameterized query functions for safe database access.
This is the recommended approach for all database operations.
"""
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from acme_shop_analytics_etl.db.connection import cursor, source_cursor
from acme_shop_analytics_etl.logging.structured_logging import get_logger

logger = get_logger(__name__)

# SQL file directory
SQL_DIR = Path(__file__).parent.parent.parent.parent / "sql"


def load_sql_file(filename: str) -> str:
    """
    Load a SQL query from the sql/ directory.
    
    Args:
        filename: Name of the SQL file.
    
    Returns:
        The SQL query string.
    """
    sql_path = SQL_DIR / filename
    logger.debug("Loading SQL file", extra={"path": str(sql_path)})
    return sql_path.read_text()


def execute_parameterized_query(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    use_source: bool = True,
) -> List[Dict[str, Any]]:
    """
    Execute a parameterized query safely.
    
    Args:
        query: SQL query with named parameters (%(name)s style).
        params: Dictionary of parameter values.
        use_source: If True, use source database; otherwise use analytics.
    
    Returns:
        List of result rows as dictionaries.
    
    Example:
        results = execute_parameterized_query(
            "SELECT * FROM users WHERE created_at >= %(start)s",
            {"start": datetime(2024, 1, 1)}
        )
    """
    params = params or {}
    
    logger.info(
        "Executing parameterized query",
        extra={
            "param_count": len(params),
            "use_source": use_source,
        },
    )
    
    cursor_ctx = source_cursor if use_source else cursor
    
    with cursor_ctx() as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


def fetch_user_analytics(
    start_date: datetime,
    end_date: datetime,
    use_v2_schema: bool = True,
) -> List[Dict[str, Any]]:
    """
    Fetch user analytics data using parameterized queries.
    
    Args:
        start_date: Start of the date range.
        end_date: End of the date range.
        use_v2_schema: Whether to use v2 schema queries.
    
    Returns:
        List of user analytics records.
    """
    if use_v2_schema:
        sql_file = "user_analytics_v2.sql"
    else:
        sql_file = "user_analytics_legacy.sql"
    
    # For now, use inline query with proper parameterization
    query = """
        SELECT 
            DATE(created_at) as registration_date,
            COUNT(*) as total_registrations,
            COUNT(CASE WHEN email_verified_at IS NOT NULL THEN 1 END) as verified_users,
            COUNT(CASE WHEN subscription_tier = 'premium' THEN 1 END) as premium_signups
        FROM users_v2
        WHERE created_at >= %(start_date)s AND created_at < %(end_date)s
        GROUP BY DATE(created_at)
        ORDER BY registration_date
    """
    
    logger.info(
        "Fetching user analytics",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "schema": "v2" if use_v2_schema else "v1",
        },
    )
    
    return execute_parameterized_query(
        query,
        {"start_date": start_date, "end_date": end_date},
    )


def fetch_order_analytics(
    start_date: datetime,
    end_date: datetime,
    statuses: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch order analytics data using parameterized queries.
    
    Args:
        start_date: Start of the date range.
        end_date: End of the date range.
        statuses: Optional list of order statuses to filter.
    
    Returns:
        List of order analytics records.
    """
    statuses = statuses or ["completed", "pending", "cancelled"]
    
    query = """
        SELECT 
            DATE(created_at) as order_date,
            status,
            COUNT(*) as order_count,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value
        FROM orders_v2
        WHERE created_at >= %(start_date)s 
          AND created_at < %(end_date)s
          AND status = ANY(%(statuses)s)
        GROUP BY DATE(created_at), status
        ORDER BY order_date, status
    """
    
    logger.info(
        "Fetching order analytics",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "statuses": statuses,
        },
    )
    
    return execute_parameterized_query(
        query,
        {
            "start_date": start_date,
            "end_date": end_date,
            "statuses": statuses,
        },
    )


def fetch_payment_analytics(
    start_date: datetime,
    end_date: datetime,
) -> List[Dict[str, Any]]:
    """
    Fetch payment analytics data using parameterized queries.
    
    Args:
        start_date: Start of the date range.
        end_date: End of the date range.
    
    Returns:
        List of payment analytics records.
    """
    query = """
        SELECT 
            DATE(created_at) as payment_date,
            payment_method,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
            AVG(CASE WHEN status = 'success' THEN processing_time_ms END) as avg_processing_time
        FROM payments_v2
        WHERE created_at >= %(start_date)s AND created_at < %(end_date)s
        GROUP BY DATE(created_at), payment_method
        ORDER BY payment_date, payment_method
    """
    
    logger.info(
        "Fetching payment analytics",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )
    
    return execute_parameterized_query(
        query,
        {"start_date": start_date, "end_date": end_date},
    )


def fetch_notification_analytics(
    start_date: datetime,
    end_date: datetime,
    channels: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch notification analytics data using parameterized queries.
    
    Args:
        start_date: Start of the date range.
        end_date: End of the date range.
        channels: Optional list of notification channels to filter.
    
    Returns:
        List of notification analytics records.
    """
    channels = channels or ["email", "sms", "push"]
    
    query = """
        SELECT 
            DATE(sent_at) as notification_date,
            channel,
            notification_type,
            COUNT(*) as total_sent,
            SUM(CASE WHEN delivered_at IS NOT NULL THEN 1 ELSE 0 END) as delivered,
            SUM(CASE WHEN opened_at IS NOT NULL THEN 1 ELSE 0 END) as opened,
            SUM(CASE WHEN clicked_at IS NOT NULL THEN 1 ELSE 0 END) as clicked
        FROM notifications_v2
        WHERE sent_at >= %(start_date)s 
          AND sent_at < %(end_date)s
          AND channel = ANY(%(channels)s)
        GROUP BY DATE(sent_at), channel, notification_type
        ORDER BY notification_date, channel
    """
    
    logger.info(
        "Fetching notification analytics",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "channels": channels,
        },
    )
    
    return execute_parameterized_query(
        query,
        {
            "start_date": start_date,
            "end_date": end_date,
            "channels": channels,
        },
    )


def insert_analytics_batch(
    table: str,
    records: List[Dict[str, Any]],
) -> int:
    """
    Insert a batch of analytics records.
    
    Args:
        table: Target table name.
        records: List of records to insert.
    
    Returns:
        Number of records inserted.
    """
    if not records:
        return 0
    
    # Get columns from first record
    columns = list(records[0].keys())
    placeholders = ", ".join(f"%({col})s" for col in columns)
    column_list = ", ".join(columns)
    
    query = f"""
        INSERT INTO {table} ({column_list})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """
    
    logger.info(
        "Inserting analytics batch",
        extra={
            "table": table,
            "record_count": len(records),
            "columns": columns,
        },
    )
    
    inserted = 0
    with cursor(use_dict=False) as cur:
        for record in records:
            cur.execute(query, record)
            inserted += cur.rowcount
    
    logger.info(
        "Batch insert complete",
        extra={"table": table, "inserted": inserted},
    )
    
    return inserted
