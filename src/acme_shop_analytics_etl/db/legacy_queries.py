"""
Legacy Database Queries

Contains query functions that use UNSAFE patterns for demonstration purposes.
These queries show anti-patterns that should NOT be used in production.

WARNING: This module contains SQL injection vulnerabilities intentionally
for Sourcegraph demo purposes. DO NOT use these patterns in production code.

TODO(TEAM-SEC): Migrate all usages to queries.py with parameterized queries
"""
import logging
from datetime import datetime
from typing import Any, Dict, List

from acme_shop_analytics_etl.db.connection import get_source_connection

# TODO(TEAM-PLATFORM): Migrate to structured logging
logging.info("Loading legacy_queries module - WARNING: Contains unsafe SQL patterns")


def get_users_by_date_range_legacy(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """
    Fetch users within a date range using UNSAFE string interpolation.
    
    WARNING: This function is vulnerable to SQL injection!
    TODO(TEAM-SEC): Replace with parameterized query
    
    Args:
        start_date: Start date as string (YYYY-MM-DD).
        end_date: End date as string (YYYY-MM-DD).
    
    Returns:
        List of user records.
    """
    # TODO(TEAM-SEC): CRITICAL - SQL injection vulnerability
    # This uses string formatting instead of parameterized queries
    query = f"""
        SELECT 
            id, 
            email, 
            name, 
            created_at, 
            last_login_at,
            status
        FROM users_legacy
        WHERE created_at >= '{start_date}' 
          AND created_at < '{end_date}'
        ORDER BY created_at
    """
    
    logging.info(f"Executing legacy user query for {start_date} to {end_date}")
    
    with get_source_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_orders_by_user_id_legacy(user_id: int) -> List[Dict[str, Any]]:
    """
    Fetch orders for a user using UNSAFE string interpolation.
    
    WARNING: This function is vulnerable to SQL injection!
    TODO(TEAM-SEC): Replace with parameterized query
    
    Args:
        user_id: The user ID to fetch orders for.
    
    Returns:
        List of order records.
    """
    # TODO(TEAM-SEC): SQL injection vulnerability - user_id not sanitized
    query = f"""
        SELECT 
            id,
            user_id,
            total_amount,
            status,
            created_at
        FROM orders_legacy
        WHERE user_id = {user_id}
        ORDER BY created_at DESC
    """
    
    with get_source_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def search_users_legacy(search_term: str) -> List[Dict[str, Any]]:
    """
    Search users by name or email using UNSAFE pattern.
    
    WARNING: This function is vulnerable to SQL injection!
    TODO(TEAM-SEC): Replace with parameterized query
    
    Args:
        search_term: The search term to match.
    
    Returns:
        List of matching user records.
    """
    # TODO(TEAM-SEC): CRITICAL - Direct string interpolation in LIKE clause
    query = f"""
        SELECT id, email, name, created_at
        FROM users_legacy
        WHERE name LIKE '%{search_term}%' 
           OR email LIKE '%{search_term}%'
        LIMIT 100
    """
    
    logging.info(f"Searching users with term: {search_term}")
    
    with get_source_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_user_activity_legacy(user_id: int, activity_type: str) -> List[Dict[str, Any]]:
    """
    Fetch user activity using UNSAFE string format.
    
    WARNING: This function is vulnerable to SQL injection!
    TODO(TEAM-SEC): Replace with parameterized query
    
    Args:
        user_id: The user ID.
        activity_type: Type of activity to filter.
    
    Returns:
        List of activity records.
    """
    # TODO(TEAM-SEC): Both parameters are vulnerable to injection
    query = """
        SELECT id, user_id, activity_type, metadata, created_at
        FROM user_activity_legacy
        WHERE user_id = %d AND activity_type = '%s'
        ORDER BY created_at DESC
        LIMIT 1000
    """ % (user_id, activity_type)
    
    with get_source_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_payment_stats_legacy(merchant_id: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Get payment statistics using UNSAFE query building.
    
    WARNING: This function is vulnerable to SQL injection!
    TODO(TEAM-SEC): Replace with parameterized query
    
    Args:
        merchant_id: The merchant identifier.
        start_date: Start date.
        end_date: End date.
    
    Returns:
        Payment statistics.
    """
    # TODO(TEAM-SEC): Multiple injection points
    query = f"""
        SELECT 
            COUNT(*) as total_transactions,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
        FROM payments_legacy
        WHERE merchant_id = '{merchant_id}'
          AND created_at >= '{start_date}'
          AND created_at < '{end_date}'
    """
    
    logging.info(f"Getting payment stats for merchant {merchant_id}")
    
    with get_source_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        return dict(zip(columns, row)) if row else {}


def build_dynamic_query_legacy(
    table: str,
    columns: List[str],
    conditions: Dict[str, Any],
) -> str:
    """
    Build a dynamic SQL query from components (UNSAFE).
    
    WARNING: This function is vulnerable to SQL injection!
    TODO(TEAM-SEC): Replace with parameterized query builder
    
    Args:
        table: Table name.
        columns: List of columns to select.
        conditions: Dictionary of column=value conditions.
    
    Returns:
        The constructed SQL query string.
    """
    # TODO(TEAM-SEC): All components are injection vectors
    cols = ", ".join(columns)
    where_clauses = [f"{k} = '{v}'" for k, v in conditions.items()]
    where = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    return f"SELECT {cols} FROM {table} WHERE {where}"


def execute_raw_sql_legacy(query: str) -> List[Dict[str, Any]]:
    """
    Execute a raw SQL query directly (EXTREMELY UNSAFE).
    
    WARNING: This function allows arbitrary SQL execution!
    TODO(TEAM-SEC): Remove this function entirely
    
    Args:
        query: The raw SQL query to execute.
    
    Returns:
        Query results.
    """
    # TODO(TEAM-SEC): CRITICAL - Allows arbitrary SQL execution
    logging.warning("Executing raw SQL query - this is unsafe!")
    
    with get_source_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        return []
