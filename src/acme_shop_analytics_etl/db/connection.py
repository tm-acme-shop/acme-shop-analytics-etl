"""
Database Connection Management

Provides connection pooling and context managers for database access.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Generator, Optional

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from acme_shop_analytics_etl.config.settings import get_settings
from acme_shop_analytics_etl.logging.structured_logging import get_logger

logger = get_logger(__name__)

# Connection pools (lazy initialized)
_analytics_pool: Optional[pool.ThreadedConnectionPool] = None
_source_pool: Optional[pool.ThreadedConnectionPool] = None


@dataclass
class DatabaseConnection:
    """
    Database connection wrapper with context manager support.
    """
    
    connection: Any
    pool: pool.ThreadedConnectionPool
    
    def __enter__(self):
        return self.connection
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.connection.rollback()
            logger.warning(
                "Database transaction rolled back",
                extra={"error_type": exc_type.__name__},
            )
        else:
            self.connection.commit()
        self.pool.putconn(self.connection)


def _get_analytics_pool() -> pool.ThreadedConnectionPool:
    """Get or create the analytics database connection pool."""
    global _analytics_pool
    
    if _analytics_pool is None:
        settings = get_settings()
        logger.info(
            "Creating analytics database connection pool",
            extra={
                "pool_size": settings.database.pool_size,
                "max_overflow": settings.database.max_overflow,
            },
        )
        _analytics_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=settings.database.pool_size + settings.database.max_overflow,
            dsn=settings.database.url,
        )
    
    return _analytics_pool


def _get_source_pool() -> pool.ThreadedConnectionPool:
    """Get or create the source database connection pool."""
    global _source_pool
    
    if _source_pool is None:
        settings = get_settings()
        logger.info(
            "Creating source database connection pool",
            extra={
                "pool_size": settings.database.pool_size,
                "max_overflow": settings.database.max_overflow,
            },
        )
        _source_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=settings.database.pool_size + settings.database.max_overflow,
            dsn=settings.database.source_url,
        )
    
    return _source_pool


def get_connection() -> DatabaseConnection:
    """
    Get a connection to the analytics database.
    
    Returns:
        DatabaseConnection: A wrapped database connection.
    
    Example:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM metrics")
    """
    pool = _get_analytics_pool()
    conn = pool.getconn()
    return DatabaseConnection(connection=conn, pool=pool)


def get_source_connection() -> DatabaseConnection:
    """
    Get a connection to the source production database.
    
    Returns:
        DatabaseConnection: A wrapped database connection.
    """
    pool = _get_source_pool()
    conn = pool.getconn()
    return DatabaseConnection(connection=conn, pool=pool)


@contextmanager
def cursor(use_dict: bool = True) -> Generator[Any, None, None]:
    """
    Context manager for getting a database cursor.
    
    Args:
        use_dict: If True, use RealDictCursor for dict-like row access.
    
    Yields:
        A database cursor.
    
    Example:
        with cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
    """
    with get_connection() as conn:
        cursor_factory = RealDictCursor if use_dict else None
        cur = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cur
        finally:
            cur.close()


@contextmanager
def source_cursor(use_dict: bool = True) -> Generator[Any, None, None]:
    """
    Context manager for getting a source database cursor.
    
    Args:
        use_dict: If True, use RealDictCursor for dict-like row access.
    
    Yields:
        A database cursor.
    """
    with get_source_connection() as conn:
        cursor_factory = RealDictCursor if use_dict else None
        cur = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cur
        finally:
            cur.close()


def close_pools() -> None:
    """
    Close all database connection pools.
    
    Should be called during application shutdown.
    """
    global _analytics_pool, _source_pool
    
    if _analytics_pool is not None:
        _analytics_pool.closeall()
        _analytics_pool = None
        logger.info("Analytics connection pool closed")
    
    if _source_pool is not None:
        _source_pool.closeall()
        _source_pool = None
        logger.info("Source connection pool closed")
