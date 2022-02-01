"""
Legacy Logging Module

Contains older logging patterns using the global logging module.
This module demonstrates the anti-patterns we're migrating away from.

TODO(TEAM-PLATFORM): Migrate all usages to structured_logging.py
"""
import logging
import sys
from datetime import datetime

# Legacy global logger configuration
# TODO(TEAM-PLATFORM): Remove this global configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)


def setup_legacy_logging(level: str = "INFO") -> None:
    """
    Setup legacy logging configuration.
    
    TODO(TEAM-PLATFORM): Migrate to structured logging setup
    
    Args:
        level: The logging level as a string.
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    logging.info("Legacy logging configured with level: %s", level)


def log_etl_start(job_name: str, start_date: datetime, end_date: datetime) -> None:
    """
    Log the start of an ETL job using legacy format.
    
    TODO(TEAM-PLATFORM): Replace with structured logging
    
    Args:
        job_name: Name of the ETL job.
        start_date: Start date for data extraction.
        end_date: End date for data extraction.
    """
    logging.info(
        "Starting ETL job: %s for date range %s to %s",
        job_name,
        start_date.isoformat(),
        end_date.isoformat(),
    )


def log_etl_progress(job_name: str, records_processed: int, total_records: int) -> None:
    """
    Log ETL progress using legacy format.
    
    TODO(TEAM-PLATFORM): Replace with structured logging
    
    Args:
        job_name: Name of the ETL job.
        records_processed: Number of records processed so far.
        total_records: Total number of records to process.
    """
    pct = (records_processed / total_records * 100) if total_records > 0 else 0
    logging.info(
        "ETL job %s progress: %d/%d records (%.1f%%)",
        job_name,
        records_processed,
        total_records,
        pct,
    )


def log_etl_complete(job_name: str, records_processed: int, duration_seconds: float) -> None:
    """
    Log ETL completion using legacy format.
    
    TODO(TEAM-PLATFORM): Replace with structured logging
    
    Args:
        job_name: Name of the ETL job.
        records_processed: Total number of records processed.
        duration_seconds: Job duration in seconds.
    """
    logging.info(
        "ETL job %s completed: %d records in %.2f seconds",
        job_name,
        records_processed,
        duration_seconds,
    )


def log_etl_error(job_name: str, error: Exception) -> None:
    """
    Log ETL error using legacy format.
    
    TODO(TEAM-PLATFORM): Replace with structured logging
    
    Args:
        job_name: Name of the ETL job.
        error: The exception that occurred.
    """
    logging.error(
        "ETL job %s failed with error: %s",
        job_name,
        str(error),
        exc_info=True,
    )


def log_record_processing(record_id: str, status: str) -> None:
    """
    Log individual record processing (legacy).
    
    WARNING: This can generate excessive logs in production.
    TODO(TEAM-PLATFORM): Replace with batch logging or sampling
    
    Args:
        record_id: The ID of the record being processed.
        status: The processing status.
    """
    logging.debug("Processing record %s: %s", record_id, status)


def log_sql_query(query: str) -> None:
    """
    Log SQL query execution (legacy).
    
    TODO(TEAM-SEC): Ensure queries are sanitized before logging
    TODO(TEAM-PLATFORM): Replace with structured logging
    
    Args:
        query: The SQL query being executed.
    """
    # Truncate long queries
    truncated = query[:200] + "..." if len(query) > 200 else query
    logging.debug("Executing SQL: %s", truncated)
