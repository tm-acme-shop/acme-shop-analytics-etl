"""
Structured Logging Module

Provides structured logging with context support for better observability.
This is the recommended logging approach for all new code.
"""
import json
import logging
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional
from threading import local

# Thread-local storage for logging context
_context = local()


class StructuredFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    
    Outputs log records as JSON for easy parsing by log aggregation systems.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add extra fields from the record
        if hasattr(record, "extra") and record.extra:
            log_data.update(record.extra)
        
        # Add any extra attributes passed via extra={}
        for key, value in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "pathname", "process", "processName", "relativeCreated",
                "stack_info", "exc_info", "exc_text", "thread", "threadName",
                "extra", "message",
            ):
                log_data[key] = value
        
        # Add thread-local context
        if hasattr(_context, "data"):
            log_data.update(_context.data)
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


class ContextAdapter(logging.LoggerAdapter):
    """
    Logger adapter that merges context with extra fields.
    """
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        extra = kwargs.get("extra", {})
        if self.extra:
            extra = {**self.extra, **extra}
        if hasattr(_context, "data"):
            extra = {**_context.data, **extra}
        kwargs["extra"] = extra
        return msg, kwargs


@dataclass
class LogContext:
    """
    Context manager for adding contextual data to all log messages.
    
    Example:
        with LogContext(request_id="abc-123", user_id="user-456"):
            logger.info("Processing request")  # Includes request_id and user_id
    """
    
    data: Dict[str, Any] = field(default_factory=dict)
    
    def __enter__(self) -> "LogContext":
        if not hasattr(_context, "data"):
            _context.data = {}
        self._previous = _context.data.copy()
        _context.data.update(self.data)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        _context.data = self._previous
    
    def add(self, key: str, value: Any) -> None:
        """Add a key-value pair to the context."""
        _context.data[key] = value


def configure_logging(
    level: str = "INFO",
    use_json: bool = True,
    service_name: str = "acme-analytics-etl",
) -> None:
    """
    Configure structured logging for the application.
    
    Args:
        level: The logging level.
        use_json: Whether to use JSON formatting.
        service_name: The name of the service for log context.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper()))
    
    if use_json:
        handler.setFormatter(StructuredFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
        )
    
    root_logger.addHandler(handler)
    
    # Set global context
    if not hasattr(_context, "data"):
        _context.data = {}
    _context.data["service"] = service_name


def get_logger(name: str, **context: Any) -> ContextAdapter:
    """
    Get a structured logger with optional context.
    
    Args:
        name: The logger name (usually __name__).
        **context: Additional context to include in all log messages.
    
    Returns:
        A logger adapter with context support.
    
    Example:
        logger = get_logger(__name__, job="user_analytics")
        logger.info("Starting extraction", extra={"batch_size": 1000})
    """
    base_logger = logging.getLogger(name)
    return ContextAdapter(base_logger, context)


@contextmanager
def log_context(**kwargs: Any):
    """
    Context manager for temporarily adding log context.
    
    Args:
        **kwargs: Key-value pairs to add to the logging context.
    
    Example:
        with log_context(request_id="abc-123"):
            logger.info("Processing")  # Includes request_id
    """
    ctx = LogContext(data=kwargs)
    with ctx:
        yield ctx


def log_etl_start(
    logger: ContextAdapter,
    job_name: str,
    start_date: datetime,
    end_date: datetime,
    **extra: Any,
) -> None:
    """
    Log the start of an ETL job with structured data.
    
    Args:
        logger: The logger instance.
        job_name: Name of the ETL job.
        start_date: Start date for data extraction.
        end_date: End date for data extraction.
        **extra: Additional context fields.
    """
    logger.info(
        "ETL job started",
        extra={
            "job_name": job_name,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "event": "etl_start",
            **extra,
        },
    )


def log_etl_complete(
    logger: ContextAdapter,
    job_name: str,
    records_processed: int,
    duration_seconds: float,
    **extra: Any,
) -> None:
    """
    Log ETL completion with structured data.
    
    Args:
        logger: The logger instance.
        job_name: Name of the ETL job.
        records_processed: Total number of records processed.
        duration_seconds: Job duration in seconds.
        **extra: Additional context fields.
    """
    logger.info(
        "ETL job completed",
        extra={
            "job_name": job_name,
            "records_processed": records_processed,
            "duration_seconds": duration_seconds,
            "records_per_second": records_processed / duration_seconds if duration_seconds > 0 else 0,
            "event": "etl_complete",
            **extra,
        },
    )


def log_etl_error(
    logger: ContextAdapter,
    job_name: str,
    error: Exception,
    **extra: Any,
) -> None:
    """
    Log ETL error with structured data.
    
    Args:
        logger: The logger instance.
        job_name: Name of the ETL job.
        error: The exception that occurred.
        **extra: Additional context fields.
    """
    logger.error(
        "ETL job failed",
        extra={
            "job_name": job_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "event": "etl_error",
            **extra,
        },
        exc_info=True,
    )
