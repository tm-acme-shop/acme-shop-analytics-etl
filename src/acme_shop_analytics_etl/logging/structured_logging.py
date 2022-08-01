"""
Structured Logging Module

Provides structured logging with context support for better observability.
"""
import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional


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
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


class JsonFormatter(logging.Formatter):
    """Simple JSON formatter for structured logging."""
    
    def format(self, record):
        log_obj = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
        }
        if hasattr(record, "extra"):
            log_obj.update(record.extra)
        return json.dumps(log_obj)


class ContextAdapter(logging.LoggerAdapter):
    """
    Logger adapter that merges context with extra fields.
    """
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        extra = kwargs.get("extra", {})
        if self.extra:
            extra = {**self.extra, **extra}
        kwargs["extra"] = extra
        return msg, kwargs


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


def get_logger(name: str, **context: Any) -> ContextAdapter:
    """
    Get a structured logger with optional context.
    
    Args:
        name: The logger name (usually __name__).
        **context: Additional context to include in all log messages.
    
    Returns:
        A logger adapter with context support.
    """
    base_logger = logging.getLogger(name)
    return ContextAdapter(base_logger, context)
