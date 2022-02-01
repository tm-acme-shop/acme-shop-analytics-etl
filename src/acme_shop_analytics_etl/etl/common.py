"""
Common ETL Utilities

Shared utilities and helpers for all ETL jobs.
"""
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Generator, List, Optional, TypeVar

from acme_shop_analytics_etl.config.settings import get_settings
from acme_shop_analytics_etl.logging.structured_logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


@dataclass
class ETLResult:
    """Result of an ETL job execution."""
    
    job_name: str
    status: str = "success"
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    records_processed: int = 0
    records_skipped: int = 0
    records_failed: int = 0
    duration_seconds: float = 0.0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "job_name": self.job_name,
            "status": self.status,
            "records_extracted": self.records_extracted,
            "records_transformed": self.records_transformed,
            "records_loaded": self.records_loaded,
            "records_processed": self.records_processed,
            "records_skipped": self.records_skipped,
            "records_failed": self.records_failed,
            "duration_seconds": self.duration_seconds,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "error_count": len(self.errors),
        }


@dataclass
class TimeWindow:
    """Represents a time window for data extraction."""
    
    start: datetime
    end: datetime
    
    @property
    def duration(self) -> timedelta:
        """Get the duration of the window."""
        return self.end - self.start
    
    def contains(self, dt: datetime) -> bool:
        """Check if a datetime falls within this window."""
        return self.start <= dt < self.end


def get_previous_day_window(reference: Optional[datetime] = None) -> TimeWindow:
    """
    Get the time window for the previous day.
    
    Args:
        reference: Reference datetime (defaults to now).
    
    Returns:
        TimeWindow for the previous full day.
    """
    ref = reference or datetime.now()
    today = ref.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    
    return TimeWindow(start=yesterday, end=today)


def get_previous_hour_window(reference: Optional[datetime] = None) -> TimeWindow:
    """
    Get the time window for the previous hour.
    
    Args:
        reference: Reference datetime (defaults to now).
    
    Returns:
        TimeWindow for the previous full hour.
    """
    ref = reference or datetime.now()
    this_hour = ref.replace(minute=0, second=0, microsecond=0)
    previous_hour = this_hour - timedelta(hours=1)
    
    return TimeWindow(start=previous_hour, end=this_hour)


def batch_records(
    records: List[T],
    batch_size: Optional[int] = None,
) -> Generator[List[T], None, None]:
    """
    Split records into batches.
    
    Args:
        records: List of records to batch.
        batch_size: Size of each batch (defaults to ETL_BATCH_SIZE setting).
    
    Yields:
        Batches of records.
    """
    settings = get_settings()
    size = batch_size or settings.etl.batch_size
    
    for i in range(0, len(records), size):
        yield records[i:i + size]


def retry_with_backoff(
    func: Callable[..., T],
    max_retries: Optional[int] = None,
    base_delay: Optional[float] = None,
    max_delay: float = 300.0,
) -> Callable[..., T]:
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        func: Function to wrap.
        max_retries: Maximum number of retries.
        base_delay: Base delay between retries in seconds.
        max_delay: Maximum delay between retries.
    
    Returns:
        Wrapped function with retry logic.
    """
    settings = get_settings()
    retries = max_retries or settings.etl.max_retries
    delay = base_delay or settings.etl.retry_delay_seconds
    
    def wrapper(*args, **kwargs) -> T:
        last_exception = None
        
        for attempt in range(retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < retries:
                    wait_time = min(delay * (2 ** attempt), max_delay)
                    logger.warning(
                        "Retry attempt",
                        extra={
                            "attempt": attempt + 1,
                            "max_retries": retries,
                            "wait_seconds": wait_time,
                            "error": str(e),
                        },
                    )
                    time.sleep(wait_time)
        
        raise last_exception
    
    return wrapper


def measure_duration(func: Callable[..., T]) -> Callable[..., tuple]:
    """
    Decorator to measure function execution time.
    
    Args:
        func: Function to wrap.
    
    Returns:
        Wrapped function that returns (result, duration_seconds).
    """
    def wrapper(*args, **kwargs) -> tuple:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        duration = time.perf_counter() - start
        return result, duration
    
    return wrapper


def validate_record(record: Dict[str, Any], required_fields: List[str]) -> bool:
    """
    Validate that a record has all required fields.
    
    Args:
        record: Record to validate.
        required_fields: List of required field names.
    
    Returns:
        True if valid, False otherwise.
    """
    for field in required_fields:
        if field not in record or record[field] is None:
            return False
    return True


def safe_get(record: Dict[str, Any], key: str, default: Any = None) -> Any:
    """
    Safely get a value from a record with a default.
    
    Args:
        record: Record dictionary.
        key: Key to look up.
        default: Default value if key missing or None.
    
    Returns:
        Value or default.
    """
    value = record.get(key)
    return value if value is not None else default


def calculate_rate(count: int, duration_seconds: float) -> float:
    """
    Calculate rate (records per second).
    
    Args:
        count: Number of records.
        duration_seconds: Duration in seconds.
    
    Returns:
        Rate in records per second.
    """
    if duration_seconds <= 0:
        return 0.0
    return count / duration_seconds


def format_duration(seconds: float) -> str:
    """
    Format a duration in human-readable form.
    
    Args:
        seconds: Duration in seconds.
    
    Returns:
        Formatted string (e.g., "2m 30s" or "150ms").
    """
    if seconds < 1:
        return f"{int(seconds * 1000)}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
