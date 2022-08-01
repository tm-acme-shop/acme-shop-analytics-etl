"""
Utility Functions

Common utility functions for the AcmeShop Analytics ETL.
"""
import hashlib
import logging
import os
import re
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, TypeVar

from acme_shop_analytics_etl.logging.structured_logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


# DATA-100: Initial ETL with MD5 deduplication (2022-02)
def md5_hash(value: str) -> str:
    """
    Compute MD5 hash of a string.
    
    DEPRECATED: MD5 is cryptographically broken and should not be used.
    TODO(TEAM-SEC): Remove this function - use sha256_hash instead.
    
    Args:
        value: String to hash.
    
    Returns:
        MD5 hash hex string.
    """
    # TODO(TEAM-SEC): CRITICAL - MD5 is cryptographically broken
    logging.warning("Using deprecated md5_hash() - migrate to sha256_hash()")
    return hashlib.md5(value.encode()).hexdigest()


def sha1_hash(value: str) -> str:
    """
    Compute SHA-1 hash of a string.
    
    DEPRECATED: SHA-1 is cryptographically weak.
    TODO(TEAM-SEC): Remove this function - use sha256_hash instead.
    
    Args:
        value: String to hash.
    
    Returns:
        SHA-1 hash hex string.
    """
    # TODO(TEAM-SEC): SHA-1 is weak - migrate to SHA-256
    logging.warning("Using deprecated sha1_hash() - migrate to sha256_hash()")
    return hashlib.sha1(value.encode()).hexdigest()


# SEC-135: SHA-256 introduced for new deduplication jobs (2022-08)
def sha256_hash(value: str) -> str:
    """
    Compute SHA-256 hash of a string.
    
    This is the recommended hash function for new code.
    
    Args:
        value: String to hash.
    
    Returns:
        SHA-256 hash hex string.
    """
    return hashlib.sha256(value.encode()).hexdigest()


def get_env_bool(key: str, default: bool = False) -> bool:
    """
    Get a boolean value from environment variable.
    
    Args:
        key: Environment variable name.
        default: Default value if not set.
    
    Returns:
        Boolean value.
    """
    value = os.getenv(key, "")
    if not value:
        return default
    return value.lower() in ("true", "1", "yes", "on")


def get_env_int(key: str, default: int = 0) -> int:
    """
    Get an integer value from environment variable.
    
    Args:
        key: Environment variable name.
        default: Default value if not set or invalid.
    
    Returns:
        Integer value.
    """
    value = os.getenv(key, "")
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid integer for {key}: {value}, using default {default}")
        return default


def parse_date_range(
    start_str: Optional[str],
    end_str: Optional[str],
    default_days_back: int = 1,
) -> tuple:
    """
    Parse date range from string inputs.
    
    Args:
        start_str: Start date string (YYYY-MM-DD) or None.
        end_str: End date string (YYYY-MM-DD) or None.
        default_days_back: Days back for default start date.
    
    Returns:
        Tuple of (start_datetime, end_datetime).
    """
    if end_str:
        end_date = datetime.strptime(end_str, "%Y-%m-%d")
    else:
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    if start_str:
        start_date = datetime.strptime(start_str, "%Y-%m-%d")
    else:
        start_date = end_date - timedelta(days=default_days_back)
    
    return start_date, end_date


def validate_email(email: str) -> bool:
    """
    Validate email format.
    
    Args:
        email: Email string to validate.
    
    Returns:
        True if valid email format.
    """
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))


def normalize_phone(phone: str) -> str:
    """
    Normalize phone number to digits only.
    
    Args:
        phone: Phone number string.
    
    Returns:
        Digits-only string.
    """
    return re.sub(r"\D", "", phone)


def chunk_list(lst: List[T], chunk_size: int) -> List[List[T]]:
    """
    Split a list into chunks of specified size.
    
    Args:
        lst: List to chunk.
        chunk_size: Size of each chunk.
    
    Returns:
        List of chunks.
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def flatten_dict(
    d: Dict[str, Any],
    parent_key: str = "",
    separator: str = "_",
) -> Dict[str, Any]:
    """
    Flatten a nested dictionary.
    
    Args:
        d: Dictionary to flatten.
        parent_key: Prefix for keys.
        separator: Separator between nested keys.
    
    Returns:
        Flattened dictionary.
    """
    items: List[tuple] = []
    for key, value in d.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default on division by zero.
    
    Args:
        numerator: Dividend.
        denominator: Divisor.
        default: Value to return on division by zero.
    
    Returns:
        Result of division or default.
    """
    if denominator == 0:
        return default
    return numerator / denominator


def retry(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable:
    """
    Decorator for retrying a function on failure.
    
    Args:
        max_retries: Maximum number of retries.
        delay: Initial delay between retries in seconds.
        backoff: Multiplier for delay after each retry.
        exceptions: Tuple of exception types to catch.
    
    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            import time
            
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(
                            f"Retry {attempt + 1}/{max_retries} for {func.__name__}: {e}",
                            extra={
                                "function": func.__name__,
                                "attempt": attempt + 1,
                                "max_retries": max_retries,
                            },
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
            
            raise last_exception
        
        return wrapper
    return decorator


def timer(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator to time function execution.
    
    Args:
        func: Function to time.
    
    Returns:
        Decorated function that logs execution time.
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        import time
        
        start = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            elapsed = time.perf_counter() - start
            logger.info(
                f"{func.__name__} completed",
                extra={
                    "function": func.__name__,
                    "duration_seconds": round(elapsed, 3),
                },
            )
    
    return wrapper


def extract_request_id_from_headers(
    headers: Dict[str, str],
) -> Optional[str]:
    """
    Extract request ID from HTTP headers.
    
    Supports both legacy and new header formats.
    
    Args:
        headers: HTTP headers dictionary.
    
    Returns:
        Request ID or None.
    """
    # New format (preferred)
    request_id = headers.get("X-Acme-Request-ID")
    if request_id:
        return request_id
    
    # TODO(TEAM-API): Remove legacy header support after migration
    legacy_id = headers.get("X-Legacy-Request-Id")
    if legacy_id:
        logging.warning("Using deprecated X-Legacy-Request-Id header")
        return legacy_id
    
    return None


def extract_user_id_from_headers(
    headers: Dict[str, str],
) -> Optional[str]:
    """
    Extract user ID from HTTP headers.
    
    Supports both legacy and new header formats.
    
    Args:
        headers: HTTP headers dictionary.
    
    Returns:
        User ID or None.
    """
    # New format (preferred)
    user_id = headers.get("X-User-Id")
    if user_id:
        return user_id
    
    # TODO(TEAM-API): Remove legacy header support after migration
    legacy_id = headers.get("X-Legacy-User-Id")
    if legacy_id:
        logging.warning("Using deprecated X-Legacy-User-Id header")
        return legacy_id
    
    return None


def format_currency(amount: float, currency: str = "USD") -> str:
    """
    Format a currency amount.
    
    Args:
        amount: Amount to format.
        currency: Currency code.
    
    Returns:
        Formatted currency string.
    """
    if currency == "USD":
        return f"${amount:,.2f}"
    elif currency == "EUR":
        return f"€{amount:,.2f}"
    elif currency == "GBP":
        return f"£{amount:,.2f}"
    else:
        return f"{amount:,.2f} {currency}"


def truncate_string(s: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate a string to a maximum length.
    
    Args:
        s: String to truncate.
        max_length: Maximum length.
        suffix: Suffix to append when truncated.
    
    Returns:
        Truncated string.
    """
    if len(s) <= max_length:
        return s
    return s[:max_length - len(suffix)] + suffix


def sanitize_for_logging(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize a dictionary for safe logging (remove sensitive fields).
    
    Args:
        data: Dictionary to sanitize.
    
    Returns:
        Sanitized dictionary.
    """
    sensitive_keys = {
        "password", "secret", "token", "api_key", "apikey",
        "credit_card", "card_number", "cvv", "ssn",
        "email", "phone", "address",
    }
    
    result = {}
    for key, value in data.items():
        key_lower = key.lower()
        if any(sk in key_lower for sk in sensitive_keys):
            result[key] = "[REDACTED]"
        elif isinstance(value, dict):
            result[key] = sanitize_for_logging(value)
        else:
            result[key] = value
    
    return result
