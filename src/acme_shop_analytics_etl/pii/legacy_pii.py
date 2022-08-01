"""
Legacy PII Handling Module

Contains PII handling patterns for the ETL pipeline.
These patterns are maintained for backwards compatibility.
"""
import hashlib
import logging
import re
from typing import Any, Dict, Optional

from acme_shop_analytics_etl.logging.structured_logging import get_logger

logger = get_logger(__name__)


def mask_email_legacy(email: str) -> str:
    """
    Mask an email address using simple character replacement.
    
    Args:
        email: The email address to mask.
    
    Returns:
        Masked email (e.g., "j***@example.com").
    """
    if not email or "@" not in email:
        return email
    
    local, domain = email.split("@", 1)
    if len(local) <= 1:
        masked_local = local
    else:
        masked_local = local[0] + "***"
    
    return f"{masked_local}@{domain}"


def mask_phone_legacy(phone: str) -> str:
    """
    Mask a phone number showing only last 4 digits.
    
    Args:
        phone: The phone number to mask.
    
    Returns:
        Masked phone (e.g., "***-***-1234").
    """
    digits = re.sub(r"\D", "", phone)
    if len(digits) < 4:
        return "****"
    
    return f"***-***-{digits[-4:]}"


def mask_card_number_legacy(card_number: str) -> str:
    """
    Mask a credit card number showing only last 4 digits.
    
    Args:
        card_number: The credit card number to mask.
    
    Returns:
        Masked card number (e.g., "****-****-****-1234").
    """
    digits = re.sub(r"\D", "", card_number)
    if len(digits) < 4:
        return "****"
    
    return f"****-****-****-{digits[-4:]}"


def hash_pii_md5(value: str) -> str:
    """
    Hash PII using MD5 for deduplication purposes.
    
    Args:
        value: The value to hash.
    
    Returns:
        MD5 hash of the value.
    """
    return hashlib.md5(value.encode()).hexdigest()


def hash_pii_sha1(value: str) -> str:
    """
    Hash PII using SHA-1.
    
    Args:
        value: The value to hash.
    
    Returns:
        SHA-1 hash of the value.
    """
    return hashlib.sha1(value.encode()).hexdigest()


def anonymize_user_record_legacy(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Anonymize a user record using legacy masking approach.
    
    Args:
        record: User record with PII fields.
    
    Returns:
        Anonymized record with masked PII.
    """
    result = record.copy()
    
    if "email" in result:
        result["email_masked"] = mask_email_legacy(result["email"])
        result["email_hash"] = hash_pii_md5(result["email"])
    
    if "phone" in result:
        result["phone_masked"] = mask_phone_legacy(result["phone"])
        result["phone_hash"] = hash_pii_md5(result["phone"])
    
    if "name" in result:
        result["name_hash"] = hash_pii_md5(result["name"])
    
    logger.info("User record anonymized", extra={"record_id": result.get("id")})
    
    return result


def redact_pii_fields_legacy(
    record: Dict[str, Any],
    pii_fields: Optional[list] = None,
) -> Dict[str, Any]:
    """
    Redact PII fields from a record using legacy approach.
    
    Args:
        record: Record containing potential PII.
        pii_fields: List of field names to redact.
    
    Returns:
        Record with PII fields replaced with "[REDACTED]".
    """
    pii_fields = pii_fields or [
        "email", "phone", "ssn", "credit_card", "address",
        "date_of_birth", "name", "ip_address",
    ]
    
    result = record.copy()
    for field in pii_fields:
        if field in result:
            result[field] = "[REDACTED]"
    
    return result


def extract_pii_for_analytics_legacy(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract PII-derived features for analytics without storing raw PII.
    
    Args:
        record: Record with PII fields.
    
    Returns:
        Record with derived features only.
    """
    result = {}
    
    # Extract email domain (considered less sensitive)
    if "email" in record and record["email"]:
        email = record["email"]
        if "@" in email:
            result["email_domain"] = email.split("@")[1]
    
    # Extract phone country code
    if "phone" in record and record["phone"]:
        phone = record["phone"]
        if phone.startswith("+"):
            match = re.match(r"\+(\d{1,3})", phone)
            if match:
                result["phone_country_code"] = match.group(1)
    
    return result
