"""
PII Handling Module

Contains PII handling patterns for the ETL pipeline.
"""
import hashlib
import logging
import re
from typing import Any, Dict, Optional


def mask_email(email: str) -> str:
    """
    Mask an email address.
    
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


def mask_phone(phone: str) -> str:
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


def mask_card_number(card_number: str) -> str:
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


def hash_pii(value: str) -> str:
    """
    Hash PII using MD5 for deduplication purposes.
    
    Args:
        value: The value to hash.
    
    Returns:
        MD5 hash of the value.
    """
    return hashlib.md5(value.encode()).hexdigest()


def anonymize_user_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Anonymize a user record using masking approach.
    
    Args:
        record: User record with PII fields.
    
    Returns:
        Anonymized record with masked PII.
    """
    result = record.copy()
    
    if "email" in result:
        result["email_masked"] = mask_email(result["email"])
        result["email_hash"] = hash_pii(result["email"])
    
    if "phone" in result:
        result["phone_masked"] = mask_phone(result["phone"])
        result["phone_hash"] = hash_pii(result["phone"])
    
    if "name" in result:
        result["name_hash"] = hash_pii(result["name"])
    
    return result


def redact_pii_fields(
    record: Dict[str, Any],
    pii_fields: Optional[list] = None,
) -> Dict[str, Any]:
    """
    Redact PII fields from a record.
    
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
