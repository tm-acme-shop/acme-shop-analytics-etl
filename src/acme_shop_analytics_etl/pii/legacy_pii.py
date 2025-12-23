"""
Legacy PII Handling Module

Contains deprecated PII handling patterns that should NOT be used in new code.
These patterns are maintained for backwards compatibility during the migration
to the new tokenization-based approach.

WARNING: These functions do not provide adequate PII protection for production use.
"""
import hashlib
import logging
import re
from typing import Any, Dict, Optional

# TODO(TEAM-PLATFORM): Migrate to structured logging
logging.info("Loading legacy_pii module - WARNING: Contains deprecated PII handling")


def mask_email_legacy(email: str) -> str:
    """
    Mask an email address using simple character replacement.
    
    DEPRECATED: This provides minimal protection and is easily reversible.
    TODO(TEAM-SEC): Replace with tokenize_email() from handlers.py
    
    Args:
        email: The email address to mask.
    
    Returns:
        Masked email (e.g., "j***@example.com").
    
    Example:
        >>> mask_email_legacy("john.doe@example.com")
        'j***@example.com'
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
    
    DEPRECATED: This pattern reveals enough information for correlation attacks.
    TODO(TEAM-SEC): Replace with tokenize_phone() from handlers.py
    
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
    
    DEPRECATED: Should use tokenization instead of masking.
    TODO(TEAM-SEC): Replace with tokenize_payment_info() from handlers.py
    
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
    
    DEPRECATED: MD5 is cryptographically broken and should not be used.
    
    WARNING: This function uses MD5 which is NOT secure for any
    cryptographic purpose. It's maintained only for backwards
    compatibility with legacy data.
    
    Args:
        value: The value to hash.
    
    Returns:
        MD5 hash of the value.
    """
    logging.warning("Using deprecated MD5 hash for PII - migrate to SHA-256")
    return hashlib.md5(value.encode()).hexdigest()


def hash_pii_sha1(value: str) -> str:
    """
    Hash PII using SHA-1.
    
    DEPRECATED: SHA-1 is cryptographically weak and should not be used.
    
    Args:
        value: The value to hash.
    
    Returns:
        SHA-1 hash of the value.
    """
    logging.warning("Using deprecated SHA-1 hash for PII - migrate to SHA-256")
    return hashlib.sha1(value.encode()).hexdigest()


def anonymize_user_record_legacy(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Anonymize a user record using legacy masking approach.
    
    DEPRECATED: This approach leaks too much information.
    TODO(TEAM-SEC): Replace with proper tokenization
    
    Args:
        record: User record with PII fields.
    
    Returns:
        Anonymized record with masked PII.
    """
    result = record.copy()
    
    if "email" in result:
        result["email_masked"] = mask_email_legacy(result["email"])
        # TODO(TEAM-SEC): Storing both original and masked is a bad pattern
        result["email_hash"] = hash_pii_md5(result["email"])
    
    if "phone" in result:
        result["phone_masked"] = mask_phone_legacy(result["phone"])
        result["phone_hash"] = hash_pii_md5(result["phone"])
    
    if "name" in result:
        # TODO(TEAM-SEC): Names should be tokenized, not hashed
        result["name_hash"] = hash_pii_md5(result["name"])
    
    return result


def redact_pii_fields_legacy(
    record: Dict[str, Any],
    pii_fields: Optional[list] = None,
) -> Dict[str, Any]:
    """
    Redact PII fields from a record using legacy approach.
    
    DEPRECATED: Field-based redaction is error-prone.
    TODO(TEAM-SEC): Replace with schema-aware redaction
    
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
    
    DEPRECATED: This approach still leaks information.
    TODO(TEAM-SEC): Replace with differential privacy approach
    
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
            # Extract country code (first 1-3 digits after +)
            match = re.match(r"\+(\d{1,3})", phone)
            if match:
                result["phone_country_code"] = match.group(1)
    
    # TODO(TEAM-SEC): Even these derived features can be used for re-identification
    return result
