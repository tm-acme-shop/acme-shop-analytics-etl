"""
PII Handlers (V2)

Provides secure PII handling using tokenization and proper cryptographic methods.
This is the recommended approach for all PII processing.
"""
import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from acme_shop_analytics_etl.config.settings import get_settings
from acme_shop_analytics_etl.logging.structured_logging import get_logger

logger = get_logger(__name__)


@dataclass
class PIITokenizer:
    """
    Tokenizer for converting PII to secure tokens.
    
    Uses HMAC-SHA256 with a secret salt for consistent tokenization
    that cannot be reversed without the salt.
    """
    
    salt: bytes = field(default_factory=lambda: os.urandom(32))
    
    def __post_init__(self):
        # Try to load salt from settings
        settings = get_settings()
        if settings.pii.tokenization_salt:
            self.salt = settings.pii.tokenization_salt.encode()
    
    def tokenize(self, value: str, prefix: str = "tok") -> str:
        """
        Generate a token for a PII value.
        
        Args:
            value: The PII value to tokenize.
            prefix: Prefix for the token (e.g., "usr", "eml").
        
        Returns:
            A token in the format "{prefix}_{hash[:16]}".
        """
        if not value:
            return ""
        
        # Use HMAC-SHA256 for secure, consistent hashing
        token_hash = hmac.new(
            self.salt,
            value.encode(),
            hashlib.sha256,
        ).hexdigest()
        
        return f"{prefix}_{token_hash[:16]}"
    
    def tokenize_batch(
        self,
        values: List[str],
        prefix: str = "tok",
    ) -> List[str]:
        """
        Tokenize a batch of PII values.
        
        Args:
            values: List of PII values.
            prefix: Prefix for tokens.
        
        Returns:
            List of tokens.
        """
        return [self.tokenize(v, prefix) for v in values]


# Global tokenizer instance
_tokenizer: Optional[PIITokenizer] = None


def _get_tokenizer() -> PIITokenizer:
    """Get the global tokenizer instance."""
    global _tokenizer
    if _tokenizer is None:
        _tokenizer = PIITokenizer()
    return _tokenizer


def tokenize_email(email: str) -> str:
    """
    Tokenize an email address.
    
    Args:
        email: The email address to tokenize.
    
    Returns:
        An email token (e.g., "eml_a1b2c3d4e5f6g7h8").
    """
    if not email:
        return ""
    
    logger.debug("Tokenizing email", extra={"has_email": True})
    return _get_tokenizer().tokenize(email.lower().strip(), prefix="eml")


def tokenize_phone(phone: str) -> str:
    """
    Tokenize a phone number.
    
    Args:
        phone: The phone number to tokenize.
    
    Returns:
        A phone token (e.g., "phn_a1b2c3d4e5f6g7h8").
    """
    if not phone:
        return ""
    
    # Normalize phone number (digits only)
    import re
    normalized = re.sub(r"\D", "", phone)
    
    logger.debug("Tokenizing phone", extra={"has_phone": True})
    return _get_tokenizer().tokenize(normalized, prefix="phn")


def tokenize_name(name: str) -> str:
    """
    Tokenize a person's name.
    
    Args:
        name: The name to tokenize.
    
    Returns:
        A name token (e.g., "nam_a1b2c3d4e5f6g7h8").
    """
    if not name:
        return ""
    
    return _get_tokenizer().tokenize(name.lower().strip(), prefix="nam")


def tokenize_payment_info(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tokenize payment information in a record.
    
    Replaces sensitive payment fields with secure tokens.
    
    Args:
        record: Payment record with potential PII.
    
    Returns:
        Record with tokenized payment information.
    """
    result = record.copy()
    tokenizer = _get_tokenizer()
    
    # Tokenize card number
    if "card_number" in result and result["card_number"]:
        card = result["card_number"]
        result["card_token"] = tokenizer.tokenize(card, prefix="crd")
        # Store last 4 digits for display (industry standard)
        digits = "".join(c for c in card if c.isdigit())
        result["card_last_four"] = digits[-4:] if len(digits) >= 4 else "****"
        del result["card_number"]
    
    # Tokenize billing address
    if "billing_address" in result and result["billing_address"]:
        result["billing_token"] = tokenizer.tokenize(
            result["billing_address"],
            prefix="adr",
        )
        del result["billing_address"]
    
    # Tokenize cardholder name
    if "cardholder_name" in result and result["cardholder_name"]:
        result["cardholder_token"] = tokenizer.tokenize(
            result["cardholder_name"],
            prefix="nam",
        )
        del result["cardholder_name"]
    
    logger.info(
        "Payment info tokenized",
        extra={"record_id": result.get("id")},
    )
    
    return result


def redact_pii(
    record: Dict[str, Any],
    pii_fields: Optional[Set[str]] = None,
    replacement: str = "[REDACTED]",
) -> Dict[str, Any]:
    """
    Redact PII fields from a record.
    
    Unlike tokenization, redaction completely removes the PII
    and replaces it with a placeholder.
    
    Args:
        record: Record containing potential PII.
        pii_fields: Set of field names to redact.
        replacement: String to replace PII with.
    
    Returns:
        Record with PII fields redacted.
    """
    default_pii_fields = {
        "email", "phone", "ssn", "social_security_number",
        "credit_card", "card_number", "cvv", "cvc",
        "address", "street_address", "billing_address",
        "date_of_birth", "dob", "birth_date",
        "name", "first_name", "last_name", "full_name",
        "ip_address", "ip",
        "password", "password_hash",
    }
    
    fields_to_redact = pii_fields or default_pii_fields
    
    result = record.copy()
    redacted_count = 0
    
    for field in fields_to_redact:
        if field in result and result[field]:
            result[field] = replacement
            redacted_count += 1
    
    if redacted_count > 0:
        logger.debug(
            "PII fields redacted",
            extra={"redacted_count": redacted_count},
        )
    
    return result


def hash_for_analytics(value: str) -> str:
    """
    Generate a secure hash for analytics purposes.
    
    Uses SHA-256 with salt for secure, one-way hashing suitable
    for analytics aggregation.
    
    Args:
        value: The value to hash.
    
    Returns:
        SHA-256 hash of the salted value.
    """
    settings = get_settings()
    salt = (settings.pii.tokenization_salt or "default-salt").encode()
    
    return hashlib.sha256(salt + value.encode()).hexdigest()


def generate_user_token(user_id: int) -> str:
    """
    Generate a unique token for a user ID.
    
    This creates a consistent, reversible token that can be used
    to reference users without exposing internal IDs.
    
    Args:
        user_id: The internal user ID.
    
    Returns:
        A user token (e.g., "usr_a1b2c3d4e5f6g7h8").
    """
    tokenizer = _get_tokenizer()
    return tokenizer.tokenize(str(user_id), prefix="usr")


def extract_safe_analytics_fields(
    record: Dict[str, Any],
    allowed_fields: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """
    Extract only safe (non-PII) fields for analytics.
    
    This is a whitelist approach that only keeps explicitly
    allowed fields, which is safer than blacklisting PII.
    
    Args:
        record: The full record.
        allowed_fields: Set of field names that are safe to keep.
    
    Returns:
        Record containing only safe fields.
    """
    default_safe_fields = {
        "id", "user_token", "created_at", "updated_at",
        "status", "type", "category", "amount", "currency",
        "count", "total", "average", "percentage",
        "country_code", "region", "timezone",
    }
    
    safe_fields = allowed_fields or default_safe_fields
    
    return {k: v for k, v in record.items() if k in safe_fields}
