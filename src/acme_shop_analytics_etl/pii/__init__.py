"""
PII Handling module for AcmeShop Analytics ETL.

Provides utilities for handling Personally Identifiable Information (PII)
including masking and redaction.
"""
from acme_shop_analytics_etl.pii.legacy_pii import (
    mask_email,
    mask_phone,
    mask_card_number,
    hash_pii,
    anonymize_user_record,
    redact_pii_fields,
)

__all__ = [
    "mask_email",
    "mask_phone",
    "mask_card_number",
    "hash_pii",
    "anonymize_user_record",
    "redact_pii_fields",
]
