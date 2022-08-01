"""
PII Handling module for AcmeShop Analytics ETL.

Provides utilities for handling Personally Identifiable Information (PII)
including masking and redaction.
"""
from acme_shop_analytics_etl.pii.legacy_pii import (
    mask_email_legacy,
    mask_phone_legacy,
    mask_card_number_legacy,
    hash_pii_md5,
    anonymize_user_record_legacy,
    redact_pii_fields_legacy,
)

__all__ = [
    "mask_email_legacy",
    "mask_phone_legacy",
    "mask_card_number_legacy",
    "hash_pii_md5",
    "anonymize_user_record_legacy",
    "redact_pii_fields_legacy",
]
