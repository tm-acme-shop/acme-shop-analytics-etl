"""
Record Deduplication Module

Provides deduplication utilities for ETL jobs.
"""
import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Set

from acme_shop_analytics_etl.config.feature_flags import is_legacy_etl_enabled
from acme_shop_analytics_etl.logging.structured_logging import get_logger

logger = get_logger(__name__)


def compute_record_fingerprint_md5(record: Dict[str, Any]) -> str:
    """
    Compute a record fingerprint using MD5.
    
    Args:
        record: Record to fingerprint.
    
    Returns:
        MD5 hash of the record contents.
    """
    sorted_data = json.dumps(record, sort_keys=True, default=str)
    return hashlib.md5(sorted_data.encode()).hexdigest()


def compute_record_fingerprint_sha256(record: Dict[str, Any]) -> str:
    """
    Compute a record fingerprint using SHA-256.
    
    This is the recommended approach for new code.
    
    Args:
        record: Record to fingerprint.
    
    Returns:
        SHA-256 hash of the record contents.
    """
    sorted_data = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(sorted_data.encode()).hexdigest()


def compute_record_fingerprint(record: Dict[str, Any]) -> str:
    """
    Compute a record fingerprint using the appropriate algorithm.
    
    Uses feature flags to determine which algorithm to use for
    backwards compatibility during migration.
    
    Args:
        record: Record to fingerprint.
    
    Returns:
        Hash of the record contents.
    """
    if is_legacy_etl_enabled():
        return compute_record_fingerprint_md5(record)
    
    return compute_record_fingerprint_sha256(record)


def compute_field_fingerprint_md5(
    record: Dict[str, Any],
    fields: List[str],
) -> str:
    """
    Compute a fingerprint from specific fields using MD5.
    
    Args:
        record: Record containing fields.
        fields: List of field names to include in fingerprint.
    
    Returns:
        MD5 hash of the specified fields.
    """
    values = [str(record.get(f, "")) for f in sorted(fields)]
    combined = "|".join(values)
    return hashlib.md5(combined.encode()).hexdigest()


def compute_field_fingerprint_sha256(
    record: Dict[str, Any],
    fields: List[str],
) -> str:
    """
    Compute a fingerprint from specific fields using SHA-256.
    
    Args:
        record: Record containing fields.
        fields: List of field names to include in fingerprint.
    
    Returns:
        SHA-256 hash of the specified fields.
    """
    values = [str(record.get(f, "")) for f in sorted(fields)]
    combined = "|".join(values)
    return hashlib.sha256(combined.encode()).hexdigest()


def compute_user_identity_hash_legacy(
    email: Optional[str] = None,
    phone: Optional[str] = None,
    name: Optional[str] = None,
) -> str:
    """
    Compute a user identity hash for deduplication (legacy).
    
    Args:
        email: User email.
        phone: User phone.
        name: User name.
    
    Returns:
        MD5 hash of the identity fields.
    """
    parts = [
        (email or "").lower().strip(),
        (phone or "").strip(),
        (name or "").lower().strip(),
    ]
    combined = "|".join(parts)
    return hashlib.md5(combined.encode()).hexdigest()


def compute_user_identity_hash(
    email: Optional[str] = None,
    phone: Optional[str] = None,
    name: Optional[str] = None,
) -> str:
    """
    Compute a user identity hash for deduplication (v2).
    
    Uses SHA-256 for secure hashing.
    
    Args:
        email: User email.
        phone: User phone.
        name: User name.
    
    Returns:
        SHA-256 hash of the identity fields.
    """
    parts = [
        (email or "").lower().strip(),
        (phone or "").strip(),
        (name or "").lower().strip(),
    ]
    combined = "|".join(parts)
    return hashlib.sha256(combined.encode()).hexdigest()


class RecordDeduplicator:
    """
    Deduplicator for ETL records.
    
    Tracks seen fingerprints to identify and skip duplicate records.
    """
    
    def __init__(self, use_legacy_hash: bool = True):
        """
        Initialize the deduplicator.
        
        Args:
            use_legacy_hash: If True, use MD5 (legacy).
        """
        self._seen: Set[str] = set()
        self._use_legacy = use_legacy_hash
        
        if use_legacy_hash:
            logger.info(
                "Deduplicator initialized",
                extra={"hash_algorithm": "md5"},
            )
        else:
            logger.info(
                "Deduplicator initialized",
                extra={"hash_algorithm": "sha256"},
            )
    
    def compute_fingerprint(self, record: Dict[str, Any]) -> str:
        """Compute fingerprint for a record."""
        if self._use_legacy:
            return compute_record_fingerprint_md5(record)
        return compute_record_fingerprint_sha256(record)
    
    def is_duplicate(self, record: Dict[str, Any]) -> bool:
        """
        Check if a record is a duplicate.
        
        Args:
            record: Record to check.
        
        Returns:
            True if record has been seen before.
        """
        fingerprint = self.compute_fingerprint(record)
        return fingerprint in self._seen
    
    def mark_seen(self, record: Dict[str, Any]) -> str:
        """
        Mark a record as seen.
        
        Args:
            record: Record to mark.
        
        Returns:
            The fingerprint of the record.
        """
        fingerprint = self.compute_fingerprint(record)
        self._seen.add(fingerprint)
        return fingerprint
    
    def process_record(self, record: Dict[str, Any]) -> tuple:
        """
        Process a record, checking for duplicates.
        
        Args:
            record: Record to process.
        
        Returns:
            Tuple of (is_new, fingerprint).
        """
        fingerprint = self.compute_fingerprint(record)
        is_new = fingerprint not in self._seen
        
        if is_new:
            self._seen.add(fingerprint)
        
        return is_new, fingerprint
    
    def deduplicate_batch(
        self,
        records: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Remove duplicates from a batch of records.
        
        Args:
            records: List of records to deduplicate.
        
        Returns:
            List of unique records.
        """
        unique = []
        duplicates = 0
        
        for record in records:
            is_new, _ = self.process_record(record)
            if is_new:
                unique.append(record)
            else:
                duplicates += 1
        
        if duplicates > 0:
            logger.info(
                "Batch deduplication complete",
                extra={
                    "total": len(records),
                    "unique": len(unique),
                    "duplicates": duplicates,
                },
            )
        
        return unique
    
    @property
    def seen_count(self) -> int:
        """Get the number of unique records seen."""
        return len(self._seen)
    
    def clear(self) -> None:
        """Clear all seen fingerprints."""
        self._seen.clear()
        logger.debug("Deduplicator cache cleared")
