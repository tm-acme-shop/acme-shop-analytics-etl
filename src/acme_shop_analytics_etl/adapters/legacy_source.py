"""
Legacy data source adapter with MD5 compatibility.

TODO(TEAM-SEC): REMOVE after legacy data source is decommissioned
This adapter uses MD5 for record fingerprinting to maintain compatibility
with the legacy analytics warehouse that hasn't been migrated yet.

Ticket: DATA-555
Owner: data-team
Deadline: 2024-12-01
"""

import hashlib
import json
import logging
from typing import Any, Dict

# TODO(TEAM-SEC): Replace with SHA-256 after warehouse migration
logger = logging.getLogger(__name__)


class LegacyDataSourceAdapter:
    """
    Adapter for legacy data source that requires MD5 fingerprints.
    
    DEPRECATED: This uses insecure MD5 hashing for backwards compatibility.
    Remove after legacy warehouse migration completes (Q4 2024).
    """
    
    def __init__(self, source_id: str):
        self.source_id = source_id
        # WARNING: Using MD5 for legacy compatibility - temporary only
        logger.warning(
            "Initializing legacy adapter with MD5 fingerprinting - "
            "TODO(TEAM-SEC): Migrate to SHA-256"
        )
    
    def compute_legacy_fingerprint(self, record: Dict[str, Any]) -> str:
        """
        Compute MD5 fingerprint for legacy warehouse compatibility.
        
        TODO(TEAM-SEC): Remove MD5 usage after migration
        WARNING: MD5 is cryptographically broken
        """
        # Sort keys for consistent hashing
        sorted_data = json.dumps(record, sort_keys=True, default=str)
        # TODO(TEAM-SEC): Replace hashlib.md5 with hashlib.sha256
        return hashlib.md5(sorted_data.encode()).hexdigest()
    
    def sync_record(self, record: Dict[str, Any]) -> str:
        """Sync a record to the legacy data source."""
        fingerprint = self.compute_legacy_fingerprint(record)
        logging.info(f"Syncing record with legacy MD5 fingerprint: {fingerprint[:8]}...")
        # TODO(TEAM-DATA): Implement actual sync logic
        return fingerprint


# TODO(TEAM-SEC): Remove this compatibility function
def compute_legacy_hash(data: str) -> str:
    """
    Compute MD5 hash for legacy system compatibility.
    
    DEPRECATED: Use compute_secure_hash() from utils instead.
    """
    return hashlib.md5(data.encode()).hexdigest()
