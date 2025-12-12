"""
Deduplication Tests

Tests for the record deduplication module including MD5 (deprecated) and SHA-256.
"""
import pytest
from unittest.mock import patch

from acme_shop_analytics_etl.etl.deduplication import (
    compute_record_fingerprint_md5,
    compute_record_fingerprint_sha256,
    compute_record_fingerprint,
    compute_field_fingerprint_md5,
    compute_field_fingerprint_sha256,
    compute_user_identity_hash_legacy,
    compute_user_identity_hash,
    RecordDeduplicator,
)
from acme_shop_analytics_etl.config.feature_flags import get_feature_flags


class TestComputeRecordFingerprintMd5:
    """
    Tests for MD5 record fingerprinting.
    
    TODO(TEAM-SEC): MD5 is deprecated. These tests exist for backwards compatibility.
    """
    
    def test_produces_md5_hash(self):
        """Test that MD5 hash is produced."""
        record = {"id": 1, "name": "test"}
        
        fingerprint = compute_record_fingerprint_md5(record)
        
        assert len(fingerprint) == 32
        assert all(c in "0123456789abcdef" for c in fingerprint)
    
    def test_consistent_output_for_same_input(self):
        """Test that same record produces same fingerprint."""
        record = {"id": 1, "name": "test", "value": 100}
        
        fp1 = compute_record_fingerprint_md5(record)
        fp2 = compute_record_fingerprint_md5(record)
        
        assert fp1 == fp2
    
    def test_different_output_for_different_input(self):
        """Test that different records produce different fingerprints."""
        record1 = {"id": 1, "name": "test"}
        record2 = {"id": 2, "name": "test"}
        
        fp1 = compute_record_fingerprint_md5(record1)
        fp2 = compute_record_fingerprint_md5(record2)
        
        assert fp1 != fp2
    
    def test_key_order_independent(self):
        """Test that key order doesn't affect fingerprint."""
        record1 = {"a": 1, "b": 2, "c": 3}
        record2 = {"c": 3, "a": 1, "b": 2}
        
        fp1 = compute_record_fingerprint_md5(record1)
        fp2 = compute_record_fingerprint_md5(record2)
        
        assert fp1 == fp2


class TestComputeRecordFingerprintSha256:
    """Tests for SHA-256 record fingerprinting (recommended)."""
    
    def test_produces_sha256_hash(self):
        """Test that SHA-256 hash is produced."""
        record = {"id": 1, "name": "test"}
        
        fingerprint = compute_record_fingerprint_sha256(record)
        
        assert len(fingerprint) == 64
        assert all(c in "0123456789abcdef" for c in fingerprint)
    
    def test_consistent_output_for_same_input(self):
        """Test that same record produces same fingerprint."""
        record = {"id": 1, "name": "test", "value": 100}
        
        fp1 = compute_record_fingerprint_sha256(record)
        fp2 = compute_record_fingerprint_sha256(record)
        
        assert fp1 == fp2
    
    def test_different_output_for_different_input(self):
        """Test that different records produce different fingerprints."""
        record1 = {"id": 1, "name": "test"}
        record2 = {"id": 2, "name": "test"}
        
        fp1 = compute_record_fingerprint_sha256(record1)
        fp2 = compute_record_fingerprint_sha256(record2)
        
        assert fp1 != fp2
    
    def test_key_order_independent(self):
        """Test that key order doesn't affect fingerprint."""
        record1 = {"a": 1, "b": 2, "c": 3}
        record2 = {"c": 3, "a": 1, "b": 2}
        
        fp1 = compute_record_fingerprint_sha256(record1)
        fp2 = compute_record_fingerprint_sha256(record2)
        
        assert fp1 == fp2
    
    def test_handles_nested_objects(self):
        """Test handling of nested dictionaries."""
        record = {
            "id": 1,
            "metadata": {"key": "value", "nested": {"deep": True}},
        }
        
        fingerprint = compute_record_fingerprint_sha256(record)
        
        assert len(fingerprint) == 64


class TestComputeRecordFingerprint:
    """Tests for the feature-flag-aware fingerprint function."""
    
    def test_uses_md5_when_legacy_enabled(self, monkeypatch):
        """Test that MD5 is used when legacy ETL is enabled."""
        monkeypatch.setenv("ENABLE_LEGACY_ETL", "true")
        get_feature_flags.cache_clear()
        
        record = {"id": 1}
        
        fingerprint = compute_record_fingerprint(record)
        
        assert len(fingerprint) == 32
    
    def test_uses_sha256_when_legacy_disabled(self, monkeypatch):
        """Test that SHA-256 is used when legacy ETL is disabled."""
        monkeypatch.setenv("ENABLE_LEGACY_ETL", "false")
        get_feature_flags.cache_clear()
        
        record = {"id": 1}
        
        fingerprint = compute_record_fingerprint(record)
        
        assert len(fingerprint) == 64


class TestComputeFieldFingerprint:
    """Tests for field-specific fingerprinting."""
    
    def test_md5_fingerprint_specific_fields(self):
        """Test MD5 fingerprint from specific fields."""
        record = {"id": 1, "email": "test@test.com", "name": "John"}
        
        fp = compute_field_fingerprint_md5(record, ["email", "name"])
        
        assert len(fp) == 32
    
    def test_sha256_fingerprint_specific_fields(self):
        """Test SHA-256 fingerprint from specific fields."""
        record = {"id": 1, "email": "test@test.com", "name": "John"}
        
        fp = compute_field_fingerprint_sha256(record, ["email", "name"])
        
        assert len(fp) == 64
    
    def test_field_order_independent(self):
        """Test that field order in list doesn't affect fingerprint."""
        record = {"a": 1, "b": 2, "c": 3}
        
        fp1 = compute_field_fingerprint_sha256(record, ["a", "b", "c"])
        fp2 = compute_field_fingerprint_sha256(record, ["c", "b", "a"])
        
        assert fp1 == fp2
    
    def test_missing_fields_handled(self):
        """Test handling of missing fields."""
        record = {"a": 1}
        
        fp = compute_field_fingerprint_sha256(record, ["a", "b", "c"])
        
        assert len(fp) == 64


class TestComputeUserIdentityHash:
    """Tests for user identity hashing."""
    
    def test_legacy_produces_md5(self):
        """Test that legacy function produces MD5 hash."""
        result = compute_user_identity_hash_legacy(
            email="test@test.com",
            phone="+1-555-1234",
            name="John Doe",
        )
        
        assert len(result) == 32
    
    def test_v2_produces_sha256(self):
        """Test that v2 function produces SHA-256 hash."""
        result = compute_user_identity_hash(
            email="test@test.com",
            phone="+1-555-1234",
            name="John Doe",
        )
        
        assert len(result) == 64
    
    def test_normalizes_inputs(self):
        """Test that inputs are normalized."""
        hash1 = compute_user_identity_hash(
            email="TEST@Test.Com",
            name="JOHN DOE",
        )
        hash2 = compute_user_identity_hash(
            email="test@test.com",
            name="john doe",
        )
        
        assert hash1 == hash2
    
    def test_handles_missing_fields(self):
        """Test handling of None/missing identity fields."""
        result = compute_user_identity_hash(email="test@test.com")
        
        assert len(result) == 64


class TestRecordDeduplicator:
    """Tests for the RecordDeduplicator class."""
    
    def test_init_default_sha256(self):
        """Test that deduplicator defaults to SHA-256."""
        dedup = RecordDeduplicator(use_legacy_hash=False)
        
        assert dedup._use_legacy is False
    
    def test_init_legacy_md5(self):
        """Test that deduplicator can use legacy MD5."""
        dedup = RecordDeduplicator(use_legacy_hash=True)
        
        assert dedup._use_legacy is True
    
    def test_is_duplicate_false_for_new_record(self):
        """Test that new records are not marked as duplicates."""
        dedup = RecordDeduplicator()
        record = {"id": 1, "name": "test"}
        
        assert dedup.is_duplicate(record) is False
    
    def test_is_duplicate_true_after_marking(self):
        """Test that records are duplicates after marking."""
        dedup = RecordDeduplicator()
        record = {"id": 1, "name": "test"}
        
        dedup.mark_seen(record)
        
        assert dedup.is_duplicate(record) is True
    
    def test_mark_seen_returns_fingerprint(self):
        """Test that mark_seen returns the fingerprint."""
        dedup = RecordDeduplicator()
        record = {"id": 1, "name": "test"}
        
        fingerprint = dedup.mark_seen(record)
        
        assert len(fingerprint) == 64
    
    def test_process_record_new(self):
        """Test process_record for a new record."""
        dedup = RecordDeduplicator()
        record = {"id": 1, "name": "test"}
        
        is_new, fingerprint = dedup.process_record(record)
        
        assert is_new is True
        assert len(fingerprint) == 64
    
    def test_process_record_duplicate(self):
        """Test process_record for a duplicate record."""
        dedup = RecordDeduplicator()
        record = {"id": 1, "name": "test"}
        
        dedup.process_record(record)
        is_new, _ = dedup.process_record(record)
        
        assert is_new is False
    
    def test_seen_count(self):
        """Test that seen_count tracks unique records."""
        dedup = RecordDeduplicator()
        
        dedup.mark_seen({"id": 1})
        dedup.mark_seen({"id": 2})
        dedup.mark_seen({"id": 3})
        dedup.mark_seen({"id": 1})
        
        assert dedup.seen_count == 3
    
    def test_clear(self):
        """Test that clear resets the deduplicator."""
        dedup = RecordDeduplicator()
        dedup.mark_seen({"id": 1})
        dedup.mark_seen({"id": 2})
        
        dedup.clear()
        
        assert dedup.seen_count == 0
        assert dedup.is_duplicate({"id": 1}) is False


class TestRecordDeduplicatorBatch:
    """Tests for batch deduplication."""
    
    def test_deduplicate_batch_removes_duplicates(self):
        """Test that batch deduplication removes duplicate records."""
        dedup = RecordDeduplicator()
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 1, "name": "Alice"},
            {"id": 3, "name": "Charlie"},
            {"id": 2, "name": "Bob"},
        ]
        
        unique = dedup.deduplicate_batch(records)
        
        assert len(unique) == 3
    
    def test_deduplicate_batch_preserves_order(self):
        """Test that batch deduplication preserves first occurrence order."""
        dedup = RecordDeduplicator()
        records = [
            {"id": 3, "name": "Charlie"},
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 1, "name": "Alice"},
        ]
        
        unique = dedup.deduplicate_batch(records)
        
        assert unique[0]["id"] == 3
        assert unique[1]["id"] == 1
        assert unique[2]["id"] == 2
    
    def test_deduplicate_batch_empty_list(self):
        """Test batch deduplication with empty list."""
        dedup = RecordDeduplicator()
        
        unique = dedup.deduplicate_batch([])
        
        assert unique == []
    
    def test_deduplicate_batch_no_duplicates(self):
        """Test batch deduplication with no duplicates."""
        dedup = RecordDeduplicator()
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
        
        unique = dedup.deduplicate_batch(records)
        
        assert len(unique) == 3
    
    def test_deduplicate_batch_all_duplicates(self):
        """Test batch deduplication when all records are duplicates."""
        dedup = RecordDeduplicator()
        record = {"id": 1, "name": "Alice"}
        records = [record.copy() for _ in range(5)]
        
        unique = dedup.deduplicate_batch(records)
        
        assert len(unique) == 1


class TestDeduplicationIntegration:
    """Integration tests for deduplication with feature flags."""
    
    def test_legacy_md5_vs_v2_sha256_different(self):
        """Verify that MD5 and SHA-256 produce different fingerprints."""
        record = {"id": 1, "name": "test", "value": 42}
        
        md5_fp = compute_record_fingerprint_md5(record)
        sha256_fp = compute_record_fingerprint_sha256(record)
        
        assert md5_fp != sha256_fp
        assert len(md5_fp) == 32
        assert len(sha256_fp) == 64
    
    def test_deduplicator_with_legacy_fixture(self, enable_legacy_flags):
        """Test deduplicator behavior with legacy flags enabled."""
        from acme_shop_analytics_etl.config.feature_flags import is_legacy_etl_enabled
        
        assert is_legacy_etl_enabled() is True
        
        dedup = RecordDeduplicator(use_legacy_hash=is_legacy_etl_enabled())
        fingerprint = dedup.compute_fingerprint({"id": 1})
        
        assert len(fingerprint) == 32
    
    def test_deduplicator_with_v2_fixture(self, disable_legacy_flags):
        """Test deduplicator behavior with v2 (legacy disabled)."""
        from acme_shop_analytics_etl.config.feature_flags import is_legacy_etl_enabled
        
        assert is_legacy_etl_enabled() is False
        
        dedup = RecordDeduplicator(use_legacy_hash=is_legacy_etl_enabled())
        fingerprint = dedup.compute_fingerprint({"id": 1})
        
        assert len(fingerprint) == 64
    
    def test_cross_algorithm_not_compatible(self):
        """Verify records fingerprinted with different algorithms are not compatible."""
        record = {"id": 1, "name": "test"}
        
        md5_dedup = RecordDeduplicator(use_legacy_hash=True)
        sha256_dedup = RecordDeduplicator(use_legacy_hash=False)
        
        md5_fp = md5_dedup.compute_fingerprint(record)
        sha256_fp = sha256_dedup.compute_fingerprint(record)
        
        assert md5_fp != sha256_fp
