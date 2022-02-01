"""
Deduplication Tests

Tests for the record deduplication module.
"""
import pytest

from acme_shop_analytics_etl.etl.deduplication import (
    compute_record_fingerprint_md5,
    compute_field_fingerprint_md5,
    compute_user_identity_hash_legacy,
)


class TestComputeRecordFingerprintMd5:
    """Tests for MD5 record fingerprinting."""
    
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


class TestComputeFieldFingerprint:
    """Tests for field-specific fingerprinting."""
    
    def test_md5_fingerprint_specific_fields(self):
        """Test MD5 fingerprint from specific fields."""
        record = {"id": 1, "email": "test@test.com", "name": "John"}
        
        fp = compute_field_fingerprint_md5(record, ["email", "name"])
        
        assert len(fp) == 32
    
    def test_field_order_independent(self):
        """Test that field order in list doesn't affect fingerprint."""
        record = {"a": 1, "b": 2, "c": 3}
        
        fp1 = compute_field_fingerprint_md5(record, ["a", "b", "c"])
        fp2 = compute_field_fingerprint_md5(record, ["c", "b", "a"])
        
        assert fp1 == fp2
    
    def test_missing_fields_handled(self):
        """Test handling of missing fields."""
        record = {"a": 1}
        
        fp = compute_field_fingerprint_md5(record, ["a", "b", "c"])
        
        assert len(fp) == 32


class TestComputeUserIdentityHash:
    """Tests for user identity hashing."""
    
    def test_produces_md5(self):
        """Test that function produces MD5 hash."""
        result = compute_user_identity_hash_legacy(
            email="test@test.com",
            phone="+1-555-1234",
            name="John Doe",
        )
        
        assert len(result) == 32
    
    def test_handles_missing_fields(self):
        """Test handling of None/missing identity fields."""
        result = compute_user_identity_hash_legacy(email="test@test.com")
        
        assert len(result) == 32
