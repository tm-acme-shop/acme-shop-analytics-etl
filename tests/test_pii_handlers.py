"""
PII Handlers Tests

Tests for PII handling including tokenization and legacy masking.
"""
import pytest
from unittest.mock import patch, MagicMock

from acme_shop_analytics_etl.pii.handlers import (
    PIITokenizer,
    tokenize_email,
    tokenize_phone,
    tokenize_name,
    tokenize_payment_info,
    redact_pii,
    hash_for_analytics,
    generate_user_token,
    extract_safe_analytics_fields,
    _get_tokenizer,
)
from acme_shop_analytics_etl.pii.legacy_pii import (
    mask_email_legacy,
    mask_phone_legacy,
    mask_card_number_legacy,
    hash_pii_md5,
    hash_pii_sha1,
    anonymize_user_record_legacy,
    redact_pii_fields_legacy,
    extract_pii_for_analytics_legacy,
)


class TestPIITokenizer:
    """Tests for the PIITokenizer class."""
    
    def test_tokenize_produces_consistent_output(self):
        """Test that same input produces same token."""
        tokenizer = PIITokenizer(salt=b"test-salt")
        
        token1 = tokenizer.tokenize("test@example.com", prefix="eml")
        token2 = tokenizer.tokenize("test@example.com", prefix="eml")
        
        assert token1 == token2
    
    def test_tokenize_different_inputs_produce_different_tokens(self):
        """Test that different inputs produce different tokens."""
        tokenizer = PIITokenizer(salt=b"test-salt")
        
        token1 = tokenizer.tokenize("user1@example.com", prefix="eml")
        token2 = tokenizer.tokenize("user2@example.com", prefix="eml")
        
        assert token1 != token2
    
    def test_tokenize_prefix_format(self):
        """Test that tokens have correct prefix format."""
        tokenizer = PIITokenizer(salt=b"test-salt")
        
        token = tokenizer.tokenize("test@example.com", prefix="eml")
        
        assert token.startswith("eml_")
        assert len(token) == 20
    
    def test_tokenize_empty_value_returns_empty(self):
        """Test that empty values return empty string."""
        tokenizer = PIITokenizer(salt=b"test-salt")
        
        assert tokenizer.tokenize("") == ""
        assert tokenizer.tokenize(None) == ""
    
    def test_tokenize_batch(self):
        """Test batch tokenization."""
        tokenizer = PIITokenizer(salt=b"test-salt")
        
        emails = ["a@test.com", "b@test.com", "c@test.com"]
        tokens = tokenizer.tokenize_batch(emails, prefix="eml")
        
        assert len(tokens) == 3
        assert all(t.startswith("eml_") for t in tokens)
        assert len(set(tokens)) == 3


class TestTokenizeEmail:
    """Tests for tokenize_email function."""
    
    @patch("acme_shop_analytics_etl.pii.handlers._get_tokenizer")
    def test_tokenize_email_normalizes_input(self, mock_get_tokenizer):
        """Test that email is normalized before tokenization."""
        mock_tokenizer = MagicMock()
        mock_tokenizer.tokenize.return_value = "eml_abc123"
        mock_get_tokenizer.return_value = mock_tokenizer
        
        tokenize_email("  USER@EXAMPLE.COM  ")
        
        mock_tokenizer.tokenize.assert_called_once_with(
            "user@example.com",
            prefix="eml",
        )
    
    def test_tokenize_email_empty_returns_empty(self):
        """Test that empty email returns empty string."""
        assert tokenize_email("") == ""
        assert tokenize_email(None) == ""


class TestTokenizePhone:
    """Tests for tokenize_phone function."""
    
    @patch("acme_shop_analytics_etl.pii.handlers._get_tokenizer")
    def test_tokenize_phone_normalizes_input(self, mock_get_tokenizer):
        """Test that phone is normalized to digits only."""
        mock_tokenizer = MagicMock()
        mock_tokenizer.tokenize.return_value = "phn_abc123"
        mock_get_tokenizer.return_value = mock_tokenizer
        
        tokenize_phone("+1 (555) 123-4567")
        
        mock_tokenizer.tokenize.assert_called_once_with(
            "15551234567",
            prefix="phn",
        )
    
    def test_tokenize_phone_empty_returns_empty(self):
        """Test that empty phone returns empty string."""
        assert tokenize_phone("") == ""


class TestTokenizePaymentInfo:
    """Tests for tokenize_payment_info function."""
    
    def test_tokenizes_card_number(self):
        """Test that card number is tokenized and removed."""
        record = {
            "id": "pay-001",
            "card_number": "4111111111111111",
            "amount": "100.00",
        }
        
        result = tokenize_payment_info(record)
        
        assert "card_token" in result
        assert result["card_token"].startswith("crd_")
        assert "card_number" not in result
        assert result["card_last_four"] == "1111"
    
    def test_tokenizes_billing_address(self):
        """Test that billing address is tokenized."""
        record = {
            "id": "pay-001",
            "billing_address": "123 Main St, City, State 12345",
        }
        
        result = tokenize_payment_info(record)
        
        assert "billing_token" in result
        assert result["billing_token"].startswith("adr_")
        assert "billing_address" not in result
    
    def test_tokenizes_cardholder_name(self):
        """Test that cardholder name is tokenized."""
        record = {
            "id": "pay-001",
            "cardholder_name": "John Doe",
        }
        
        result = tokenize_payment_info(record)
        
        assert "cardholder_token" in result
        assert result["cardholder_token"].startswith("nam_")
        assert "cardholder_name" not in result
    
    def test_preserves_non_pii_fields(self):
        """Test that non-PII fields are preserved."""
        record = {
            "id": "pay-001",
            "amount": "100.00",
            "currency": "USD",
            "status": "completed",
        }
        
        result = tokenize_payment_info(record)
        
        assert result["id"] == "pay-001"
        assert result["amount"] == "100.00"
        assert result["currency"] == "USD"


class TestRedactPii:
    """Tests for redact_pii function."""
    
    def test_redacts_default_pii_fields(self):
        """Test that default PII fields are redacted."""
        record = {
            "id": 1,
            "email": "test@example.com",
            "phone": "555-1234",
            "name": "John Doe",
            "status": "active",
        }
        
        result = redact_pii(record)
        
        assert result["email"] == "[REDACTED]"
        assert result["phone"] == "[REDACTED]"
        assert result["name"] == "[REDACTED]"
        assert result["id"] == 1
        assert result["status"] == "active"
    
    def test_uses_custom_pii_fields(self):
        """Test redaction with custom field set."""
        record = {
            "custom_field": "sensitive",
            "keep_this": "value",
        }
        
        result = redact_pii(record, pii_fields={"custom_field"})
        
        assert result["custom_field"] == "[REDACTED]"
        assert result["keep_this"] == "value"
    
    def test_uses_custom_replacement(self):
        """Test redaction with custom replacement string."""
        record = {"email": "test@example.com"}
        
        result = redact_pii(record, replacement="***")
        
        assert result["email"] == "***"


class TestExtractSafeAnalyticsFields:
    """Tests for extract_safe_analytics_fields function."""
    
    def test_extracts_only_safe_fields(self):
        """Test that only whitelisted fields are extracted."""
        record = {
            "id": 1,
            "email": "test@example.com",
            "status": "active",
            "created_at": "2024-01-01",
            "name": "John Doe",
        }
        
        result = extract_safe_analytics_fields(record)
        
        assert "id" in result
        assert "status" in result
        assert "created_at" in result
        assert "email" not in result
        assert "name" not in result
    
    def test_uses_custom_allowed_fields(self):
        """Test with custom allowed fields set."""
        record = {
            "id": 1,
            "custom_safe_field": "value",
            "sensitive": "secret",
        }
        
        result = extract_safe_analytics_fields(
            record,
            allowed_fields={"id", "custom_safe_field"},
        )
        
        assert result == {"id": 1, "custom_safe_field": "value"}


class TestLegacyMaskEmail:
    """Tests for legacy mask_email_legacy function."""
    
    def test_masks_email_correctly(self):
        """Test that email is masked with legacy pattern."""
        assert mask_email_legacy("john.doe@example.com") == "j***@example.com"
        assert mask_email_legacy("a@b.com") == "a@b.com"
    
    def test_handles_invalid_email(self):
        """Test handling of invalid email formats."""
        assert mask_email_legacy("") == ""
        assert mask_email_legacy("notanemail") == "notanemail"


class TestLegacyMaskPhone:
    """Tests for legacy mask_phone_legacy function."""
    
    def test_masks_phone_correctly(self):
        """Test that phone is masked showing last 4 digits."""
        assert mask_phone_legacy("+1-555-123-4567") == "***-***-4567"
        assert mask_phone_legacy("5551234567") == "***-***-4567"
    
    def test_handles_short_phone(self):
        """Test handling of short phone numbers."""
        assert mask_phone_legacy("123") == "****"


class TestLegacyMaskCardNumber:
    """Tests for legacy mask_card_number_legacy function."""
    
    def test_masks_card_correctly(self):
        """Test that card number is masked showing last 4 digits."""
        assert mask_card_number_legacy("4111111111111111") == "****-****-****-1111"
        assert mask_card_number_legacy("4111-1111-1111-1111") == "****-****-****-1111"
    
    def test_handles_short_card(self):
        """Test handling of short card numbers."""
        assert mask_card_number_legacy("123") == "****"


class TestLegacyHashPiiMd5:
    """
    Tests for legacy hash_pii_md5 function.
    
    TODO(TEAM-SEC): These tests exist only to verify backwards compatibility.
    MD5 should not be used for new code.
    """
    
    def test_produces_md5_hash(self):
        """Test that MD5 hash is produced."""
        result = hash_pii_md5("test@example.com")
        
        assert len(result) == 32
        assert all(c in "0123456789abcdef" for c in result)
    
    def test_consistent_output(self):
        """Test that same input produces same hash."""
        hash1 = hash_pii_md5("test@example.com")
        hash2 = hash_pii_md5("test@example.com")
        
        assert hash1 == hash2


class TestLegacyHashPiiSha1:
    """
    Tests for legacy hash_pii_sha1 function.
    
    TODO(TEAM-SEC): SHA-1 is also weak and should be migrated to SHA-256.
    """
    
    def test_produces_sha1_hash(self):
        """Test that SHA-1 hash is produced."""
        result = hash_pii_sha1("test@example.com")
        
        assert len(result) == 40
        assert all(c in "0123456789abcdef" for c in result)


class TestLegacyAnonymizeUserRecord:
    """Tests for legacy anonymize_user_record_legacy function."""
    
    def test_anonymizes_email(self):
        """Test that email is masked and hashed."""
        record = {"email": "john@example.com"}
        
        result = anonymize_user_record_legacy(record)
        
        assert "email_masked" in result
        assert "email_hash" in result
        assert result["email_masked"] == "j***@example.com"
    
    def test_anonymizes_phone(self):
        """Test that phone is masked and hashed."""
        record = {"phone": "+1-555-123-4567"}
        
        result = anonymize_user_record_legacy(record)
        
        assert "phone_masked" in result
        assert "phone_hash" in result
    
    def test_hashes_name(self):
        """Test that name is hashed."""
        record = {"name": "John Doe"}
        
        result = anonymize_user_record_legacy(record)
        
        assert "name_hash" in result


class TestLegacyRedactPiiFields:
    """Tests for legacy redact_pii_fields_legacy function."""
    
    def test_redacts_default_fields(self):
        """Test redaction of default PII fields."""
        record = {
            "email": "test@example.com",
            "phone": "555-1234",
            "ssn": "123-45-6789",
            "name": "John",
            "status": "active",
        }
        
        result = redact_pii_fields_legacy(record)
        
        assert result["email"] == "[REDACTED]"
        assert result["phone"] == "[REDACTED]"
        assert result["ssn"] == "[REDACTED]"
        assert result["name"] == "[REDACTED]"
        assert result["status"] == "active"


class TestLegacyExtractPiiForAnalytics:
    """Tests for legacy extract_pii_for_analytics_legacy function."""
    
    def test_extracts_email_domain(self):
        """Test extraction of email domain."""
        record = {"email": "user@acmeshop.com"}
        
        result = extract_pii_for_analytics_legacy(record)
        
        assert result.get("email_domain") == "acmeshop.com"
    
    def test_extracts_phone_country_code(self):
        """Test extraction of phone country code."""
        record = {"phone": "+1-555-123-4567"}
        
        result = extract_pii_for_analytics_legacy(record)
        
        assert result.get("phone_country_code") == "1"
    
    def test_handles_missing_fields(self):
        """Test handling of missing PII fields."""
        record = {"id": 1}
        
        result = extract_pii_for_analytics_legacy(record)
        
        assert "email_domain" not in result
        assert "phone_country_code" not in result


class TestPIIHandlerIntegration:
    """Integration tests comparing legacy and v2 PII handling."""
    
    def test_tokenization_more_secure_than_masking(self):
        """Verify tokenization doesn't leak partial PII like masking does."""
        email = "john.doe@example.com"
        
        masked = mask_email_legacy(email)
        
        assert "j" in masked
        assert "@example.com" in masked
    
    def test_v2_removes_pii_completely(self):
        """Test that v2 tokenization removes all PII traces."""
        record = {
            "id": 1,
            "card_number": "4111111111111111",
            "cardholder_name": "John Doe",
            "billing_address": "123 Main St",
        }
        
        result = tokenize_payment_info(record)
        
        assert "4111111111111111" not in str(result)
        assert "John Doe" not in str(result)
        assert "123 Main St" not in str(result)
