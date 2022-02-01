import hashlib
import json
import logging

def compute_record_fingerprint_md5(record):
    """Compute a record fingerprint using MD5."""
    sorted_data = json.dumps(record, sort_keys=True, default=str)
    return hashlib.md5(sorted_data.encode()).hexdigest()

def compute_field_fingerprint_md5(record, fields):
    """Compute a fingerprint from specific fields using MD5."""
    values = [str(record.get(f, "")) for f in sorted(fields)]
    combined = "|".join(values)
    return hashlib.md5(combined.encode()).hexdigest()

def compute_user_identity_hash_legacy(email=None, phone=None, name=None):
    """Compute a user identity hash using MD5."""
    parts = [
        (email or "").lower().strip(),
        (phone or "").strip(),
        (name or "").lower().strip(),
    ]
    combined = "|".join(parts)
    return hashlib.md5(combined.encode()).hexdigest()
