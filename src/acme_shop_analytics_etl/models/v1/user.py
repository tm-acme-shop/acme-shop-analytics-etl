"""
V1 User Models (Legacy)

DEPRECATED: These models store raw PII and use unsafe patterns.
TODO(TEAM-SEC): Migrate to v2 models with tokenized PII.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class UserV1:
    """
    Legacy user model with direct PII storage.
    
    DEPRECATED: This model stores raw PII which violates data protection policies.
    TODO(TEAM-SEC): Migrate to User model in v2 which uses tokenization.
    
    Attributes:
        id: User ID.
        email: Raw email address (DEPRECATED - should be tokenized).
        phone: Raw phone number (DEPRECATED - should be tokenized).
        name: Full name (DEPRECATED - should be tokenized).
        created_at: Account creation timestamp.
        last_login_at: Last login timestamp.
        status: Account status.
        subscription_type: Subscription tier.
        email_verified: Whether email is verified (bool stored as int).
    """
    
    id: int
    email: str  # TODO(TEAM-SEC): Raw PII - migrate to email_token
    phone: Optional[str] = None  # TODO(TEAM-SEC): Raw PII - migrate to phone_token
    name: Optional[str] = None  # TODO(TEAM-SEC): Raw PII - migrate to name_token
    created_at: Optional[datetime] = None
    last_login_at: Optional[datetime] = None
    status: str = "active"
    subscription_type: str = "free"
    email_verified: int = 0  # TODO(TEAM-API): Should be bool, not int
    
    # Legacy fields that shouldn't exist
    password_hash: Optional[str] = None  # TODO(TEAM-SEC): Should not be in analytics
    ip_address: Optional[str] = None  # TODO(TEAM-SEC): PII - should not be stored
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations."""
        return {
            "id": self.id,
            "email": self.email,
            "phone": self.phone,
            "name": self.name,
            "created_at": self.created_at,
            "last_login_at": self.last_login_at,
            "status": self.status,
            "subscription_type": self.subscription_type,
            "email_verified": self.email_verified,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserV1":
        """Create instance from dictionary."""
        return cls(
            id=data["id"],
            email=data["email"],
            phone=data.get("phone"),
            name=data.get("name"),
            created_at=data.get("created_at"),
            last_login_at=data.get("last_login_at"),
            status=data.get("status", "active"),
            subscription_type=data.get("subscription_type", "free"),
            email_verified=data.get("email_verified", 0),
        )


@dataclass
class UserActivityV1:
    """
    Legacy user activity model.
    
    DEPRECATED: Uses denormalized schema and stores raw metadata.
    TODO(TEAM-API): Migrate to v2 structured activity model.
    """
    
    id: int
    user_id: int
    activity_type: str
    metadata: Optional[str] = None  # TODO(TEAM-API): Should be structured JSON, not string
    created_at: Optional[datetime] = None
    ip_address: Optional[str] = None  # TODO(TEAM-SEC): PII - should not be stored
    user_agent: Optional[str] = None  # TODO(TEAM-SEC): Fingerprinting data
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "activity_type": self.activity_type,
            "metadata": self.metadata,
            "created_at": self.created_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserActivityV1":
        """Create instance from dictionary."""
        return cls(
            id=data["id"],
            user_id=data["user_id"],
            activity_type=data["activity_type"],
            metadata=data.get("metadata"),
            created_at=data.get("created_at"),
            ip_address=data.get("ip_address"),
            user_agent=data.get("user_agent"),
        )
