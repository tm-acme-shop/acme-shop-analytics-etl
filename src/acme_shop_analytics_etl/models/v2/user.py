"""
V2 User Models

Modern user models with tokenized PII and proper typing.
"""
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
from enum import Enum


class UserStatus(str, Enum):
    """User account status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"
    CHURNED = "churned"
    PENDING_VERIFICATION = "pending_verification"


class SubscriptionTier(str, Enum):
    """User subscription tier."""
    FREE = "free"
    BASIC = "basic"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"


@dataclass
class User:
    """
    V2 User model with tokenized PII.
    
    This model stores only tokens instead of raw PII, complying with
    data protection requirements.
    
    Attributes:
        id: User ID.
        user_token: Unique token for referencing user externally.
        email_token: Tokenized email (not reversible without key).
        phone_token: Tokenized phone (not reversible without key).
        name_token: Tokenized name (not reversible without key).
        identity_hash: SHA-256 hash for deduplication.
        status: Account status.
        subscription_tier: Subscription tier.
        email_verified_at: Email verification timestamp.
    """
    
    id: int
    user_token: str
    email_token: Optional[str] = None
    phone_token: Optional[str] = None
    name_token: Optional[str] = None
    identity_hash: Optional[str] = None
    status: UserStatus = UserStatus.ACTIVE
    subscription_tier: SubscriptionTier = SubscriptionTier.FREE
    email_verified_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_activity_at: Optional[datetime] = None
    
    # Analytics metadata (non-PII)
    signup_source: Optional[str] = None
    country_code: Optional[str] = None
    timezone: Optional[str] = None
    preferred_language: str = "en"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations."""
        return {
            "id": self.id,
            "user_token": self.user_token,
            "email_token": self.email_token,
            "phone_token": self.phone_token,
            "name_token": self.name_token,
            "identity_hash": self.identity_hash,
            "status": self.status.value if isinstance(self.status, UserStatus) else self.status,
            "subscription_tier": self.subscription_tier.value if isinstance(self.subscription_tier, SubscriptionTier) else self.subscription_tier,
            "email_verified_at": self.email_verified_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_activity_at": self.last_activity_at,
            "signup_source": self.signup_source,
            "country_code": self.country_code,
            "timezone": self.timezone,
            "preferred_language": self.preferred_language,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "User":
        """Create instance from dictionary."""
        status = data.get("status", "active")
        if isinstance(status, str):
            status = UserStatus(status)
        
        tier = data.get("subscription_tier", "free")
        if isinstance(tier, str):
            tier = SubscriptionTier(tier)
        
        return cls(
            id=data["id"],
            user_token=data["user_token"],
            email_token=data.get("email_token"),
            phone_token=data.get("phone_token"),
            name_token=data.get("name_token"),
            identity_hash=data.get("identity_hash"),
            status=status,
            subscription_tier=tier,
            email_verified_at=data.get("email_verified_at"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            last_activity_at=data.get("last_activity_at"),
            signup_source=data.get("signup_source"),
            country_code=data.get("country_code"),
            timezone=data.get("timezone"),
            preferred_language=data.get("preferred_language", "en"),
        )
    
    def is_verified(self) -> bool:
        """Check if user's email is verified."""
        return self.email_verified_at is not None
    
    def is_premium(self) -> bool:
        """Check if user has a paid subscription."""
        return self.subscription_tier in (SubscriptionTier.PREMIUM, SubscriptionTier.ENTERPRISE)


class ActivityType(str, Enum):
    """Types of user activity."""
    LOGIN = "login"
    LOGOUT = "logout"
    PAGE_VIEW = "page_view"
    SEARCH = "search"
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    CHECKOUT_START = "checkout_start"
    PURCHASE = "purchase"
    REVIEW_SUBMIT = "review_submit"
    PROFILE_UPDATE = "profile_update"


@dataclass
class UserActivity:
    """
    V2 User activity model.
    
    Structured activity tracking without PII.
    """
    
    id: int
    user_id: int
    user_token: str
    activity_type: ActivityType
    created_at: datetime
    
    # Structured metadata
    session_id: Optional[str] = None
    page_path: Optional[str] = None
    referrer: Optional[str] = None
    duration_seconds: Optional[int] = None
    
    # Device info (non-identifying)
    device_type: Optional[str] = None  # mobile, desktop, tablet
    platform: Optional[str] = None  # web, ios, android
    
    # Context
    product_id: Optional[int] = None
    category_id: Optional[int] = None
    search_query: Optional[str] = None  # Sanitized, no PII
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "user_token": self.user_token,
            "activity_type": self.activity_type.value if isinstance(self.activity_type, ActivityType) else self.activity_type,
            "created_at": self.created_at,
            "session_id": self.session_id,
            "page_path": self.page_path,
            "duration_seconds": self.duration_seconds,
            "device_type": self.device_type,
            "platform": self.platform,
            "product_id": self.product_id,
            "category_id": self.category_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserActivity":
        """Create instance from dictionary."""
        activity_type = data["activity_type"]
        if isinstance(activity_type, str):
            activity_type = ActivityType(activity_type)
        
        return cls(
            id=data["id"],
            user_id=data["user_id"],
            user_token=data["user_token"],
            activity_type=activity_type,
            created_at=data["created_at"],
            session_id=data.get("session_id"),
            page_path=data.get("page_path"),
            referrer=data.get("referrer"),
            duration_seconds=data.get("duration_seconds"),
            device_type=data.get("device_type"),
            platform=data.get("platform"),
            product_id=data.get("product_id"),
            category_id=data.get("category_id"),
            search_query=data.get("search_query"),
        )
