"""
V1 Notification Models (Legacy)

DEPRECATED: These models store recipient PII directly.
TODO(TEAM-API): Migrate to v2 models with tokenized recipients.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class NotificationV1:
    """
    Legacy notification model.
    
    DEPRECATED: Stores raw recipient PII.
    TODO(TEAM-SEC): Migrate to Notification model in v2 with tokens.
    
    Attributes:
        id: Notification ID.
        user_id: Recipient user ID.
        channel: Notification channel (email, sms, push).
        recipient_email: Raw email (DEPRECATED - should use user reference).
        recipient_phone: Raw phone (DEPRECATED - should use user reference).
    """
    
    id: int
    user_id: int
    channel: str  # email, sms, push
    notification_type: str  # order_confirmation, shipping_update, etc.
    status: str = "pending"
    
    # Raw recipient PII (anti-pattern)
    recipient_email: Optional[str] = None  # TODO(TEAM-SEC): Raw PII - duplicate of user.email
    recipient_phone: Optional[str] = None  # TODO(TEAM-SEC): Raw PII - duplicate of user.phone
    recipient_name: Optional[str] = None  # TODO(TEAM-SEC): Raw PII
    
    # Content (may contain PII)
    subject: Optional[str] = None
    body: Optional[str] = None  # TODO(TEAM-SEC): May contain PII in message body
    
    # Timestamps
    created_at: Optional[datetime] = None
    sent_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    opened_at: Optional[datetime] = None
    clicked_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    
    # Legacy tracking
    error_message: Optional[str] = None
    external_id: Optional[str] = None  # Provider's message ID
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (excludes PII)."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "channel": self.channel,
            "notification_type": self.notification_type,
            "status": self.status,
            "created_at": self.created_at,
            "sent_at": self.sent_at,
            "delivered_at": self.delivered_at,
            "opened_at": self.opened_at,
            "clicked_at": self.clicked_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NotificationV1":
        """Create instance from dictionary."""
        return cls(
            id=data["id"],
            user_id=data["user_id"],
            channel=data["channel"],
            notification_type=data["notification_type"],
            status=data.get("status", "pending"),
            recipient_email=data.get("recipient_email"),
            recipient_phone=data.get("recipient_phone"),
            recipient_name=data.get("recipient_name"),
            subject=data.get("subject"),
            body=data.get("body"),
            created_at=data.get("created_at"),
            sent_at=data.get("sent_at"),
            delivered_at=data.get("delivered_at"),
            opened_at=data.get("opened_at"),
            clicked_at=data.get("clicked_at"),
            failed_at=data.get("failed_at"),
            error_message=data.get("error_message"),
            external_id=data.get("external_id"),
        )
    
    def is_delivered(self) -> bool:
        """Check if notification was delivered."""
        return self.delivered_at is not None
    
    def is_opened(self) -> bool:
        """Check if notification was opened."""
        return self.opened_at is not None
    
    def is_clicked(self) -> bool:
        """Check if notification was clicked."""
        return self.clicked_at is not None
