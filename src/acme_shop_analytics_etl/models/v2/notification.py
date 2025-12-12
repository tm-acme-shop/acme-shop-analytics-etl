"""
V2 Notification Models

Modern notification models without raw recipient PII.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional
from enum import Enum


class NotificationChannel(str, Enum):
    """Notification delivery channel."""
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    IN_APP = "in_app"
    WEBHOOK = "webhook"


class NotificationType(str, Enum):
    """Types of notifications."""
    ORDER_CONFIRMATION = "order_confirmation"
    ORDER_SHIPPED = "order_shipped"
    ORDER_DELIVERED = "order_delivered"
    ORDER_CANCELLED = "order_cancelled"
    PAYMENT_RECEIVED = "payment_received"
    PAYMENT_FAILED = "payment_failed"
    REFUND_PROCESSED = "refund_processed"
    PASSWORD_RESET = "password_reset"
    ACCOUNT_VERIFICATION = "account_verification"
    PROMOTIONAL = "promotional"
    SYSTEM_ALERT = "system_alert"


class NotificationStatus(str, Enum):
    """Notification delivery status."""
    PENDING = "pending"
    QUEUED = "queued"
    SENT = "sent"
    DELIVERED = "delivered"
    OPENED = "opened"
    CLICKED = "clicked"
    BOUNCED = "bounced"
    FAILED = "failed"
    UNSUBSCRIBED = "unsubscribed"


@dataclass
class Notification:
    """
    V2 Notification model.
    
    References users by token/ID instead of storing raw contact info.
    
    Attributes:
        id: Notification ID.
        user_id: Recipient user ID.
        user_token: Recipient user token.
        channel: Delivery channel.
        notification_type: Type of notification.
        status: Delivery status.
    """
    
    id: int
    user_id: int
    user_token: str
    
    channel: NotificationChannel
    notification_type: NotificationType
    status: NotificationStatus = NotificationStatus.PENDING
    
    # Template reference (content stored separately)
    template_id: Optional[str] = None
    template_version: Optional[int] = None
    
    # Personalization data (no PII, just references)
    order_id: Optional[int] = None
    product_ids: Optional[str] = None  # JSON array of product IDs
    
    # Timestamps
    created_at: Optional[datetime] = None
    queued_at: Optional[datetime] = None
    sent_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    opened_at: Optional[datetime] = None
    clicked_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    
    # Provider tracking
    provider: Optional[str] = None  # sendgrid, twilio, firebase
    provider_message_id: Optional[str] = None
    
    # Error handling
    failure_reason: Optional[str] = None
    failure_code: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    
    # Tracking
    campaign_id: Optional[str] = None
    correlation_id: Optional[str] = None  # X-Acme-Request-ID
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "user_token": self.user_token,
            "channel": self.channel.value if isinstance(self.channel, NotificationChannel) else self.channel,
            "notification_type": self.notification_type.value if isinstance(self.notification_type, NotificationType) else self.notification_type,
            "status": self.status.value if isinstance(self.status, NotificationStatus) else self.status,
            "template_id": self.template_id,
            "template_version": self.template_version,
            "order_id": self.order_id,
            "created_at": self.created_at,
            "queued_at": self.queued_at,
            "sent_at": self.sent_at,
            "delivered_at": self.delivered_at,
            "opened_at": self.opened_at,
            "clicked_at": self.clicked_at,
            "failed_at": self.failed_at,
            "provider": self.provider,
            "provider_message_id": self.provider_message_id,
            "retry_count": self.retry_count,
            "campaign_id": self.campaign_id,
            "correlation_id": self.correlation_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Notification":
        """Create instance from dictionary."""
        channel = data["channel"]
        if isinstance(channel, str):
            channel = NotificationChannel(channel)
        
        notif_type = data["notification_type"]
        if isinstance(notif_type, str):
            notif_type = NotificationType(notif_type)
        
        status = data.get("status", "pending")
        if isinstance(status, str):
            status = NotificationStatus(status)
        
        return cls(
            id=data["id"],
            user_id=data["user_id"],
            user_token=data["user_token"],
            channel=channel,
            notification_type=notif_type,
            status=status,
            template_id=data.get("template_id"),
            template_version=data.get("template_version"),
            order_id=data.get("order_id"),
            product_ids=data.get("product_ids"),
            created_at=data.get("created_at"),
            queued_at=data.get("queued_at"),
            sent_at=data.get("sent_at"),
            delivered_at=data.get("delivered_at"),
            opened_at=data.get("opened_at"),
            clicked_at=data.get("clicked_at"),
            failed_at=data.get("failed_at"),
            provider=data.get("provider"),
            provider_message_id=data.get("provider_message_id"),
            failure_reason=data.get("failure_reason"),
            failure_code=data.get("failure_code"),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            campaign_id=data.get("campaign_id"),
            correlation_id=data.get("correlation_id"),
        )
    
    def is_delivered(self) -> bool:
        """Check if notification was delivered."""
        return self.status in (
            NotificationStatus.DELIVERED,
            NotificationStatus.OPENED,
            NotificationStatus.CLICKED,
        )
    
    def is_engaged(self) -> bool:
        """Check if user engaged with notification."""
        return self.status in (
            NotificationStatus.OPENED,
            NotificationStatus.CLICKED,
        )
    
    def can_retry(self) -> bool:
        """Check if notification can be retried."""
        return self.retry_count < self.max_retries and self.status == NotificationStatus.FAILED
    
    def calculate_delivery_time_ms(self) -> Optional[int]:
        """Calculate time from sent to delivered in milliseconds."""
        if self.sent_at and self.delivered_at:
            delta = self.delivered_at - self.sent_at
            return int(delta.total_seconds() * 1000)
        return None
