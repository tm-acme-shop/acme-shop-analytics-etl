"""
V1 Payment Models (Legacy)

DEPRECATED: These models store sensitive payment data directly.
TODO(TEAM-PAYMENTS): Migrate to v2 models with proper tokenization.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class PaymentV1:
    """
    Legacy payment model.
    
    DEPRECATED: Stores raw card data which violates PCI-DSS.
    TODO(TEAM-SEC): Migrate to Payment model in v2 with tokenized data.
    
    Attributes:
        id: Payment ID.
        order_id: Associated order ID.
        amount: Payment amount as float (DEPRECATED - use Decimal).
        status: Payment status.
        card_number: Raw card number (CRITICAL - PCI violation).
        card_last_four: Last 4 digits of card.
    """
    
    id: int
    order_id: int
    user_id: int
    amount: float  # TODO(TEAM-PAYMENTS): Should be Decimal
    currency: str = "USD"
    status: str = "pending"
    payment_method: str = "card"
    created_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    
    # CRITICAL: Raw payment data (PCI-DSS violation)
    card_number: Optional[str] = None  # TODO(TEAM-SEC): CRITICAL - Must remove
    card_expiry: Optional[str] = None  # TODO(TEAM-SEC): CRITICAL - Must remove
    card_cvv: Optional[str] = None  # TODO(TEAM-SEC): CRITICAL - Never store CVV
    card_last_four: Optional[str] = None
    cardholder_name: Optional[str] = None  # TODO(TEAM-SEC): PII
    
    # Billing PII
    billing_address: Optional[str] = None  # TODO(TEAM-SEC): PII
    billing_email: Optional[str] = None  # TODO(TEAM-SEC): Duplicated PII
    billing_phone: Optional[str] = None  # TODO(TEAM-SEC): Duplicated PII
    
    # Legacy transaction data
    gateway_response: Optional[str] = None  # May contain sensitive data
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (excludes sensitive fields)."""
        return {
            "id": self.id,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "amount": self.amount,
            "currency": self.currency,
            "status": self.status,
            "payment_method": self.payment_method,
            "created_at": self.created_at,
            "processed_at": self.processed_at,
            "card_last_four": self.card_last_four,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PaymentV1":
        """Create instance from dictionary."""
        return cls(
            id=data["id"],
            order_id=data["order_id"],
            user_id=data["user_id"],
            amount=float(data.get("amount", 0)),
            currency=data.get("currency", "USD"),
            status=data.get("status", "pending"),
            payment_method=data.get("payment_method", "card"),
            created_at=data.get("created_at"),
            processed_at=data.get("processed_at"),
            card_number=data.get("card_number"),
            card_expiry=data.get("card_expiry"),
            card_last_four=data.get("card_last_four"),
            cardholder_name=data.get("cardholder_name"),
            billing_address=data.get("billing_address"),
        )


@dataclass
class RefundV1:
    """
    Legacy refund model.
    
    DEPRECATED: Uses float for amounts.
    TODO(TEAM-PAYMENTS): Migrate to Refund in v2.
    """
    
    id: int
    payment_id: int
    order_id: int
    amount: float  # TODO(TEAM-PAYMENTS): Should be Decimal
    reason: Optional[str] = None
    status: str = "pending"
    created_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    
    # May contain sensitive information
    notes: Optional[str] = None  # TODO(TEAM-SEC): May contain PII
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "payment_id": self.payment_id,
            "order_id": self.order_id,
            "amount": self.amount,
            "reason": self.reason,
            "status": self.status,
            "created_at": self.created_at,
            "processed_at": self.processed_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RefundV1":
        """Create instance from dictionary."""
        return cls(
            id=data["id"],
            payment_id=data["payment_id"],
            order_id=data["order_id"],
            amount=float(data.get("amount", 0)),
            reason=data.get("reason"),
            status=data.get("status", "pending"),
            created_at=data.get("created_at"),
            processed_at=data.get("processed_at"),
        )
