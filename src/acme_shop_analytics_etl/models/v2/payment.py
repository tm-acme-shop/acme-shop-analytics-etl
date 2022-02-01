"""
V2 Payment Models

Modern payment models with tokenized card data and Decimal for currency.
PCI-DSS compliant - no raw card data stored.
"""
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional
from enum import Enum


class PaymentStatus(str, Enum):
    """Payment status."""
    PENDING = "pending"
    AUTHORIZED = "authorized"
    CAPTURED = "captured"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"
    PARTIALLY_REFUNDED = "partially_refunded"


class PaymentMethod(str, Enum):
    """Payment method types."""
    CARD = "card"
    BANK_TRANSFER = "bank_transfer"
    PAYPAL = "paypal"
    APPLE_PAY = "apple_pay"
    GOOGLE_PAY = "google_pay"
    CRYPTO = "crypto"


class CardBrand(str, Enum):
    """Card brand types."""
    VISA = "visa"
    MASTERCARD = "mastercard"
    AMEX = "amex"
    DISCOVER = "discover"
    UNKNOWN = "unknown"


@dataclass
class Payment:
    """
    V2 Payment model with tokenized data.
    
    PCI-DSS compliant - stores only tokens, not raw card data.
    
    Attributes:
        id: Payment ID.
        order_id: Associated order ID.
        user_id: Associated user ID.
        amount: Payment amount (Decimal).
        currency: Payment currency.
        status: Payment status.
        payment_method: Type of payment.
        card_token: Tokenized card reference (from payment provider).
        card_last_four: Last 4 digits for display.
        card_brand: Card brand for display.
    """
    
    id: int
    order_id: int
    user_id: int
    user_token: str
    
    # Monetary values as Decimal
    amount: Decimal = Decimal("0.00")
    currency: str = "USD"
    
    status: PaymentStatus = PaymentStatus.PENDING
    payment_method: PaymentMethod = PaymentMethod.CARD
    
    # Tokenized card data (PCI compliant)
    card_token: Optional[str] = None
    card_last_four: Optional[str] = None
    card_brand: Optional[CardBrand] = None
    card_exp_month: Optional[int] = None
    card_exp_year: Optional[int] = None
    
    # Tokenized billing info
    billing_address_token: Optional[str] = None
    cardholder_token: Optional[str] = None
    
    # Provider details
    provider: Optional[str] = None  # stripe, braintree, etc.
    provider_transaction_id: Optional[str] = None
    provider_response_code: Optional[str] = None
    
    # Timestamps
    created_at: Optional[datetime] = None
    authorized_at: Optional[datetime] = None
    captured_at: Optional[datetime] = None
    
    # Performance metrics
    processing_time_ms: Optional[int] = None
    
    # Error handling
    failure_reason: Optional[str] = None
    failure_code: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations."""
        return {
            "id": self.id,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "user_token": self.user_token,
            "amount": str(self.amount),
            "currency": self.currency,
            "status": self.status.value if isinstance(self.status, PaymentStatus) else self.status,
            "payment_method": self.payment_method.value if isinstance(self.payment_method, PaymentMethod) else self.payment_method,
            "card_token": self.card_token,
            "card_last_four": self.card_last_four,
            "card_brand": self.card_brand.value if isinstance(self.card_brand, CardBrand) else self.card_brand,
            "provider": self.provider,
            "provider_transaction_id": self.provider_transaction_id,
            "created_at": self.created_at,
            "authorized_at": self.authorized_at,
            "captured_at": self.captured_at,
            "processing_time_ms": self.processing_time_ms,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Payment":
        """Create instance from dictionary."""
        status = data.get("status", "pending")
        if isinstance(status, str):
            status = PaymentStatus(status)
        
        method = data.get("payment_method", "card")
        if isinstance(method, str):
            method = PaymentMethod(method)
        
        brand = data.get("card_brand")
        if isinstance(brand, str):
            brand = CardBrand(brand)
        
        return cls(
            id=data["id"],
            order_id=data["order_id"],
            user_id=data["user_id"],
            user_token=data["user_token"],
            amount=Decimal(str(data.get("amount", "0.00"))),
            currency=data.get("currency", "USD"),
            status=status,
            payment_method=method,
            card_token=data.get("card_token"),
            card_last_four=data.get("card_last_four"),
            card_brand=brand,
            card_exp_month=data.get("card_exp_month"),
            card_exp_year=data.get("card_exp_year"),
            billing_address_token=data.get("billing_address_token"),
            cardholder_token=data.get("cardholder_token"),
            provider=data.get("provider"),
            provider_transaction_id=data.get("provider_transaction_id"),
            provider_response_code=data.get("provider_response_code"),
            created_at=data.get("created_at"),
            authorized_at=data.get("authorized_at"),
            captured_at=data.get("captured_at"),
            processing_time_ms=data.get("processing_time_ms"),
            failure_reason=data.get("failure_reason"),
            failure_code=data.get("failure_code"),
        )
    
    def is_successful(self) -> bool:
        """Check if payment was successful."""
        return self.status in (PaymentStatus.SUCCESS, PaymentStatus.CAPTURED)
    
    def is_refundable(self) -> bool:
        """Check if payment can be refunded."""
        return self.status in (PaymentStatus.SUCCESS, PaymentStatus.CAPTURED)


@dataclass
class Refund:
    """
    V2 Refund model.
    
    Tracks refunds with proper decimal handling.
    """
    
    id: int
    payment_id: int
    order_id: int
    
    # Monetary values as Decimal
    amount: Decimal = Decimal("0.00")
    currency: str = "USD"
    
    reason: Optional[str] = None
    status: str = "pending"
    
    # Provider details
    provider_refund_id: Optional[str] = None
    
    # Timestamps
    created_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    
    # Metadata
    initiated_by: Optional[str] = None  # customer, admin, system
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "payment_id": self.payment_id,
            "order_id": self.order_id,
            "amount": str(self.amount),
            "currency": self.currency,
            "reason": self.reason,
            "status": self.status,
            "provider_refund_id": self.provider_refund_id,
            "created_at": self.created_at,
            "processed_at": self.processed_at,
            "initiated_by": self.initiated_by,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Refund":
        """Create instance from dictionary."""
        return cls(
            id=data["id"],
            payment_id=data["payment_id"],
            order_id=data["order_id"],
            amount=Decimal(str(data.get("amount", "0.00"))),
            currency=data.get("currency", "USD"),
            reason=data.get("reason"),
            status=data.get("status", "pending"),
            provider_refund_id=data.get("provider_refund_id"),
            created_at=data.get("created_at"),
            processed_at=data.get("processed_at"),
            initiated_by=data.get("initiated_by"),
        )
