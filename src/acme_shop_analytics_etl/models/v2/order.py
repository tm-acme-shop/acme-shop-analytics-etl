"""
V2 Order Models

Modern order models with Decimal for currency and proper normalization.
"""
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
from enum import Enum


class OrderStatus(str, Enum):
    """Order status."""
    PENDING = "pending"
    CONFIRMED = "confirmed"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"


class Currency(str, Enum):
    """Supported currencies."""
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    CAD = "CAD"
    AUD = "AUD"


@dataclass
class Order:
    """
    V2 Order model with proper currency handling.
    
    Uses Decimal for monetary values and tokens for addresses.
    
    Attributes:
        id: Order ID.
        user_id: Associated user ID.
        user_token: User's external token.
        subtotal: Order subtotal (Decimal).
        tax_amount: Tax amount (Decimal).
        shipping_amount: Shipping cost (Decimal).
        discount_amount: Applied discounts (Decimal).
        total_amount: Order total (Decimal).
        currency: Order currency.
        status: Order status.
    """
    
    id: int
    user_id: int
    user_token: str
    order_number: str
    
    # Monetary values as Decimal
    subtotal: Decimal = Decimal("0.00")
    tax_amount: Decimal = Decimal("0.00")
    shipping_amount: Decimal = Decimal("0.00")
    discount_amount: Decimal = Decimal("0.00")
    total_amount: Decimal = Decimal("0.00")
    currency: Currency = Currency.USD
    
    status: OrderStatus = OrderStatus.PENDING
    item_count: int = 0
    
    # Tokenized addresses
    shipping_address_token: Optional[str] = None
    billing_address_token: Optional[str] = None
    
    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    shipped_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    
    # Metadata
    source: Optional[str] = None  # web, mobile, api
    coupon_code: Optional[str] = None
    notes: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "user_token": self.user_token,
            "order_number": self.order_number,
            "subtotal": str(self.subtotal),
            "tax_amount": str(self.tax_amount),
            "shipping_amount": str(self.shipping_amount),
            "discount_amount": str(self.discount_amount),
            "total_amount": str(self.total_amount),
            "currency": self.currency.value if isinstance(self.currency, Currency) else self.currency,
            "status": self.status.value if isinstance(self.status, OrderStatus) else self.status,
            "item_count": self.item_count,
            "shipping_address_token": self.shipping_address_token,
            "billing_address_token": self.billing_address_token,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "confirmed_at": self.confirmed_at,
            "shipped_at": self.shipped_at,
            "delivered_at": self.delivered_at,
            "source": self.source,
            "coupon_code": self.coupon_code,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Order":
        """Create instance from dictionary."""
        status = data.get("status", "pending")
        if isinstance(status, str):
            status = OrderStatus(status)
        
        currency = data.get("currency", "USD")
        if isinstance(currency, str):
            currency = Currency(currency)
        
        return cls(
            id=data["id"],
            user_id=data["user_id"],
            user_token=data["user_token"],
            order_number=data["order_number"],
            subtotal=Decimal(str(data.get("subtotal", "0.00"))),
            tax_amount=Decimal(str(data.get("tax_amount", "0.00"))),
            shipping_amount=Decimal(str(data.get("shipping_amount", "0.00"))),
            discount_amount=Decimal(str(data.get("discount_amount", "0.00"))),
            total_amount=Decimal(str(data.get("total_amount", "0.00"))),
            currency=currency,
            status=status,
            item_count=data.get("item_count", 0),
            shipping_address_token=data.get("shipping_address_token"),
            billing_address_token=data.get("billing_address_token"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            confirmed_at=data.get("confirmed_at"),
            shipped_at=data.get("shipped_at"),
            delivered_at=data.get("delivered_at"),
            source=data.get("source"),
            coupon_code=data.get("coupon_code"),
        )
    
    def is_completed(self) -> bool:
        """Check if order is completed."""
        return self.status == OrderStatus.DELIVERED
    
    def is_active(self) -> bool:
        """Check if order is in progress."""
        return self.status in (
            OrderStatus.PENDING,
            OrderStatus.CONFIRMED,
            OrderStatus.PROCESSING,
            OrderStatus.SHIPPED,
        )


@dataclass
class OrderItem:
    """
    V2 Order item model.
    
    Properly normalized with product references.
    """
    
    id: int
    order_id: int
    product_id: int
    product_variant_id: Optional[int] = None
    
    # Pricing as Decimal
    quantity: int = 1
    unit_price: Decimal = Decimal("0.00")
    discount_amount: Decimal = Decimal("0.00")
    tax_amount: Decimal = Decimal("0.00")
    total_price: Decimal = Decimal("0.00")
    
    # Product snapshot (for historical accuracy)
    product_name: Optional[str] = None
    product_sku: Optional[str] = None
    
    # Metadata
    is_gift: bool = False
    gift_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "order_id": self.order_id,
            "product_id": self.product_id,
            "product_variant_id": self.product_variant_id,
            "quantity": self.quantity,
            "unit_price": str(self.unit_price),
            "discount_amount": str(self.discount_amount),
            "tax_amount": str(self.tax_amount),
            "total_price": str(self.total_price),
            "product_name": self.product_name,
            "product_sku": self.product_sku,
            "is_gift": self.is_gift,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderItem":
        """Create instance from dictionary."""
        return cls(
            id=data["id"],
            order_id=data["order_id"],
            product_id=data["product_id"],
            product_variant_id=data.get("product_variant_id"),
            quantity=data.get("quantity", 1),
            unit_price=Decimal(str(data.get("unit_price", "0.00"))),
            discount_amount=Decimal(str(data.get("discount_amount", "0.00"))),
            tax_amount=Decimal(str(data.get("tax_amount", "0.00"))),
            total_price=Decimal(str(data.get("total_price", "0.00"))),
            product_name=data.get("product_name"),
            product_sku=data.get("product_sku"),
            is_gift=data.get("is_gift", False),
            gift_message=data.get("gift_message"),
        )
