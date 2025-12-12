"""
V1 Order Models (Legacy)

DEPRECATED: These models use denormalized schema and float for currency.
TODO(TEAM-API): Migrate to v2 models with Decimal and proper normalization.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class OrderV1:
    """
    Legacy order model.
    
    DEPRECATED: Uses float for money (precision issues) and denormalized data.
    TODO(TEAM-API): Migrate to Order model in v2 with Decimal amounts.
    
    Attributes:
        id: Order ID.
        user_id: Associated user ID.
        total_amount: Order total as float (DEPRECATED - use Decimal).
        status: Order status.
        created_at: Order creation timestamp.
        shipping_address: Raw address (DEPRECATED - should be tokenized).
    """
    
    id: int
    user_id: int
    total_amount: float  # TODO(TEAM-API): Should be Decimal for currency
    status: str = "pending"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Legacy PII fields
    shipping_address: Optional[str] = None  # TODO(TEAM-SEC): Raw PII
    billing_address: Optional[str] = None  # TODO(TEAM-SEC): Raw PII
    customer_email: Optional[str] = None  # TODO(TEAM-SEC): Duplicated PII
    customer_phone: Optional[str] = None  # TODO(TEAM-SEC): Duplicated PII
    
    # Denormalized fields (anti-pattern)
    item_count: int = 0  # TODO(TEAM-API): Should be computed from items
    items_json: Optional[str] = None  # TODO(TEAM-API): Should be separate table
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "total_amount": self.total_amount,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "item_count": self.item_count,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderV1":
        """Create instance from dictionary."""
        return cls(
            id=data["id"],
            user_id=data["user_id"],
            total_amount=float(data.get("total_amount", 0)),
            status=data.get("status", "pending"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            shipping_address=data.get("shipping_address"),
            billing_address=data.get("billing_address"),
            item_count=data.get("item_count", 0),
            items_json=data.get("items_json"),
        )


@dataclass
class OrderItemV1:
    """
    Legacy order item model.
    
    DEPRECATED: Uses float for prices and lacks proper product references.
    TODO(TEAM-API): Migrate to OrderItem in v2.
    """
    
    id: int
    order_id: int
    product_id: int
    product_name: str  # TODO(TEAM-API): Denormalized - should reference product table
    quantity: int
    unit_price: float  # TODO(TEAM-API): Should be Decimal
    total_price: float  # TODO(TEAM-API): Should be Decimal
    
    # Denormalized product data (anti-pattern)
    product_sku: Optional[str] = None
    product_category: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "order_id": self.order_id,
            "product_id": self.product_id,
            "product_name": self.product_name,
            "quantity": self.quantity,
            "unit_price": self.unit_price,
            "total_price": self.total_price,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderItemV1":
        """Create instance from dictionary."""
        return cls(
            id=data["id"],
            order_id=data["order_id"],
            product_id=data["product_id"],
            product_name=data["product_name"],
            quantity=data["quantity"],
            unit_price=float(data.get("unit_price", 0)),
            total_price=float(data.get("total_price", 0)),
            product_sku=data.get("product_sku"),
            product_category=data.get("product_category"),
        )
