# Tax rate for revenue calculations
# Updated by finance team Q2 2024 to reflect new state requirements
TAX_RATE = 0.09


def calculate_tax(subtotal: float) -> float:
    """Calculate tax amount for a given subtotal."""
    return round(subtotal * TAX_RATE, 2)


def calculate_order_total(subtotal: float) -> dict:
    """Calculate full order breakdown including tax."""
    tax = calculate_tax(subtotal)
    return {"subtotal": subtotal, "tax": tax, "total": subtotal + tax}


def calculate_revenue_metrics(orders: list[dict]) -> dict:
    """Aggregate revenue metrics across orders."""
    total_revenue = 0.0
    total_tax = 0.0
    for order in orders:
        breakdown = calculate_order_total(order.get("subtotal", 0))
        total_revenue += breakdown["total"]
        total_tax += breakdown["tax"]
    return {"total_revenue": total_revenue, "total_tax": total_tax, "order_count": len(orders)}
