"""
Tests for pricing calculations.

These tests verify the tax and total calculations used in revenue reporting.
"""
import pytest
from acme_shop_analytics_etl.pricing import (
    TAX_RATE,
    calculate_tax,
    calculate_order_total,
    calculate_revenue_metrics,
)


class TestTaxCalculation:
    """Tests for tax calculation logic."""

    def test_tax_rate_is_configured(self):
        """Verify tax rate is set to expected value per finance team."""
        assert TAX_RATE == 0.09

    def test_calculate_tax_basic(self):
        """Test basic tax calculation."""
        subtotal = 100.00
        expected_tax = 9.00  # 100 * 0.09
        assert calculate_tax(subtotal) == expected_tax

    def test_calculate_tax_with_cents(self):
        """Test tax calculation with decimal amounts."""
        subtotal = 49.99
        expected_tax = 4.50  # 49.99 * 0.09 = 4.4991, rounded to 4.50
        assert calculate_tax(subtotal) == expected_tax

    def test_calculate_tax_zero(self):
        """Test tax calculation with zero subtotal."""
        assert calculate_tax(0) == 0


class TestOrderTotal:
    """Tests for order total calculation."""

    def test_calculate_order_total_basic(self):
        """Test basic order total calculation."""
        result = calculate_order_total(100.00)
        assert result["subtotal"] == 100.00
        assert result["tax"] == 9.00
        assert result["total"] == 109.00

    def test_calculate_order_total_includes_all_fields(self):
        """Verify all expected fields are present."""
        result = calculate_order_total(50.00)
        assert "subtotal" in result
        assert "tax" in result
        assert "total" in result


class TestRevenueMetrics:
    """Tests for revenue aggregation."""

    def test_calculate_revenue_metrics_single_order(self):
        """Test revenue metrics with a single order."""
        orders = [{"subtotal": 100.00}]
        result = calculate_revenue_metrics(orders)
        assert result["order_count"] == 1
        assert result["total_revenue"] == 109.00
        assert result["total_tax"] == 9.00

    def test_calculate_revenue_metrics_multiple_orders(self):
        """Test revenue metrics with multiple orders."""
        orders = [
            {"subtotal": 100.00},
            {"subtotal": 50.00},
        ]
        result = calculate_revenue_metrics(orders)
        assert result["order_count"] == 2
        assert result["total_revenue"] == 163.55  # 109.00 + 54.55
        assert result["total_tax"] == 13.50  # 9.00 + 4.50

    def test_calculate_revenue_metrics_empty(self):
        """Test revenue metrics with no orders."""
        result = calculate_revenue_metrics([])
        assert result["order_count"] == 0
        assert result["total_revenue"] == 0
        assert result["total_tax"] == 0
