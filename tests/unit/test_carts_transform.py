# tests/unit/test_carts_transform.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Unit tests for carts transform logic
#
# MOST IMPORTANT TESTS IN THE PIPELINE:
# The explode logic is the most complex transform we wrote
# One cart → multiple rows
# If this breaks → fact_orders has wrong data
# Tests catch this immediately ✅
# ═══════════════════════════════════════════════════════════

import pytest
from datetime import date
from etl.transform.carts_transform import (
    transform_cart,
    parse_cart_date,
)


# ─────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────

@pytest.fixture
def fake_cart_one_product():
    """Cart with exactly one product."""
    return {
        "id":     1,
        "userId": 2,
        "date":   "2020-03-02T00:00:00.000Z",
        "products": [
            {"productId": 5, "quantity": 3}
        ]
    }


@pytest.fixture
def fake_cart_three_products():
    """
    Cart with three products.
    Core explode test — 1 cart must become 3 rows.
    """
    return {
        "id":     2,
        "userId": 1,
        "date":   "2020-01-15T00:00:00.000Z",
        "products": [
            {"productId": 1, "quantity": 4},
            {"productId": 2, "quantity": 1},
            {"productId": 3, "quantity": 6},
        ]
    }


@pytest.fixture
def fake_cart_no_products():
    """
    Cart with empty products list.
    Edge case — should return empty list, not crash.
    """
    return {
        "id":     3,
        "userId": 1,
        "date":   "2020-05-10T00:00:00.000Z",
        "products": []
    }


# ─────────────────────────────────────────────────────────
# DATE PARSING TESTS
# ─────────────────────────────────────────────────────────

def test_parse_cart_date_returns_date_object():
    """
    parse_cart_date must return a date object not a string.
    PostgreSQL DATE column needs Python date, not string.
    """
    result = parse_cart_date("2020-03-02T00:00:00.000Z")
    assert isinstance(result, date)


def test_parse_cart_date_correct_values():
    """
    Date values must be correctly parsed.
    Year, month, day must all match the input string.
    """
    result = parse_cart_date("2020-03-02T00:00:00.000Z")
    assert result.year  == 2020
    assert result.month == 3
    assert result.day   == 2


def test_parse_cart_date_strips_time_component():
    """
    Time component T00:00:00.000Z must be stripped.
    We store DATE only — no time precision in this data.
    """
    result = parse_cart_date("2020-01-15T00:00:00.000Z")
    assert result == date(2020, 1, 15)


# ─────────────────────────────────────────────────────────
# EXPLODE LOGIC TESTS — most important tests here
# ─────────────────────────────────────────────────────────

def test_transform_cart_one_product_returns_one_row(
    fake_cart_one_product
):
    """
    1 cart with 1 product → must return list of 1 row.
    Basic explode sanity check.
    """
    result = transform_cart(fake_cart_one_product)
    assert len(result) == 1


def test_transform_cart_three_products_returns_three_rows(
    fake_cart_three_products
):
    """
    1 cart with 3 products → must return list of 3 rows.
    This is the core explode test.

    WHY is this so important?
    If explode is broken:
      3 products → 1 row  (data loss) ❌
      3 products → 9 rows (duplicates) ❌
    Must be exactly 3 rows. ✅
    """
    result = transform_cart(fake_cart_three_products)
    assert len(result) == 3


def test_transform_cart_empty_products_returns_empty_list(
    fake_cart_no_products
):
    """
    Cart with no products → must return empty list.
    Should not crash. Should not return None.
    """
    result = transform_cart(fake_cart_no_products)
    assert result == []


def test_transform_cart_row_has_correct_keys(
    fake_cart_one_product
):
    """
    Each exploded row must have exactly the right keys.
    Missing keys → INSERT fails.
    """
    result  = transform_cart(fake_cart_one_product)
    row     = result[0]

    expected_keys = {
        "cart_source_id",
        "user_source_id",
        "product_source_id",
        "quantity",
        "cart_date",
    }

    assert set(row.keys()) == expected_keys


def test_transform_cart_preserves_cart_id_in_all_rows(
    fake_cart_three_products
):
    """
    All exploded rows must carry the same cart_source_id.

    WHY test this?
    After exploding, we need to be able to
    GROUP BY cart_source_id to reconstruct the cart
    If cart_id is wrong → broken traceability ❌
    """
    result = transform_cart(fake_cart_three_products)

    for row in result:
        assert row["cart_source_id"] == 2


def test_transform_cart_preserves_user_id_in_all_rows(
    fake_cart_three_products
):
    """All exploded rows must carry the same user_source_id."""
    result = transform_cart(fake_cart_three_products)

    for row in result:
        assert row["user_source_id"] == 1


def test_transform_cart_correct_product_ids(
    fake_cart_three_products
):
    """
    Each row must have the correct product_source_id.
    Products must not be mixed up during explode.
    """
    result      = transform_cart(fake_cart_three_products)
    product_ids = [row["product_source_id"] for row in result]

    assert product_ids == [1, 2, 3]


def test_transform_cart_correct_quantities(
    fake_cart_three_products
):
    """
    Quantity must match the correct product.
    Product 1 → quantity 4, not product 2's quantity.
    """
    result     = transform_cart(fake_cart_three_products)
    quantities = [row["quantity"] for row in result]

    assert quantities == [4, 1, 6]


def test_transform_cart_date_is_date_object(
    fake_cart_one_product
):
    """
    cart_date in output must be a date object.
    Not a string — PostgreSQL DATE needs Python date.
    """
    result = transform_cart(fake_cart_one_product)
    assert isinstance(result[0]["cart_date"], date)


def test_transform_cart_correct_date_value(
    fake_cart_one_product
):
    """cart_date must match the cart's date field."""
    result = transform_cart(fake_cart_one_product)
    assert result[0]["cart_date"] == date(2020, 3, 2)