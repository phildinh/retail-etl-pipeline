# tests/unit/test_products_transform.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Unit tests for products transform logic
#
# UNIT TEST RULES:
#   No database connections
#   No API calls
#   No file system access
#   Test ONE function at a time with fake data
#
# WHY fake data (fixtures)?
#   Real API data can change anytime
#   Tests must be deterministic:
#   Same input → same output → every single time
#   Fake data we control = reliable tests ✅
# ═══════════════════════════════════════════════════════════

import pytest
from etl.transform.products_transform import transform_product


# ─────────────────────────────────────────────────────────
# FIXTURES
#
# WHY use fixtures instead of repeating fake data?
#
# Without fixtures:
#   def test_one():
#       product = {"id": 1, "title": "Backpack", ...}
#   def test_two():
#       product = {"id": 1, "title": "Backpack", ...}
#   → same fake data copy-pasted everywhere ❌
#   → change one field → update every test ❌
#
# With fixtures:
#   @pytest.fixture
#   def fake_product(): return {"id": 1, ...}
#
#   def test_one(fake_product): ...
#   def test_two(fake_product): ...
#   → defined once, used everywhere ✅
#   → change one field → all tests updated ✅
# ─────────────────────────────────────────────────────────

@pytest.fixture
def fake_product():
    """
    A complete fake product record exactly as API sends it.
    Used as input for all product transform tests.
    """
    return {
        "id":          1,
        "title":       "Fjallraven Backpack",
        "price":       109.95,
        "category":    "men's clothing",
        "description": "Your perfect pack for everyday use",
        "image":       "https://fakestoreapi.com/img/1.jpg",
        "rating": {
            "rate":  3.9,
            "count": 120
        }
    }


@pytest.fixture
def fake_product_no_rating():
    """
    A product with missing rating field.
    Tests defensive coding with .get() default.
    """
    return {
        "id":          2,
        "title":       "No Rating Product",
        "price":       49.99,
        "category":    "electronics",
        "description": "A product with no rating",
        "image":       "https://fakestoreapi.com/img/2.jpg",
        # rating field deliberately missing
    }


# ─────────────────────────────────────────────────────────
# TESTS
#
# Naming convention: test_{what}_{condition}_{expected}
# Makes test failures self-explanatory:
#   FAILED test_transform_product_flattens_rating
#   → immediately know what broke without reading code
# ─────────────────────────────────────────────────────────

def test_transform_product_returns_correct_source_id(fake_product):
    """
    source_id should equal the API's original id field.
    We rename id → source_id to be explicit about origin.
    """
    result = transform_product(fake_product)
    assert result["source_id"] == 1


def test_transform_product_flattens_rating_rate(fake_product):
    """
    rating.rate nested field should become flat rating_rate.
    Core flatten logic — must work correctly.
    """
    result = transform_product(fake_product)
    assert result["rating_rate"] == 3.9


def test_transform_product_flattens_rating_count(fake_product):
    """
    rating.count nested field should become flat rating_count.
    """
    result = transform_product(fake_product)
    assert result["rating_count"] == 120


def test_transform_product_casts_price_to_float(fake_product):
    """
    Price must be a float for NUMERIC(10,2) column.
    API sends float already but we explicitly cast it.
    """
    result = transform_product(fake_product)
    assert isinstance(result["price"], float)
    assert result["price"] == 109.95


def test_transform_product_renames_image_to_image_url(fake_product):
    """
    API field 'image' must be renamed to 'image_url'.
    More descriptive name — unambiguous it is a URL.
    """
    result = transform_product(fake_product)

    # image_url should exist
    assert "image_url" in result

    # original 'image' key should NOT be in output
    assert "image" not in result


def test_transform_product_handles_missing_rating(
    fake_product_no_rating
):
    """
    If rating field is missing entirely from API response,
    transform should not crash.
    rating_rate and rating_count should be None.

    WHY test this?
    APIs change without warning.
    Defensive .get("rating", {}) must handle missing field.
    Pipeline should continue, not crash. ✅
    """
    result = transform_product(fake_product_no_rating)

    assert result["rating_rate"]  is None
    assert result["rating_count"] is None


def test_transform_product_output_has_correct_keys(fake_product):
    """
    Output dict must have exactly the right keys.
    No extra keys, no missing keys.

    WHY test this?
    If a key is missing → INSERT will fail with missing column
    If extra keys exist → INSERT may fail with unknown column
    """
    result = transform_product(fake_product)

    expected_keys = {
        "source_id",
        "title",
        "price",
        "category",
        "description",
        "image_url",
        "rating_rate",
        "rating_count",
    }

    assert set(result.keys()) == expected_keys


def test_transform_product_preserves_title(fake_product):
    """Title should pass through unchanged."""
    result = transform_product(fake_product)
    assert result["title"] == "Fjallraven Backpack"


def test_transform_product_preserves_category(fake_product):
    """Category should pass through unchanged."""
    result = transform_product(fake_product)
    assert result["category"] == "men's clothing"