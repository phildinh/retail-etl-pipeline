# tests/integration/test_pipeline_integration.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Integration tests for the full pipeline
#
# USES: retail_etl_test database (NOT dev or prod)
#
# WHY a separate test database?
#   Integration tests INSERT and TRUNCATE real tables
#   Running against dev → wipes your real data ❌
#   Running against test → safe, isolated, repeatable ✅
#
# HOW TO RUN:
#   Set ENV=test before running:
#   Windows: $env:ENV="test"; pytest tests/integration/ -v
# ═══════════════════════════════════════════════════════════

import pytest
import json
import os
from datetime import datetime, timezone
from sqlalchemy import text


# ─────────────────────────────────────────────────────────
# SKIP GUARD
#
# WHY skip if ENV is not test?
# Integration tests touch a real database
# If someone runs pytest without setting ENV=test:
# → tests would run against dev database
# → truncate real data ❌
#
# Skip guard protects against this:
# ENV=dev  → skip all integration tests safely ✅
# ENV=test → run integration tests ✅
# ─────────────────────────────────────────────────────────
if os.getenv("ENV", "dev") != "test":
    pytest.skip(
        "Integration tests only run with ENV=test",
        allow_module_level=True
    )


from etl.utils.db import get_db_connection
from etl.transform.products_transform import run_products_transform
from etl.transform.carts_transform import run_carts_transform
from etl.transform.users_transform import run_users_transform


# ─────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clean_test_tables():
    """
    Clean up test tables before AND after every test.

    WHY autouse=True?
    autouse means this fixture runs automatically
    for every test in this file
    No need to add it as a parameter to each test ✅

    WHY clean before AND after?
    Before: ensures no leftover data from previous test run
    After:  leaves database clean for next test run

    This is called a TEARDOWN pattern:
    Setup   → clean tables before test
    Test    → run the test
    Teardown → clean tables after test
    """
    # Setup — clean before test
    with get_db_connection() as conn:
        conn.execute(text("TRUNCATE TABLE staging.products"))
        conn.execute(text("TRUNCATE TABLE staging.carts"))
        conn.execute(text("TRUNCATE TABLE staging.users"))
        conn.execute(text("TRUNCATE TABLE raw.products"))
        conn.execute(text("TRUNCATE TABLE raw.carts"))
        conn.execute(text("TRUNCATE TABLE raw.users"))

    yield  # test runs here

    # Teardown — clean after test
    with get_db_connection() as conn:
        conn.execute(text("TRUNCATE TABLE staging.products"))
        conn.execute(text("TRUNCATE TABLE staging.carts"))
        conn.execute(text("TRUNCATE TABLE staging.users"))
        conn.execute(text("TRUNCATE TABLE raw.products"))
        conn.execute(text("TRUNCATE TABLE raw.carts"))
        conn.execute(text("TRUNCATE TABLE raw.users"))


def insert_raw_products(conn, products: list):
    """Helper to insert fake products into raw.products."""
    for product in products:
        conn.execute(
            text("""
                INSERT INTO raw.products (raw_data, loaded_at)
                VALUES (:raw_data, :loaded_at)
            """),
            {
                "raw_data":  json.dumps(product),
                "loaded_at": datetime.now(timezone.utc)
            }
        )


def insert_raw_carts(conn, carts: list):
    """Helper to insert fake carts into raw.carts."""
    for cart in carts:
        conn.execute(
            text("""
                INSERT INTO raw.carts (raw_data, loaded_at)
                VALUES (:raw_data, :loaded_at)
            """),
            {
                "raw_data":  json.dumps(cart),
                "loaded_at": datetime.now(timezone.utc)
            }
        )


def insert_raw_users(conn, users: list):
    """Helper to insert fake users into raw.users."""
    for user in users:
        conn.execute(
            text("""
                INSERT INTO raw.users (raw_data, loaded_at)
                VALUES (:raw_data, :loaded_at)
            """),
            {
                "raw_data":  json.dumps(user),
                "loaded_at": datetime.now(timezone.utc)
            }
        )


# ─────────────────────────────────────────────────────────
# INTEGRATION TESTS
# ─────────────────────────────────────────────────────────

def test_products_transform_reads_raw_writes_staging():
    """
    Full cycle test:
    Insert raw → run transform → check staging

    Proves the entire read/transform/write
    cycle works end to end with a real database.
    """
    # Arrange — insert fake raw products
    fake_products = [
        {
            "id": 1, "title": "Backpack",
            "price": 109.95, "category": "men's clothing",
            "description": "A bag", "image": "https://img.url",
            "rating": {"rate": 3.9, "count": 120}
        },
        {
            "id": 2, "title": "T-Shirt",
            "price": 22.30, "category": "men's clothing",
            "description": "A shirt", "image": "https://img2.url",
            "rating": {"rate": 4.1, "count": 259}
        }
    ]

    with get_db_connection() as conn:
        insert_raw_products(conn, fake_products)

    # Act — run transform
    count = run_products_transform()

    # Assert — check staging has correct data
    assert count == 2

    with get_db_connection() as conn:
        result = conn.execute(
            text("""
                SELECT
                    source_id, title, price,
                    rating_rate, rating_count
                FROM staging.products
                ORDER BY source_id
            """)
        )
        rows = result.fetchall()

    assert len(rows) == 2
    assert rows[0].source_id   == 1
    assert rows[0].title       == "Backpack"
    assert float(rows[0].price) == 109.95
    assert float(rows[0].rating_rate)  == 3.9
    assert rows[0].rating_count == 120


def test_carts_transform_explodes_correctly():
    """
    Full cycle test for cart explode:
    1 cart with 3 products → 3 staging rows
    """
    # Arrange
    fake_carts = [
        {
            "id": 1, "userId": 1,
            "date": "2020-03-02T00:00:00.000Z",
            "products": [
                {"productId": 1, "quantity": 4},
                {"productId": 2, "quantity": 1},
                {"productId": 3, "quantity": 6},
            ]
        }
    ]

    with get_db_connection() as conn:
        insert_raw_carts(conn, fake_carts)

    # Act
    count = run_carts_transform()

    # Assert — 1 cart with 3 products = 3 rows
    assert count == 3

    with get_db_connection() as conn:
        result = conn.execute(
            text("""
                SELECT
                    cart_source_id,
                    product_source_id,
                    quantity
                FROM staging.carts
                ORDER BY product_source_id
            """)
        )
        rows = result.fetchall()

    assert len(rows) == 3

    # All rows belong to same cart
    assert all(r.cart_source_id == 1 for r in rows)

    # Correct product ids and quantities
    assert rows[0].product_source_id == 1
    assert rows[0].quantity          == 4
    assert rows[1].product_source_id == 2
    assert rows[1].quantity          == 1
    assert rows[2].product_source_id == 3
    assert rows[2].quantity          == 6


def test_users_transform_excludes_password():
    """
    Full cycle test:
    Raw user with password → staging must not have password

    Most important integration test:
    Proves password never reaches the database
    even after the full read/write cycle ✅
    """
    # Arrange
    fake_users = [
        {
            "id": 1,
            "email":    "john@gmail.com",
            "username": "johnd",
            "password": "supersecret123",
            "name": {
                "firstname": "John",
                "lastname":  "Doe"
            },
            "address": {
                "street":  "7835 new road",
                "city":    "Ann Arbor",
                "zipcode": "48100",
                "geolocation": {
                    "lat":  "-37.3159",
                    "long": "81.1496"
                }
            },
            "phone": "1-570-236-7033"
        }
    ]

    with get_db_connection() as conn:
        insert_raw_users(conn, fake_users)

    # Act
    count = run_users_transform()

    # Assert
    assert count == 1

    with get_db_connection() as conn:
        # Check staging.users columns
        result = conn.execute(
            text("""
                SELECT *
                FROM staging.users
                WHERE source_id = 1
            """)
        )
        row = result.fetchone()

    # User was saved
    assert row is not None

    # Core fields correct
    assert row.email      == "john@gmail.com"
    assert row.first_name == "John"
    assert row.last_name  == "Doe"
    assert row.address_city == "Ann Arbor"

    # Password column does not exist in staging
    # If it did → this would raise AttributeError
    columns = row._fields
    assert "password" not in columns


def test_products_transform_truncates_on_rerun():
    """
    Running transform twice must not duplicate rows.

    WHY test this?
    Truncate + reload pattern must work correctly:
    Run 1 → 2 products → staging has 2 rows
    Run 2 → 2 products → staging still has 2 rows (not 4)
    """
    fake_products = [
        {
            "id": 1, "title": "Backpack",
            "price": 109.95, "category": "men's clothing",
            "description": "A bag", "image": "https://img.url",
            "rating": {"rate": 3.9, "count": 120}
        }
    ]

    with get_db_connection() as conn:
        insert_raw_products(conn, fake_products)

    # Run transform TWICE
    run_products_transform()
    run_products_transform()

    # Staging must still have only 1 row not 2
    with get_db_connection() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM staging.products")
        )
        count = result.scalar()

    assert count == 1