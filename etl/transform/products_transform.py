# etl/transform/products_transform.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Transform raw products into clean staging rows
#
# READS FROM:  raw.products     (JSONB, one row per product)
# WRITES TO:   staging.products (flat, typed, clean)
#
# WHAT THIS TRANSFORM DOES:
#   1. Read raw JSONB from raw.products
#   2. Parse JSON string into Python dict
#   3. Flatten rating dict into two separate fields
#   4. Cast price to float (becomes NUMERIC in DB)
#   5. Truncate staging.products (wipe previous run)
#   6. Insert clean rows into staging.products
#
# WHY TRUNCATE BEFORE INSERT?
#   Staging is a temporary workspace, not a history store
#   Every pipeline run starts fresh:
#   Truncate → reload from raw → warehouse reads staging
#   This prevents duplicate rows building up over time
#   History is preserved in warehouse layer, not staging ✅
# ═══════════════════════════════════════════════════════════

import json
from datetime import datetime, timezone
from sqlalchemy import text
from etl.utils.db import get_db_connection
from etl.utils.logger import get_logger

logger = get_logger(__name__)


def transform_product(raw_record: dict) -> dict:
    """
    Transform one raw product record into a clean staging row.

    Args:
        raw_record: dict parsed from raw.products.raw_data
                    exactly as API sent it

    Returns:
        Clean dict ready to insert into staging.products

    WHY a separate function per record?
    Single responsibility:
        transform_product()  → transforms ONE record (pure logic)
        run_products_transform() → orchestrates read/write

    Benefits:
    → Easy to unit test with a single dict input ✅
    → No database needed to test the logic ✅
    → Logic is isolated, easy to change ✅

    Example input:
    {
        "id": 1,
        "title": "Backpack",
        "price": 109.95,
        "category": "men's clothing",
        "description": "Your perfect pack...",
        "image": "https://...",
        "rating": {"rate": 3.9, "count": 120}
    }

    Example output:
    {
        "source_id": 1,
        "title": "Backpack",
        "price": 109.95,
        "category": "men's clothing",
        "description": "Your perfect pack...",
        "image_url": "https://...",
        "rating_rate": 3.9,
        "rating_count": 120
    }
    """
    # ── Extract rating nested dict ────────────────────────
    # raw_record["rating"] = {"rate": 3.9, "count": 120}
    # .get() used instead of [] for safety:
    #   raw_record["rating"]        → KeyError if missing ❌
    #   raw_record.get("rating", {}) → empty dict if missing ✅
    rating = raw_record.get("rating", {})

    return {
        # source_id = API's original id
        # We rename it to be explicit:
        # "this id came from the source system"
        "source_id":    raw_record["id"],

        "title":        raw_record["title"],

        # price stays as float here
        # PostgreSQL NUMERIC(10,2) handles precision on insert
        "price":        float(raw_record["price"]),

        "category":     raw_record["category"],

        # description can be long text, allow None
        "description":  raw_record.get("description"),

        # renamed from "image" to "image_url" for clarity
        # "image" is ambiguous — is it binary? a filename? a URL?
        # "image_url" is unambiguous ✅
        "image_url":    raw_record.get("image"),

        # Flatten rating dict into two separate columns
        # rating.rate  → rating_rate
        # rating.count → rating_count
        "rating_rate":  rating.get("rate"),
        "rating_count": rating.get("count"),
    }


def run_products_transform() -> int:
    """
    Read all raw products, transform, write to staging.

    Returns:
        Number of records written to staging.products

    STEPS:
    1. Read all rows from raw.products
    2. Parse raw_data JSONB → Python dict
    3. Transform each record
    4. Truncate staging.products
    5. Insert all transformed records
    """
    logger.info("Starting products transform")

    with get_db_connection() as conn:

        # ── Step 1: Read from raw ─────────────────────────
        # WHY SELECT raw_data FROM raw.products?
        # raw_data is the JSONB column holding the API response
        # We ignore the raw.products.id (our surrogate)
        # We only want the original API data
        result = conn.execute(
            text("SELECT raw_data FROM raw.products")
        )
        raw_rows = result.fetchall()
        logger.info(f"Read {len(raw_rows)} rows from raw.products")

        # ── Step 2: Transform each record ────────────────
        transformed = []
        for row in raw_rows:
            # row[0] = raw_data column (comes back as dict
            # from SQLAlchemy when column is JSONB)
            raw_record = row[0]
            clean = transform_product(raw_record)
            transformed.append(clean)

        # ── Step 3: Truncate staging ──────────────────────
        # WHY TRUNCATE not DELETE?
        # DELETE FROM staging.products → removes rows one by one
        #                                slow on large tables ❌
        # TRUNCATE staging.products    → removes all rows instantly
        #                                much faster ✅
        # Both achieve the same result, TRUNCATE is faster
        #
        # WHY truncate AFTER reading raw but BEFORE inserting?
        # If we truncate first and then insert fails:
        # → staging is empty, pipeline is broken ❌
        #
        # Better pattern:
        # Read raw → transform in memory → truncate → insert
        # If insert fails → staging is empty but raw is intact
        # → re-run pipeline, raw replays into staging ✅
        conn.execute(text("TRUNCATE TABLE staging.products"))
        logger.info("Truncated staging.products")

        # ── Step 4: Insert transformed records ───────────
        for record in transformed:
            conn.execute(
                text("""
                    INSERT INTO staging.products (
                        source_id,
                        title,
                        price,
                        category,
                        description,
                        image_url,
                        rating_rate,
                        rating_count,
                        loaded_at
                    ) VALUES (
                        :source_id,
                        :title,
                        :price,
                        :category,
                        :description,
                        :image_url,
                        :rating_rate,
                        :rating_count,
                        :loaded_at
                    )
                """),
                {
                    **record,
                    "loaded_at": datetime.now(timezone.utc)
                }
            )

        logger.info(
            f"Inserted {len(transformed)} rows "
            f"into staging.products"
        )

    return len(transformed)


if __name__ == "__main__":
    count = run_products_transform()
    print("\n" + "=" * 40)
    print("PRODUCTS TRANSFORM RESULT")
    print("=" * 40)
    print(f"  Records written to staging.products: {count}")
    print("=" * 40)