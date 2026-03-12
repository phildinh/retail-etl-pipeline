# etl/load/products_load.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Load products from staging into warehouse
#          using SCD Type 2 pattern
#
# READS FROM:  staging.products
# WRITES TO:   warehouse.dim_products
#
# SCD TYPE 2 LOGIC:
#   New product    → INSERT new row, is_current=TRUE
#   Changed product → expire old row (valid_to=today,
#                     is_current=FALSE)
#                     INSERT new row (valid_from=today,
#                     is_current=TRUE)
#   Unchanged      → do nothing
#
# WHY NOT TRUNCATE AND RELOAD FOR WAREHOUSE?
#   Truncate destroys history ❌
#   SCD Type 2 preserves every version forever ✅
#   Analysts can answer: "what was the price last January?"
# ═══════════════════════════════════════════════════════════

from datetime import date, datetime, timezone
from sqlalchemy import text
from etl.utils.db import get_db_connection
from etl.utils.logger import get_logger

logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────
# WHICH FIELDS TRIGGER A NEW SCD2 VERSION?
#
# Not every field change needs a new version
# We only track fields that affect business analysis:
#   price    → revenue calculations change if price changes
#   category → grouping/filtering changes if category changes
#   title    → product identity changes if title changes
#
# We do NOT track:
#   image_url    → cosmetic, no analytical impact
#   description  → cosmetic, no analytical impact
#   rating_*     → constantly changing, not worth versioning
# ─────────────────────────────────────────────────────────
TRACKED_FIELDS = ["title", "price", "category"]


def get_current_products(conn) -> dict:
    """
    Fetch all currently active products from dim_products.

    Returns:
        Dict keyed by source_id:
        {
            1: {
                "product_sk": 1001,
                "title": "Backpack",
                "price": Decimal("109.95"),
                "category": "men's clothing"
            },
            ...
        }

    WHY dict keyed by source_id?
    When processing staging records we need to quickly
    check: "does this source_id already exist in warehouse?"

    Dict lookup is O(1) — instant regardless of size:
        current[source_id]  → O(1) ✅
    vs list scan which is O(n) — slower as table grows:
        for row in current_list → O(n) ❌
    """
    result = conn.execute(
        text("""
            SELECT
                product_sk,
                source_id,
                title,
                price,
                category
            FROM warehouse.dim_products
            WHERE is_current = TRUE
        """)
    )

    # Build dict: {source_id: {all fields}}
    return {
        row.source_id: {
            "product_sk": row.product_sk,
            "title":      row.title,
            "price":      row.price,
            "category":   row.category,
        }
        for row in result.fetchall()
    }


def has_changed(current: dict, staging: dict) -> bool:
    """
    Check if any tracked field has changed.

    Args:
        current: current warehouse record
        staging: incoming staging record

    Returns:
        True if any tracked field differs → need new version
        False if nothing changed → do nothing

    WHY compare as strings for price?
    current["price"] comes from PostgreSQL as Decimal type
    staging["price"] comes from Python as float type
    Decimal("109.95") == float(109.95) can be unreliable
    Comparing as strings avoids floating point surprises:
        str(Decimal("109.95")) == str(float(109.95))
        "109.95" == "109.95" ✅ reliable comparison
    """
    for field in TRACKED_FIELDS:
        current_val = str(current.get(field, "")).strip()
        staging_val = str(staging.get(field, "")).strip()
        if current_val != staging_val:
            logger.debug(
                f"Change detected in {field}: "
                f"{current_val!r} → {staging_val!r}"
            )
            return True
    return False


def expire_product(conn, product_sk: int, today: date) -> None:
    """
    Expire an existing product version.

    Sets valid_to = today and is_current = FALSE
    This marks the old version as no longer active
    but PRESERVES it in the table forever ✅

    Args:
        conn:       database connection
        product_sk: surrogate key of version to expire
        today:      date this version became inactive
    """
    conn.execute(
        text("""
            UPDATE warehouse.dim_products
            SET
                valid_to   = :valid_to,
                is_current = FALSE
            WHERE product_sk = :product_sk
        """),
        {
            "valid_to":   today,
            "product_sk": product_sk,
        }
    )
    logger.debug(f"Expired product_sk={product_sk}")


def insert_product(conn, record: dict, today: date) -> None:
    """
    Insert a new product version into dim_products.

    Used for both:
    - Brand new products (never seen before)
    - Updated products (new version after expiring old one)

    Args:
        conn:   database connection
        record: staging.products row as dict
        today:  valid_from date for this version
    """
    conn.execute(
        text("""
            INSERT INTO warehouse.dim_products (
                source_id,
                title,
                price,
                category,
                description,
                image_url,
                rating_rate,
                rating_count,
                valid_from,
                valid_to,
                is_current
            ) VALUES (
                :source_id,
                :title,
                :price,
                :category,
                :description,
                :image_url,
                :rating_rate,
                :rating_count,
                :valid_from,
                '9999-12-31',
                TRUE
            )
        """),
        {
            **record,
            "valid_from": today,
        }
    )


def run_products_load() -> dict:
    """
    Load staging products into warehouse using SCD Type 2.

    Returns:
        Dict with counts of what happened:
        {
            "inserted": 15,  ← new products never seen before
            "updated":   3,  ← products with changed fields
            "unchanged": 2   ← products with no changes
        }

    WHY return a breakdown not just total?
    Observability — caller can detect unusual patterns:
        inserted=0, updated=0, unchanged=20
        → pipeline ran but nothing changed (normal) ✅

        inserted=20, updated=0, unchanged=0
        → first run ever (normal) ✅

        inserted=0, updated=20, unchanged=0
        → ALL products changed at once (suspicious ⚠)
        → worth investigating
    """
    logger.info("Starting products load (SCD Type 2)")

    today = date.today()
    counts = {"inserted": 0, "updated": 0, "unchanged": 0}

    with get_db_connection() as conn:

        # ── Step 1: Get current warehouse state ───────────
        current_products = get_current_products(conn)
        logger.info(
            f"Current warehouse has "
            f"{len(current_products)} active products"
        )

        # ── Step 2: Read staging products ────────────────
        result = conn.execute(
            text("""
                SELECT
                    source_id, title, price, category,
                    description, image_url,
                    rating_rate, rating_count
                FROM staging.products
            """)
        )
        staging_rows = result.fetchall()
        logger.info(
            f"Staging has {len(staging_rows)} products to process"
        )

        # ── Step 3: Compare and apply SCD2 logic ─────────
        for row in staging_rows:
            staging = dict(row._mapping)
            source_id = staging["source_id"]

            if source_id not in current_products:
                # ── CASE 1: Brand new product ─────────────
                # Never seen this source_id before
                # Insert as new active version
                insert_product(conn, staging, today)
                counts["inserted"] += 1
                logger.debug(
                    f"NEW product source_id={source_id}"
                )

            elif has_changed(current_products[source_id], staging):
                # ── CASE 2: Existing product has changed ──
                # Step A: expire the current version
                expire_product(
                    conn,
                    current_products[source_id]["product_sk"],
                    today
                )
                # Step B: insert new version
                insert_product(conn, staging, today)
                counts["updated"] += 1
                logger.debug(
                    f"UPDATED product source_id={source_id}"
                )

            else:
                # ── CASE 3: Nothing changed ───────────────
                # Do absolutely nothing
                # No UPDATE, no INSERT, no touching the row
                counts["unchanged"] += 1
                logger.debug(
                    f"UNCHANGED product source_id={source_id}"
                )

    logger.info(
        f"Products load complete | "
        f"inserted={counts['inserted']} | "
        f"updated={counts['updated']} | "
        f"unchanged={counts['unchanged']}"
    )

    return counts


if __name__ == "__main__":
    counts = run_products_load()

    print("\n" + "=" * 45)
    print("PRODUCTS LOAD RESULT (SCD Type 2)")
    print("=" * 45)
    print(f"  New products inserted:  {counts['inserted']}")
    print(f"  Existing products updated: {counts['updated']}")
    print(f"  Unchanged products:     {counts['unchanged']}")
    print("=" * 45)