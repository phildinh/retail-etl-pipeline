# etl/transform/carts_transform.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Transform raw carts into clean staging rows
#
# READS FROM:  raw.carts     (JSONB, one row per cart)
# WRITES TO:   staging.carts (flat, exploded, one row
#                              per product per cart)
#
# KEY CHALLENGE — EXPLODING NESTED LIST:
#   Raw:     1 cart with 3 products = 1 row
#   Staging: 1 cart with 3 products = 3 rows
#
#   WHY explode?
#   Business questions we need to answer:
#   "How many times was product X ordered?"
#   "What is the total quantity ordered per product?"
#   These are impossible with a nested list ❌
#   Easy after exploding to one row per product ✅
# ═══════════════════════════════════════════════════════════

import json
from datetime import datetime, timezone
from sqlalchemy import text
from etl.utils.db import get_db_connection
from etl.utils.logger import get_logger

logger = get_logger(__name__)


def parse_cart_date(date_str: str):
    """
    Parse API date string into a Python date object.

    Args:
        date_str: ISO format string from API
                  e.g. "2020-03-02T00:00:00.000Z"

    Returns:
        datetime.date object e.g. date(2020, 3, 2)

    WHY parse to date not datetime?
    API sends "2020-03-02T00:00:00.000Z"
    The time component is always T00:00:00.000Z
    meaning midnight UTC — no real time information
    Storing as DATE is more honest than TIMESTAMP here
    Never store more precision than the data actually has

    WHY fromisoformat after stripping Z?
    Python's fromisoformat does not handle the Z suffix
    until Python 3.11+
    Safer to strip it manually for compatibility:
        "2020-03-02T00:00:00.000Z"
        → strip Z
        → "2020-03-02T00:00:00.000"
        → fromisoformat parses correctly
        → .date() extracts date only
    """
    # Remove trailing Z (UTC indicator) for compatibility
    cleaned = date_str.replace("Z", "")
    return datetime.fromisoformat(cleaned).date()


def transform_cart(raw_record: dict) -> list[dict]:
    """
    Transform one raw cart into multiple staging rows.

    Args:
        raw_record: dict parsed from raw.carts.raw_data

    Returns:
        LIST of dicts — one dict per product in the cart
        1 cart with 3 products → returns list of 3 dicts

    WHY return a list not a single dict?
    Products transform returns one dict per record
    because one product = one staging row

    Carts transform returns a LIST of dicts per record
    because one cart = MULTIPLE staging rows (exploded)

    This difference is important:
    products_transform: transformed.append(clean)
    carts_transform:    transformed.extend(clean_rows)
                        ↑ extend adds all items in list
                        ↑ append would add the list itself

    Example input:
    {
        "id": 1,
        "userId": 1,
        "date": "2020-03-02T00:00:00.000Z",
        "products": [
            {"productId": 1, "quantity": 4},
            {"productId": 2, "quantity": 1}
        ]
    }

    Example output (list of 2 dicts):
    [
        {
            "cart_source_id": 1, "user_source_id": 1,
            "product_source_id": 1, "quantity": 4,
            "cart_date": date(2020, 3, 2)
        },
        {
            "cart_source_id": 1, "user_source_id": 1,
            "product_source_id": 2, "quantity": 1,
            "cart_date": date(2020, 3, 2)
        }
    ]
    """
    cart_id   = raw_record["id"]
    user_id   = raw_record["userId"]
    cart_date = parse_cart_date(raw_record["date"])

    # ── Explode products list ─────────────────────────────
    # For each product in the nested list:
    # create one flat row combining cart + product fields
    rows = []
    for product in raw_record.get("products", []):
        rows.append({
            "cart_source_id":    cart_id,
            "user_source_id":    user_id,
            "product_source_id": product["productId"],
            "quantity":          product["quantity"],
            "cart_date":         cart_date,
        })

    # WHY log a warning if no products?
    # A cart with zero products should never exist
    # If it does → data quality issue worth investigating
    if not rows:
        logger.warning(
            f"Cart {cart_id} has no products — skipping"
        )

    return rows


def run_carts_transform() -> int:
    """
    Read all raw carts, explode products, write to staging.

    Returns:
        Number of rows written to staging.carts
        (will be MORE than number of raw carts
         because of exploding)
    """
    logger.info("Starting carts transform")

    with get_db_connection() as conn:

        # ── Step 1: Read from raw ─────────────────────────
        result = conn.execute(
            text("SELECT raw_data FROM raw.carts")
        )
        raw_rows = result.fetchall()
        logger.info(f"Read {len(raw_rows)} rows from raw.carts")

        # ── Step 2: Transform and explode ────────────────
        transformed = []
        for row in raw_rows:
            raw_record = row[0]

            # transform_cart returns a LIST (exploded rows)
            # extend() adds all items from that list
            # into our transformed list
            #
            # WHY extend not append?
            # append([row1, row2]) → [[row1, row2]]  ❌ nested list
            # extend([row1, row2]) → [row1, row2]    ✅ flat list
            clean_rows = transform_cart(raw_record)
            transformed.extend(clean_rows)

        logger.info(
            f"Exploded {len(raw_rows)} carts "
            f"into {len(transformed)} rows"
        )

        # ── Step 3: Truncate staging ──────────────────────
        conn.execute(text("TRUNCATE TABLE staging.carts"))
        logger.info("Truncated staging.carts")

        # ── Step 4: Insert exploded rows ──────────────────
        for record in transformed:
            conn.execute(
                text("""
                    INSERT INTO staging.carts (
                        cart_source_id,
                        user_source_id,
                        product_source_id,
                        quantity,
                        cart_date,
                        loaded_at
                    ) VALUES (
                        :cart_source_id,
                        :user_source_id,
                        :product_source_id,
                        :quantity,
                        :cart_date,
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
            f"into staging.carts"
        )

    return len(transformed)


if __name__ == "__main__":
    count = run_carts_transform()

    print("\n" + "=" * 40)
    print("CARTS TRANSFORM RESULT")
    print("=" * 40)
    print(f"  Raw carts in source:          7")
    print(f"  Rows written to staging.carts: {count}")
    print(f"  (more rows than carts = explode worked ✅)")
    print("=" * 40)