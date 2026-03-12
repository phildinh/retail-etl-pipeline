# etl/load/orders_load.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Load orders from staging into warehouse
#
# READS FROM:  staging.carts
# WRITES TO:   warehouse.fact_orders
#
# PATTERN: APPEND ONLY (different from SCD2 dimensions)
#
# WHY no SCD2 here?
# Orders are immutable historical events:
#   "User 1 bought 4 units of product 1 on March 2"
#   This fact never changes after it happened
#   We only ever ADD new orders, never modify old ones
#
# HOW WE PREVENT DUPLICATES:
# Pipeline runs every day
# Some orders from yesterday are already in fact_orders
# We use cart_source_id to check what's already loaded:
#   Already exists → skip
#   New order      → insert
# ═══════════════════════════════════════════════════════════

from datetime import datetime, timezone
from sqlalchemy import text
from etl.utils.db import get_db_connection
from etl.utils.logger import get_logger

logger = get_logger(__name__)


def get_loaded_cart_ids(conn) -> set:
    """
    Fetch all cart_source_ids already in fact_orders.

    Returns:
        Set of cart_source_ids already loaded:
        {1, 2, 3, 4, 5}

    WHY a set not a list?
    We use this for membership checks:
        cart_id in loaded_ids

    Set lookup is O(1) — instant regardless of size ✅
    List lookup is O(n) — slower as table grows ❌

    Same reasoning as using dict in dimension loads:
    Always use the right data structure for the operation
    """
    result = conn.execute(
        text("""
            SELECT DISTINCT cart_source_id
            FROM warehouse.fact_orders
        """)
    )
    return {row.cart_source_id for row in result.fetchall()}


def get_current_product_sk(conn, product_source_id: int) -> int | None:
    """
    Look up the current surrogate key for a product.

    Args:
        product_source_id: the API's product id

    Returns:
        product_sk (our surrogate key) or None if not found

    WHY look up product_sk instead of using source_id directly?
    fact_orders.product_sk is a FOREIGN KEY to dim_products
    It must reference an actual product_sk that exists

    We use is_current=TRUE because:
    We want the price that is active RIGHT NOW
    At first load this is the only version anyway
    In future runs this gives us current pricing ✅
    """
    result = conn.execute(
        text("""
            SELECT product_sk, price
            FROM warehouse.dim_products
            WHERE source_id  = :source_id
              AND is_current  = TRUE
        """),
        {"source_id": product_source_id}
    )
    row = result.fetchone()
    return row if row else None


def get_current_user_sk(conn, user_source_id: int) -> int | None:
    """
    Look up the current surrogate key for a user.

    Args:
        user_source_id: the API's user id

    Returns:
        user_sk (our surrogate key) or None if not found
    """
    result = conn.execute(
        text("""
            SELECT user_sk
            FROM warehouse.dim_users
            WHERE source_id = :source_id
              AND is_current = TRUE
        """),
        {"source_id": user_source_id}
    )
    row = result.fetchone()
    return row.user_sk if row else None


def run_orders_load() -> dict:
    """
    Load staging carts into warehouse.fact_orders.

    Returns:
        Dict with counts:
        {
            "inserted": X,  ← new orders loaded
            "skipped":  Y,  ← already existed, skipped
            "errors":   Z   ← could not find product or user
        }

    WHY track errors separately?
    If a staging cart references a product_source_id
    that does not exist in dim_products → we cannot insert
    This should never happen if pipeline runs in order:
        1. products_load → 2. users_load → 3. orders_load
    But if it does happen → log warning, skip, count it
    Pipeline continues rather than crashing entirely ✅
    """
    logger.info("Starting orders load")

    counts = {"inserted": 0, "skipped": 0, "errors": 0}

    with get_db_connection() as conn:

        # ── Step 1: Get already loaded cart ids ───────────
        # WHY check this first?
        # Pipeline runs daily
        # Cart id 1 was loaded yesterday
        # Today's staging still has cart id 1
        # Without this check → duplicate rows ❌
        # With this check    → skip cart id 1 ✅
        loaded_ids = get_loaded_cart_ids(conn)
        logger.info(
            f"fact_orders already has "
            f"{len(loaded_ids)} loaded cart ids"
        )

        # ── Step 2: Read staging carts ────────────────────
        result = conn.execute(
            text("""
                SELECT
                    cart_source_id,
                    user_source_id,
                    product_source_id,
                    quantity,
                    cart_date
                FROM staging.carts
            """)
        )
        staging_rows = result.fetchall()
        logger.info(
            f"Staging has {len(staging_rows)} cart rows to process"
        )

        # ── Step 3: Insert new orders ─────────────────────
        for row in staging_rows:
            cart_source_id    = row.cart_source_id
            user_source_id    = row.user_source_id
            product_source_id = row.product_source_id
            quantity          = row.quantity
            cart_date         = row.cart_date

            # Already loaded → skip
            if cart_source_id in loaded_ids:
                counts["skipped"] += 1
                continue

            # Look up product surrogate key + price
            # WHY get price from dim_products not staging?
            # dim_products has SCD2 history
            # staging.products only has current state
            # For consistency we always source price
            # from the warehouse dimension ✅
            product_row = get_current_product_sk(
                conn, product_source_id
            )

            if not product_row:
                logger.warning(
                    f"Product source_id={product_source_id} "
                    f"not found in dim_products — skipping"
                )
                counts["errors"] += 1
                continue

            # Look up user surrogate key
            user_sk = get_current_user_sk(conn, user_source_id)

            if not user_sk:
                logger.warning(
                    f"User source_id={user_source_id} "
                    f"not found in dim_users — skipping"
                )
                counts["errors"] += 1
                continue

            product_sk = product_row.product_sk
            unit_price = float(product_row.price)
            total_price = round(unit_price * quantity, 2)

            # Insert the order fact row
            conn.execute(
                text("""
                    INSERT INTO warehouse.fact_orders (
                        cart_source_id,
                        product_sk,
                        user_sk,
                        quantity,
                        unit_price,
                        total_price,
                        order_date,
                        loaded_at
                    ) VALUES (
                        :cart_source_id,
                        :product_sk,
                        :user_sk,
                        :quantity,
                        :unit_price,
                        :total_price,
                        :order_date,
                        :loaded_at
                    )
                """),
                {
                    "cart_source_id": cart_source_id,
                    "product_sk":     product_sk,
                    "user_sk":        user_sk,
                    "quantity":       quantity,
                    "unit_price":     unit_price,
                    "total_price":    total_price,
                    "order_date":     cart_date,
                    "loaded_at":      datetime.now(timezone.utc),
                }
            )
            counts["inserted"] += 1

    logger.info(
        f"Orders load complete | "
        f"inserted={counts['inserted']} | "
        f"skipped={counts['skipped']} | "
        f"errors={counts['errors']}"
    )

    return counts


if __name__ == "__main__":
    counts = run_orders_load()

    print("\n" + "=" * 45)
    print("ORDERS LOAD RESULT")
    print("=" * 45)
    print(f"  Orders inserted:  {counts['inserted']}")
    print(f"  Orders skipped:   {counts['skipped']}")
    print(f"  Errors:           {counts['errors']}")
    print("=" * 45)