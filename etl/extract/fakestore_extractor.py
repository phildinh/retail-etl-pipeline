# etl/extract/fakestore_extractor.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Extract data from FakeStoreAPI
#          Save raw JSON to raw schema in PostgreSQL
#
# SINGLE RESPONSIBILITY:
#   This file ONLY knows about FakeStoreAPI
#   It knows the endpoints (/products, /carts, /users)
#   It saves raw responses to raw.products, raw.carts, raw.users
#   It does NOT clean or transform anything
#
# RELATIONSHIP WITH api_client.py:
#   api_client.py  → HOW to make HTTP calls (generic)
#   this file      → WHAT to call and WHERE to save (specific)
#
# PATTERN — why save raw JSON to database?
#   Option A: save raw to database first (what we do)
#   Option B: transform in memory, save only clean data
#
#   Option A is safer because:
#   Raw data preserved forever as audit trail
#   If transform logic has a bug → replay from raw ✅
#   If API changes → raw captures the change ✅
# ═══════════════════════════════════════════════════════════

import json
from datetime import datetime, timezone
from sqlalchemy import text
from etl.extract.api_client import api_client
from etl.utils.db import get_db_connection
from etl.utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────
# WHY define endpoints as a constant dict?
#
# Bad approach — hardcode in every function:
#   def extract_products():
#       data = api_client.get("/products")  ← magic string
#   def extract_users():
#       data = api_client.get("/users")     ← magic string
#
#   Magic strings scattered everywhere ❌
#   Change endpoint name → find and update every function ❌
#
# Good approach — define once at top:
#   ENDPOINTS = {"products": "/products", ...}
#   Change endpoint → update ONE place ✅
#   Also documents all endpoints clearly in one spot ✅
# ─────────────────────────────────────────────────────────
ENDPOINTS = {
    "products": "/products",
    "carts":    "/carts",
    "users":    "/users",
}


def extract_endpoint(name: str, endpoint: str) -> list:
    """
    Pull all records from one API endpoint.

    Args:
        name:     human readable name e.g. "products"
        endpoint: API path e.g. "/products"

    Returns:
        List of raw records exactly as API sent them

    WHY return the raw data instead of saving inside here?
    Single responsibility:
        extract_endpoint = ONLY fetches data
        save_raw         = ONLY saves to database
    Separating them makes each function easier to test:
        test fetch without needing a database ✅
        test save without needing an API ✅
    """
    logger.info(f"Extracting {name} from API")

    data = api_client.get(endpoint)

    logger.info(f"Extracted {len(data)} {name} records")
    return data


def save_raw(name: str, records: list) -> int:
    """
    Save raw API records to the raw schema.

    Args:
        name:    table name e.g. "products"
                 maps to raw.products table
        records: list of dicts from API response

    Returns:
        Number of records saved

    WHY store as JSONB (raw_data column)?
    Each record is stored exactly as the API sent it:
        {"id": 1, "title": "Backpack", "price": 109.95, ...}

    Benefits:
    → API adds new field tomorrow? Captured automatically ✅
    → No schema changes needed in raw layer ✅
    → Original data preserved forever ✅

    WHY use executemany style with a loop?
    We insert one record at a time inside a transaction
    If ANY record fails → entire batch rolls back
    Either ALL records saved or NONE ✅
    Prevents partial saves (half the products saved) ❌

    WHY json.dumps(record)?
    SQLAlchemy needs a string to insert into JSONB column
    json.dumps converts dict → JSON string:
        {"id": 1, "title": "Backpack"} → '{"id": 1, "title": "Backpack"}'
    PostgreSQL then stores it as JSONB binary ✅
    """
    table = f"raw.{name}"
    saved_count = 0

    # get_db_connection() is our context manager from db.py
    # → handles commit on success, rollback on failure
    # → closes connection when block exits
    with get_db_connection() as conn:
        for record in records:
            conn.execute(
                text(f"""
                    INSERT INTO {table} (raw_data, loaded_at)
                    VALUES (:raw_data, :loaded_at)
                """),
                {
                    # Convert dict to JSON string for JSONB column
                    "raw_data":  json.dumps(record),

                    # WHY timezone.utc?
                    # Always store timestamps in UTC
                    # Never store local time in a database
                    #
                    # Why? Daylight saving time causes chaos:
                    # Sydney is UTC+11 in summer, UTC+10 in winter
                    # If you store local time:
                    #   "2026-04-05 02:30:00" → ambiguous ❌
                    #   did this happen before or after clocks changed?
                    # UTC has no daylight saving → always unambiguous ✅
                    # Convert to local time only when displaying to users
                    "loaded_at": datetime.now(timezone.utc),
                }
            )
            saved_count += 1

    logger.info(f"Saved {saved_count} records to {table}")
    return saved_count


def extract_all() -> dict:
    """
    Extract all three endpoints and save to raw schema.

    Returns:
        Dict with record counts per endpoint:
        {"products": 20, "carts": 7, "users": 10}

    WHY return counts?
    Caller can log or check counts:
        results = extract_all()
        if results["products"] == 0:
            → something is wrong, alert someone

    This is called an OBSERVABILITY pattern:
    Your pipeline reports what it did
    So you can detect problems without reading logs ✅

    WHY extract all three in one function?
    Pipeline runner (Stage 8) calls extract_all()
    One call, everything extracted ✅
    No need to call three separate functions ✅
    """
    logger.info("Starting full extraction from FakeStoreAPI")

    results = {}

    for name, endpoint in ENDPOINTS.items():
        # Step 1: fetch from API
        records = extract_endpoint(name, endpoint)

        # Step 2: save raw to database
        count = save_raw(name, records)

        # Step 3: record how many we saved
        results[name] = count

    logger.info(
        f"Extraction complete | "
        f"products={results.get('products', 0)} | "
        f"carts={results.get('carts', 0)} | "
        f"users={results.get('users', 0)}"
    )

    return results


# ─────────────────────────────────────────────────────────
# ALLOW RUNNING THIS FILE DIRECTLY FOR TESTING
#
# WHY if __name__ == "__main__"?
#
# When Python imports a file:
#   from etl.extract.fakestore_extractor import extract_all
#   → runs all code at module level
#   → but NOT code inside if __name__ == "__main__"
#
# When Python runs a file directly:
#   python etl/extract/fakestore_extractor.py
#   → runs everything including if __name__ == "__main__"
#
# This lets us:
#   Test this file standalone: python fakestore_extractor.py
#   Import it in pipeline:     from etl.extract... import extract_all
#   Both work without conflict ✅
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    results = extract_all()

    print("\n" + "=" * 40)
    print("EXTRACTION RESULTS")
    print("=" * 40)
    for name, count in results.items():
        print(f"  {name:<15} {count} records saved to raw.{name}")
    print("=" * 40)