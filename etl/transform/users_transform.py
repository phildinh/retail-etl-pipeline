# etl/transform/users_transform.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Transform raw users into clean staging rows
#
# READS FROM:  raw.users     (JSONB, one row per user)
# WRITES TO:   staging.users (flat, typed, clean)
#
# KEY CHALLENGES:
#   1. Deeply nested objects:
#      name    → {firstname, lastname}
#      address → {street, city, zipcode, geolocation}
#      geolocation → {lat, long}  (nested inside address!)
#
#   2. Password field exists in raw → NEVER stored
#      Security rule: exclude at earliest possible point
#      Never let it reach staging or warehouse
# ═══════════════════════════════════════════════════════════

from datetime import datetime, timezone
from sqlalchemy import text
from etl.utils.db import get_db_connection
from etl.utils.logger import get_logger

logger = get_logger(__name__)


def transform_user(raw_record: dict) -> dict:
    """
    Transform one raw user record into a clean staging row.

    Args:
        raw_record: dict parsed from raw.users.raw_data

    Returns:
        Clean dict ready to insert into staging.users
        NOTE: password is deliberately excluded

    WHY so many .get() calls with defaults?
    Discovery showed no nulls in this API
    But defensive coding means we handle missing fields
    gracefully instead of crashing:

        raw_record["name"]["firstname"]
        → KeyError if name is missing ❌
        → KeyError if firstname is missing ❌

        raw_record.get("name", {}).get("firstname")
        → returns None if name missing ✅
        → returns None if firstname missing ✅
        → pipeline continues, null stored in staging ✅

    This matters in production because:
    APIs change without warning
    A missing field should not crash your entire pipeline

    Example input:
    {
        "id": 1,
        "email": "john@gmail.com",
        "username": "johnd",
        "password": "m38rmF$",        ← never stored
        "name": {
            "firstname": "John",
            "lastname": "Doe"
        },
        "address": {
            "street": "7835 new road",
            "city": "Ann Arbor",
            "zipcode": "48100",
            "geolocation": {
                "lat": "-37.3159",
                "long": "81.1496"
            }
        },
        "phone": "1-570-236-7033"
    }

    Example output:
    {
        "source_id":     1,
        "email":         "john@gmail.com",
        "username":      "johnd",
        "first_name":    "John",
        "last_name":     "Doe",
        "phone":         "1-570-236-7033",
        "address_street":"7835 new road",
        "address_city":  "Ann Arbor",
        "address_zip":   "48100",
        "address_lat":   -37.3159,
        "address_lng":   81.1496
        # password → GONE ✅
    }
    """
    # ── Extract nested objects safely ─────────────────────
    # Each .get() with default {} means:
    # "if this key is missing, give me an empty dict
    #  so the next .get() doesn't crash"
    name        = raw_record.get("name", {})
    address     = raw_record.get("address", {})
    geolocation = address.get("geolocation", {})

    # ── Parse geolocation strings to float ───────────────
    # Discovery showed lat/long come as STRINGS not numbers:
    # "lat": "-37.3159"  ← string with quotes
    # "long": "81.1496"  ← string with quotes
    #
    # WHY strings? Probably an API quirk
    # We must cast to float for NUMERIC(9,6) column
    # float("-37.3159") → -37.3159 ✅
    #
    # WHY try/except here?
    # If lat/long is missing or not a valid number:
    # float(None)  → TypeError ❌
    # float("abc") → ValueError ❌
    # We catch both and store None instead of crashing
    try:
        lat = float(geolocation.get("lat")) \
            if geolocation.get("lat") else None
        lng = float(geolocation.get("long")) \
            if geolocation.get("long") else None
    except (TypeError, ValueError):
        logger.warning(
            f"Could not parse geolocation for "
            f"user {raw_record.get('id')}"
        )
        lat = None
        lng = None

    return {
        "source_id":      raw_record["id"],
        "email":          raw_record["email"],
        "username":       raw_record["username"],

        # password deliberately excluded here
        # no comment needed — absence is the security decision

        "first_name":     name.get("firstname"),
        "last_name":      name.get("lastname"),
        "phone":          raw_record.get("phone"),
        "address_street": address.get("street"),
        "address_city":   address.get("city"),
        "address_zip":    address.get("zipcode"),
        "address_lat":    lat,
        "address_lng":    lng,
    }


def run_users_transform() -> int:
    """
    Read all raw users, transform, write to staging.

    Returns:
        Number of records written to staging.users
    """
    logger.info("Starting users transform")

    with get_db_connection() as conn:

        # ── Step 1: Read from raw ─────────────────────────
        result = conn.execute(
            text("SELECT raw_data FROM raw.users")
        )
        raw_rows = result.fetchall()
        logger.info(f"Read {len(raw_rows)} rows from raw.users")

        # ── Step 2: Transform each record ────────────────
        transformed = []
        for row in raw_rows:
            raw_record = row[0]
            clean = transform_user(raw_record)
            transformed.append(clean)

        # ── Step 3: Truncate staging ──────────────────────
        conn.execute(text("TRUNCATE TABLE staging.users"))
        logger.info("Truncated staging.users")

        # ── Step 4: Insert transformed records ───────────
        for record in transformed:
            conn.execute(
                text("""
                    INSERT INTO staging.users (
                        source_id,
                        email,
                        username,
                        first_name,
                        last_name,
                        phone,
                        address_street,
                        address_city,
                        address_zip,
                        address_lat,
                        address_lng,
                        loaded_at
                    ) VALUES (
                        :source_id,
                        :email,
                        :username,
                        :first_name,
                        :last_name,
                        :phone,
                        :address_street,
                        :address_city,
                        :address_zip,
                        :address_lat,
                        :address_lng,
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
            f"into staging.users"
        )

    return len(transformed)


if __name__ == "__main__":
    count = run_users_transform()

    print("\n" + "=" * 40)
    print("USERS TRANSFORM RESULT")
    print("=" * 40)
    print(f"  Records written to staging.users: {count}")
    print(f"  password field excluded ✅")
    print("=" * 40)