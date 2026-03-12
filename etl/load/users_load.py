# etl/load/users_load.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Load users from staging into warehouse
#          using SCD Type 2 pattern
#
# READS FROM:  staging.users
# WRITES TO:   warehouse.dim_users
#
# SAME SCD2 PATTERN AS products_load.py:
#   New user     → INSERT new row, is_current=TRUE
#   Changed user → expire old row, INSERT new row
#   Unchanged    → do nothing
#
# TRACKED FIELDS (fields worth versioning):
#   email    → if email changes, user identity changes
#   username → same reason
#   address  → address changes affect regional analysis
#
# NOT TRACKED (not worth versioning):
#   phone    → cosmetic, low analytical value
# ═══════════════════════════════════════════════════════════

from datetime import date, datetime, timezone
from sqlalchemy import text
from etl.utils.db import get_db_connection
from etl.utils.logger import get_logger

logger = get_logger(__name__)

TRACKED_FIELDS = [
    "email",
    "username",
    "address_street",
    "address_city",
    "address_zip",
]


def get_current_users(conn) -> dict:
    """
    Fetch all currently active users from dim_users.

    Returns:
        Dict keyed by source_id for O(1) lookup:
        {
            1: {
                "user_sk":      1001,
                "email":        "john@gmail.com",
                "username":     "johnd",
                "address_city": "Ann Arbor",
                ...
            }
        }
    """
    result = conn.execute(
        text("""
            SELECT
                user_sk,
                source_id,
                email,
                username,
                address_street,
                address_city,
                address_zip
            FROM warehouse.dim_users
            WHERE is_current = TRUE
        """)
    )

    return {
        row.source_id: {
            "user_sk":       row.user_sk,
            "email":         row.email,
            "username":      row.username,
            "address_street":row.address_street,
            "address_city":  row.address_city,
            "address_zip":   row.address_zip,
        }
        for row in result.fetchall()
    }


def has_changed(current: dict, staging: dict) -> bool:
    """
    Check if any tracked field has changed.

    Same logic as products_load.py:
    Compare as strings to avoid type mismatch issues.
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


def expire_user(conn, user_sk: int, today: date) -> None:
    """
    Expire an existing user version.

    Sets valid_to = today, is_current = FALSE.
    Old version preserved forever in table ✅
    """
    conn.execute(
        text("""
            UPDATE warehouse.dim_users
            SET
                valid_to   = :valid_to,
                is_current = FALSE
            WHERE user_sk = :user_sk
        """),
        {
            "valid_to": today,
            "user_sk":  user_sk,
        }
    )
    logger.debug(f"Expired user_sk={user_sk}")


def insert_user(conn, record: dict, today: date) -> None:
    """
    Insert a new user version into dim_users.

    Used for both new users and updated users.
    """
    conn.execute(
        text("""
            INSERT INTO warehouse.dim_users (
                source_id,
                email,
                username,
                first_name,
                last_name,
                phone,
                address_street,
                address_city,
                address_zip,
                valid_from,
                valid_to,
                is_current
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


def run_users_load() -> dict:
    """
    Load staging users into warehouse using SCD Type 2.

    Returns:
        Dict with counts:
        {"inserted": X, "updated": Y, "unchanged": Z}
    """
    logger.info("Starting users load (SCD Type 2)")

    today = date.today()
    counts = {"inserted": 0, "updated": 0, "unchanged": 0}

    with get_db_connection() as conn:

        # ── Step 1: Get current warehouse state ───────────
        current_users = get_current_users(conn)
        logger.info(
            f"Current warehouse has "
            f"{len(current_users)} active users"
        )

        # ── Step 2: Read staging users ────────────────────
        result = conn.execute(
            text("""
                SELECT
                    source_id, email, username,
                    first_name, last_name, phone,
                    address_street, address_city, address_zip
                FROM staging.users
            """)
        )
        staging_rows = result.fetchall()
        logger.info(
            f"Staging has {len(staging_rows)} users to process"
        )

        # ── Step 3: SCD2 logic ────────────────────────────
        for row in staging_rows:
            staging = dict(row._mapping)
            source_id = staging["source_id"]

            if source_id not in current_users:
                # CASE 1: Brand new user
                insert_user(conn, staging, today)
                counts["inserted"] += 1
                logger.debug(
                    f"NEW user source_id={source_id}"
                )

            elif has_changed(current_users[source_id], staging):
                # CASE 2: User details have changed
                # expire old → insert new version
                expire_user(
                    conn,
                    current_users[source_id]["user_sk"],
                    today
                )
                insert_user(conn, staging, today)
                counts["updated"] += 1
                logger.debug(
                    f"UPDATED user source_id={source_id}"
                )

            else:
                # CASE 3: Nothing changed — do nothing
                counts["unchanged"] += 1
                logger.debug(
                    f"UNCHANGED user source_id={source_id}"
                )

    logger.info(
        f"Users load complete | "
        f"inserted={counts['inserted']} | "
        f"updated={counts['updated']} | "
        f"unchanged={counts['unchanged']}"
    )

    return counts


if __name__ == "__main__":
    counts = run_users_load()

    print("\n" + "=" * 45)
    print("USERS LOAD RESULT (SCD Type 2)")
    print("=" * 45)
    print(f"  New users inserted:     {counts['inserted']}")
    print(f"  Existing users updated: {counts['updated']}")
    print(f"  Unchanged users:        {counts['unchanged']}")
    print("=" * 45)