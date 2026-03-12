# run_pipeline.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Master pipeline runner
#          Orchestrates all stages in correct order
#          One command runs the entire pipeline end to end
#
# USAGE:
#   python run_pipeline.py
#
# WHAT THIS FILE DOES:
#   1. Extract  → API → raw schema
#   2. Transform → raw → staging schema
#   3. Load     → staging → warehouse schema
#
# WHY ONE FILE AT PROJECT ROOT?
#   All individual stage files live deep in etl/
#   This file is the single entry point:
#   Operators, Airflow, CI/CD all call THIS file
#   Nobody needs to know the internal structure ✅
#
# FAIL FAST PRINCIPLE:
#   If any step fails → stop immediately
#   Do not continue to next step with bad data
#   Better to have a clean failure than corrupt warehouse
# ═══════════════════════════════════════════════════════════

import sys
from datetime import datetime, timezone
from etl.utils.logger import get_logger

# ── Import all pipeline stages ────────────────────────────
from etl.extract.fakestore_extractor import extract_all
from etl.transform.products_transform import run_products_transform
from etl.transform.carts_transform import run_carts_transform
from etl.transform.users_transform import run_users_transform
from etl.load.products_load import run_products_load
from etl.load.users_load import run_users_load
from etl.load.orders_load import run_orders_load

logger = get_logger(__name__)


def run_step(step_name: str, step_func, results: dict):
    """
    Run a single pipeline step safely.

    Args:
        step_name: human readable name for logging
                   e.g. "Extract — FakeStoreAPI"
        step_func: the function to call
        results:   dict to store step results in

    WHY wrap every step in try/except?
    Without wrapping:
        Step 3 fails → Python prints traceback → stops
        No summary of what succeeded before failure
        Hard to know pipeline state ❌

    With wrapping:
        Step 3 fails → log clear error message
                    → record which step failed
                    → print full summary of all steps
                    → exit with error code 1
        Clear picture of exactly what happened ✅

    WHY return the result?
    Each step returns counts (inserted, updated etc)
    We store these in results dict
    Final summary prints everything in one place ✅

    WHY sys.exit(1) on failure?
    Exit code 1 = failure in Unix/Linux convention
    Exit code 0 = success

    This matters for automation:
    Airflow checks exit code to know if step succeeded
    GitHub Actions checks exit code to mark run as failed
    Cron jobs check exit code to send alerts
    Without proper exit codes → automation is blind ❌
    """
    logger.info(f"{'─'*50}")
    logger.info(f"STEP: {step_name}")
    logger.info(f"{'─'*50}")

    try:
        result = step_func()
        results[step_name] = {
            "status": "SUCCESS",
            "result": result
        }
        logger.info(f"STEP COMPLETE: {step_name} ✅")
        return result

    except Exception as e:
        results[step_name] = {
            "status": "FAILED",
            "error":  str(e)
        }
        logger.error(
            f"STEP FAILED: {step_name} ❌\n"
            f"Error: {e}",
            exc_info=True
        )

        # Print summary of what happened before failure
        print_summary(results, failed_at=step_name)

        # Exit with error code so automation knows it failed
        sys.exit(1)


def print_summary(results: dict, failed_at: str = None):
    """
    Print a clear summary of all pipeline steps.

    Args:
        results:   dict of step results collected so far
        failed_at: name of step that failed (if any)

    WHY print a summary?
    Logs are detailed but hard to scan quickly
    Summary gives operators instant pipeline status:
        ✅ Extract completed
        ✅ Transform products completed
        ❌ Transform carts FAILED
        ── Load steps not reached

    One glance tells you everything ✅
    """
    print("\n" + "═" * 55)
    print("  PIPELINE SUMMARY")
    print("═" * 55)

    for step, data in results.items():
        status = data["status"]
        icon   = "✅" if status == "SUCCESS" else "❌"
        print(f"  {icon}  {step}")

        # Show counts if step succeeded and returned dict
        if status == "SUCCESS" and isinstance(data["result"], dict):
            for key, val in data["result"].items():
                print(f"       {key}: {val}")

        # Show error if step failed
        if status == "FAILED":
            print(f"       ERROR: {data['error']}")

    # Show steps that never ran due to earlier failure
    if failed_at:
        print(f"\n  ──  Pipeline stopped at: {failed_at}")

    print("═" * 55)


def run_pipeline():
    """
    Run the full ETL pipeline end to end.

    STEP ORDER (cannot be changed):
    1. Extract     → must run first, populates raw
    2. Transform   → must run after extract, reads raw
    3. Load dims   → must run before facts (FK constraints)
    4. Load facts  → must run last, references dimensions

    WHY this exact order?
    Each step depends on the previous one:
    Transform needs raw data    → extract must run first
    fact_orders needs dim keys  → dimensions must load first
    Wrong order = empty tables, FK violations, corrupt data
    """
    pipeline_start = datetime.now(timezone.utc)
    results = {}

    logger.info("═" * 55)
    logger.info("  RETAIL ETL PIPELINE STARTED")
    logger.info(f"  {pipeline_start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info("═" * 55)

    # ── Stage 1: Extract ──────────────────────────────────
    # Pull all three endpoints from FakeStoreAPI
    # Save raw JSON to raw.products, raw.carts, raw.users
    run_step(
        "Extract — FakeStoreAPI",
        extract_all,
        results
    )

    # ── Stage 2: Transform ────────────────────────────────
    # Read raw JSONB → clean → write to staging
    # Order within transform does not matter
    # (staging tables are independent of each other)
    run_step(
        "Transform — Products",
        run_products_transform,
        results
    )
    run_step(
        "Transform — Carts",
        run_carts_transform,
        results
    )
    run_step(
        "Transform — Users",
        run_users_transform,
        results
    )

    # ── Stage 3: Load Dimensions ──────────────────────────
    # MUST load dimensions before facts
    # fact_orders.product_sk → dim_products.product_sk
    # fact_orders.user_sk    → dim_users.user_sk
    # FK constraint enforces this at database level
    run_step(
        "Load — dim_products (SCD2)",
        run_products_load,
        results
    )
    run_step(
        "Load — dim_users (SCD2)",
        run_users_load,
        results
    )

    # ── Stage 4: Load Facts ───────────────────────────────
    # Must run after dimensions are loaded
    run_step(
        "Load — fact_orders",
        run_orders_load,
        results
    )

    # ── Pipeline Complete ─────────────────────────────────
    pipeline_end = datetime.now(timezone.utc)
    duration = (pipeline_end - pipeline_start).total_seconds()

    logger.info(
        f"Pipeline finished in {duration:.1f} seconds"
    )

    print_summary(results)

    print(f"\n  Total time: {duration:.1f} seconds")
    print(f"  Finished:   "
          f"{pipeline_end.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("═" * 55 + "\n")


if __name__ == "__main__":
    run_pipeline()