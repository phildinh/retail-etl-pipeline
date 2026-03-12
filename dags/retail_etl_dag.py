# dags/retail_etl_dag.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Airflow DAG for retail ETL pipeline
#
# AIRFLOW vs run_pipeline.py:
#   run_pipeline.py → runs once when you call it manually
#   Airflow DAG     → runs on a schedule automatically
#                     retries on failure automatically
#                     sends alerts on failure
#                     shows visual history of every run
#                     lets you rerun individual failed steps
#
# WHAT IS A DAG?
#   Directed Acyclic Graph
#   Directed  → steps flow in one direction (no loops)
#   Acyclic   → no step can depend on itself
#   Graph     → steps and their dependencies visualised
# ═══════════════════════════════════════════════════════════

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Import all pipeline functions ─────────────────────────
from etl.extract.fakestore_extractor import extract_all
from etl.transform.products_transform import run_products_transform
from etl.transform.carts_transform import run_carts_transform
from etl.transform.users_transform import run_users_transform
from etl.load.products_load import run_products_load
from etl.load.users_load import run_users_load
from etl.load.orders_load import run_orders_load

# ─────────────────────────────────────────────────────────
# DEFAULT ARGS
#
# These settings apply to every task in the DAG
# unless a task overrides them specifically
#
# owner        → who owns this DAG (shows in Airflow UI)
# retries      → how many times to retry on failure
# retry_delay  → how long to wait between retries
# start_date   → when the DAG becomes active
#                set in the past → Airflow won't backfill
#
# WHY retries=2?
# FakeStoreAPI occasionally times out
# Retry gives it 2 more chances before marking as failed
# retry_delay=5min → don't hammer the API immediately ✅
# ─────────────────────────────────────────────────────────
default_args = {
    "owner":          "retail_etl",
    "retries":        2,
    "retry_delay":    timedelta(minutes=5),
    "start_date":     datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
}

# ─────────────────────────────────────────────────────────
# DAG DEFINITION
#
# dag_id        → unique name, shows in Airflow UI
# schedule      → cron expression for when to run
#                 "0 2 * * *" = 2am every day
#                 WHY 2am? Low traffic, fresh daily data
# catchup=False → don't run missed historical runs
#                 WHY False? We don't want Airflow to
#                 run the pipeline for every day since
#                 start_date when we first deploy ❌
# ─────────────────────────────────────────────────────────
with DAG(
    dag_id="retail_etl_pipeline",
    description="Daily ETL pipeline: FakeStoreAPI → warehouse",
    default_args=default_args,
    schedule="0 2 * * *",
    catchup=False,
    tags=["retail", "etl", "fakestore"],
) as dag:

    # ── Task 1: Extract ───────────────────────────────────
    # PythonOperator → calls a Python function as a task
    # task_id → unique name within this DAG
    extract_task = PythonOperator(
        task_id="extract_fakestore_api",
        python_callable=extract_all,
    )

    # ── Task 2: Transform ─────────────────────────────────
    # These three can run in parallel
    # WHY? They read from different raw tables
    # products transform doesn't affect carts transform
    # Running in parallel → faster pipeline ✅
    transform_products_task = PythonOperator(
        task_id="transform_products",
        python_callable=run_products_transform,
    )

    transform_carts_task = PythonOperator(
        task_id="transform_carts",
        python_callable=run_carts_transform,
    )

    transform_users_task = PythonOperator(
        task_id="transform_users",
        python_callable=run_users_transform,
    )

    # ── Task 3: Load Dimensions ───────────────────────────
    # Products and users load can also run in parallel
    # Both read from staging, write to different tables
    load_products_task = PythonOperator(
        task_id="load_dim_products",
        python_callable=run_products_load,
    )

    load_users_task = PythonOperator(
        task_id="load_dim_users",
        python_callable=run_users_load,
    )

    # ── Task 4: Load Facts ────────────────────────────────
    # Must run AFTER both dimensions are loaded
    # FK constraints enforce this at DB level
    # DAG dependencies enforce this at pipeline level
    load_orders_task = PythonOperator(
        task_id="load_fact_orders",
        python_callable=run_orders_load,
    )

    # ─────────────────────────────────────────────────────
    # TASK DEPENDENCIES
    #
    # >> operator means "must run before"
    # a >> b means: a must complete before b starts
    #
    # VISUAL FLOW:
    #
    # extract
    #    ↓
    # transform_products  transform_carts  transform_users
    #    ↓                                      ↓
    # load_products                        load_users
    #         ↘                           ↙
    #            load_orders (fact table)
    #
    # WHY parallel transforms?
    # Airflow can run independent tasks simultaneously
    # Faster than running them sequentially
    # ─────────────────────────────────────────────────────

    # Extract must finish before any transform starts
    extract_task >> [
        transform_products_task,
        transform_carts_task,
        transform_users_task,
    ]

    # Products transform → products load
    transform_products_task >> load_products_task

    # Users transform → users load
    transform_users_task >> load_users_task

    # Carts transform feeds into orders
    # BUT orders also needs both dimensions loaded first
    [
        transform_carts_task,
        load_products_task,
        load_users_task,
    ] >> load_orders_task