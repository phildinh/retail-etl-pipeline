# etl/utils/db.py

import psycopg2
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

from etl.utils.config import settings
from etl.utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────
# SQLALCHEMY ENGINE — THE CONNECTION POOL
#
# WHY create_engine() ONCE at module level?
# Same reason as settings singleton in config.py.
# Creating an engine is expensive — it sets up the pool.
# We create it once, every file imports and reuses it.
#
# WHY QueuePool?
# SQLAlchemy has several pool types:
#   NullPool    → no pooling, new connection every time ❌
#   StaticPool  → single connection, for testing only
#   QueuePool   → queue of reusable connections ✅ production
#
# pool_size=5 means:
#   5 connections always open and waiting
#   Good for our pipeline — we rarely need more than 5 at once
#
# max_overflow=10 means:
#   If all 5 are busy, open up to 10 MORE temporarily
#   Total max = 5 + 10 = 15 connections under heavy load
#
# pool_timeout=30 means:
#   If no connection available after 30 seconds → raise error
#   Prevents infinite waiting
#
# pool_pre_ping=True means:
#   Before giving you a connection from the pool,
#   test it with a lightweight "SELECT 1"
#   If the connection died (DB restart, network blip),
#   discard it and give you a fresh one instead
#   Without this: you get a stale connection → cryptic error ❌
#   With this: always get a working connection ✅
# ─────────────────────────────────────────────────────────────
def create_db_engine():
    """
    Create SQLAlchemy engine with connection pooling.
    Called once at module level — reused everywhere.
    """
    engine = create_engine(
        settings.db_url,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_pre_ping=True,
        echo=False,  # Set True to see all SQL in logs (dev debugging)
    )
    logger.info(f"Database engine created: {settings.db_url_safe}")
    return engine


# ─────────────────────────────────────────────────────────────
# WHY MODULE LEVEL SINGLETON?
# Every file that imports db.py shares this same engine.
# The pool is created once, shared across all imports.
# ─────────────────────────────────────────────────────────────
engine = create_db_engine()


# ─────────────────────────────────────────────────────────────
# CONTEXT MANAGER — THE SAFETY GUARANTEE
#
# WHY @contextmanager?
# This is the "with statement" pattern.
#
# Without context manager:
#   conn = engine.connect()
#   result = conn.execute(query)   ← what if this crashes?
#   conn.close()                   ← this never runs ❌
#   connection leaks forever, pool fills up, pipeline hangs
#
# With context manager:
#   with get_db_connection() as conn:
#       result = conn.execute(query)  ← crashes here?
#   ↓ Python GUARANTEES the finally block runs
#   connection always returned to pool ✅
#
# The try/yield/finally pattern:
#   try     → setup (get connection, begin transaction)
#   yield   → hand control to the "with" block
#   finally → teardown (always runs, even on exception)
# ─────────────────────────────────────────────────────────────
@contextmanager
def get_db_connection():
    """
    SQLAlchemy connection context manager.

    Usage:
        with get_db_connection() as conn:
            result = conn.execute(text("SELECT 1"))

    Guarantees:
        - Connection always returned to pool after use
        - Transaction rolled back on error
        - Never leaks connections
    """
    conn = engine.connect()
    try:
        logger.debug("Database connection acquired from pool")
        yield conn
        conn.commit()
        logger.debug("Transaction committed")
    except Exception as e:
        conn.rollback()
        logger.error(f"Transaction rolled back due to error: {e}")
        raise  # Re-raise so the caller knows something went wrong
    finally:
        conn.close()
        logger.debug("Database connection returned to pool")


# ─────────────────────────────────────────────────────────────
# RAW PSYCOPG2 CONNECTION — FOR BULK OPERATIONS
#
# WHY psycopg2 directly instead of SQLAlchemy?
# SQLAlchemy is an abstraction layer on top of psycopg2.
# Abstraction = convenience but slight overhead.
#
# For bulk loading 100,000+ rows:
#   SQLAlchemy .to_sql()     → builds INSERT statements ❌ slow
#   psycopg2 execute_values → bulk INSERT, much faster ✅
#   psycopg2 COPY           → fastest possible bulk load ✅✅
#
# We use raw psycopg2 specifically in the load stage
# when inserting large datasets into staging/warehouse.
# ─────────────────────────────────────────────────────────────
@contextmanager
def get_raw_connection():
    """
    Raw psycopg2 connection for bulk operations.

    Usage:
        with get_raw_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("COPY ...")

    WHY NOT use the SQLAlchemy pool here?
    psycopg2 COPY command needs a raw psycopg2 connection.
    SQLAlchemy wraps psycopg2 — we bypass the wrapper here
    for maximum bulk insert performance.
    """
    conn = psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )
    try:
        logger.debug("Raw psycopg2 connection opened")
        yield conn
        conn.commit()
        logger.debug("Raw connection transaction committed")
    except Exception as e:
        conn.rollback()
        logger.error(f"Raw connection rolled back due to error: {e}")
        raise
    finally:
        conn.close()
        logger.debug("Raw psycopg2 connection closed")


# ─────────────────────────────────────────────────────────────
# HEALTH CHECK — TEST THE CONNECTION
#
# WHY have a test_connection() function?
# Every pipeline run should verify DB is reachable FIRST
# before doing any real work.
#
# Fail fast principle:
#   Discover the problem at step 1 (connection check)
#   NOT at step 8 (halfway through loading data)
#   A half-loaded pipeline is worse than a pipeline
#   that never started — data integrity risk ❌
# ─────────────────────────────────────────────────────────────
def test_connection() -> bool:
    """
    Verify database is reachable and responding.
    Returns True if healthy, False if not.

    Use this at the start of every pipeline run.
    """
    try:
        with get_db_connection() as conn:
            result = conn.execute(text("SELECT 1 AS health_check"))
            row = result.fetchone()
            if row[0] == 1:
                logger.info(
                    f"Database connection healthy: {settings.db_url_safe}"
                )
                return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False