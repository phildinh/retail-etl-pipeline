# etl/utils/logger.py

import logging
import sys
from pathlib import Path


# ─────────────────────────────────────────────────────────────
# WHY TWO HANDLERS?
#
# Handler = a destination where log messages are sent
#
# StreamHandler → prints to terminal (console)
#   Good for: seeing logs in real time while developing
#   Good for: Docker containers (logs go to stdout)
#
# FileHandler → writes to a log file
#   Good for: keeping history of pipeline runs
#   Good for: debugging issues after the fact
#   Good for: audit trail ("what ran and when?")
#
# We want BOTH:
#   Developer watches terminal in real time ✅
#   Logs saved to file for later investigation ✅
# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────
# WHY THIS LOG FORMAT?
#
# %(asctime)s    → timestamp: when did this happen?
# %(levelname)s  → severity: INFO, ERROR, DEBUG etc
# %(name)s       → which module: etl.utils.db etc
# %(message)s    → the actual message
#
# Example output:
# 2024-01-15 02:13:45 | INFO     | etl.utils.db | Engine created
# ─────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_log_level(env: str) -> int:
    """
    Return appropriate log level based on environment.

    WHY DIFFERENT LEVELS PER ENVIRONMENT?
    dev  → DEBUG: see everything, helpful for development
    test → DEBUG: see everything, helpful for test debugging
    prod → INFO: only meaningful events, reduce noise

    In production, DEBUG logs would:
    - Flood your log files with noise
    - Expose sensitive query details
    - Make it hard to spot real problems
    """
    level_map = {
        "dev": logging.DEBUG,
        "test": logging.DEBUG,
        "prod": logging.INFO,
    }
    return level_map.get(env.lower(), logging.INFO)


def setup_logger(env: str = "dev") -> None:
    """
    Configure the root logger once at application startup.

    WHY CONFIGURE ROOT LOGGER?
    Python logging works as a hierarchy:
        root logger
        └── etl (our package)
            ├── etl.utils.config
            ├── etl.utils.db
            └── etl.extract.api_client

    Configuring the root logger means ALL loggers
    in our entire application inherit these settings.
    One setup call → everything is configured ✅

    WHY CALL THIS ONLY ONCE?
    Calling it multiple times adds duplicate handlers.
    You'd see every log message printed twice, three times etc.
    We guard against this with hasHandlers() check below.
    """
    root_logger = logging.getLogger()

    # Guard — only set up if not already configured
    # WHY? If two files both call setup_logger(),
    # without this guard you get duplicate log lines
    if root_logger.hasHandlers():
        return

    log_level = get_log_level(env)
    root_logger.setLevel(log_level)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # ── Handler 1: Console (terminal output) ──────────────────
    # WHY sys.stdout instead of sys.stderr?
    # stderr is for errors only — mixing INFO logs into stderr
    # makes it hard to separate normal output from errors.
    # stdout = all our logs
    # stderr = only Python crashes/tracebacks
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # ── Handler 2: File (persistent log history) ──────────────
    # WHY logs/ folder?
    # Already exists in our project structure from Stage 1.
    # Centralised location for all pipeline run history.
    #
    # WHY retail_etl.log as filename?
    # Single log file for the whole application.
    # In production you might rotate logs daily:
    # retail_etl_2024-01-15.log etc (TimedRotatingFileHandler)
    # For now, single file keeps it simple.
    log_dir = Path(__file__).parent.parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)  # Create logs/ if it doesn't exist

    file_handler = logging.FileHandler(
        log_dir / "retail_etl.log",
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Log the first message confirming setup worked
    logger = logging.getLogger(__name__)
    logger.info(f"Logger initialised | env={env} | level={logging.getLevelName(log_level)}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a named logger for any module in the pipeline.

    WHY name parameter?
    Pass __name__ from any file:
        logger = get_logger(__name__)

    __name__ automatically equals the module path:
        etl.utils.db
        etl.extract.api_client
        etl.load.full_loader

    Every log line then shows exactly which file it came from.

    Usage in any file:
        from etl.utils.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Starting extraction")
        logger.error("Connection failed")
    """
    return logging.getLogger(name)