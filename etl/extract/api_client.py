# etl/extract/api_client.py
# ═══════════════════════════════════════════════════════════
# PURPOSE: Generic HTTP client for any API
#
# SINGLE RESPONSIBILITY:
#   This file ONLY handles HTTP communication
#   It knows nothing about FakeStoreAPI specifically
#   It knows nothing about our database
#   It ONLY does: make request → retry if needed → return data
#
# WHY generic?
#   Today we use FakeStoreAPI
#   Tomorrow we might switch to Shopify or another API
#   This file works for ANY API without changing ✅
# ═══════════════════════════════════════════════════════════

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import logging
from etl.utils.config import settings
from etl.utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────
# RETRY CONFIGURATION
#
# We use tenacity library to handle retries automatically
# Instead of writing try/except loops manually:
#
# BAD (manual retry — messy, error prone):
#   for attempt in range(3):
#       try:
#           response = requests.get(url)
#           break
#       except:
#           time.sleep(2 ** attempt)
#
# GOOD (tenacity — clean, declarative):
#   @retry(stop=stop_after_attempt(3))
#   def get(url): ...
#   → tenacity handles all the retry logic for us ✅
#
# HOW EXPONENTIAL BACKOFF WORKS:
#   multiplier=1, min=1, max=10 means:
#   Attempt 1 fails → wait 1 second
#   Attempt 2 fails → wait 2 seconds
#   Attempt 3 fails → wait 4 seconds
#   Attempt 4 fails → stop, raise error
#   Each wait doubles (exponential) up to max of 10 seconds
#
# WHY only retry on ConnectionError and Timeout?
#   These are TEMPORARY problems (network blip, server busy)
#   → worth retrying ✅
#
#   HTTP 404 Not Found = endpoint is wrong
#   → retrying 3 times won't fix a wrong URL ❌
#   → fail immediately instead
# ─────────────────────────────────────────────────────────
RETRY_CONFIG = dict(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(
        (requests.ConnectionError, requests.Timeout)
    ),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


class APIClient:
    # ─────────────────────────────────────────────────────
    # WHY a class instead of just functions?
    #
    # A class lets us store shared state:
    #   base_url    → set once, used in every request
    #   timeout     → set once, applied to every request
    #   session     → one connection pool, reused every request
    #
    # Without class (functions only):
    #   def get(url, base_url, timeout, session, ...):
    #   → pass same arguments to every function call ❌
    #   → easy to forget one, easy to make mistakes
    #
    # With class:
    #   client = APIClient(base_url="https://fakestoreapi.com")
    #   client.get("/products")  → clean, no repeated args ✅
    #   client.get("/users")     → same settings automatically
    # ─────────────────────────────────────────────────────

    def __init__(self, base_url: str, timeout: int = 10):
        """
        Initialise the API client.

        Args:
            base_url: root URL for all requests
                      e.g. "https://fakestoreapi.com"
            timeout:  seconds to wait before giving up
                      default 10 seconds

        WHY store a requests.Session?
        requests.get() opens a NEW connection every call:
            Call 1: open connection → get data → close connection
            Call 2: open connection → get data → close connection
            → slow, wasteful ❌

        requests.Session() reuses the same connection:
            Open connection once
            Call 1: get data (connection stays open)
            Call 2: get data (same connection) ✅
            → faster, more efficient
            → same concept as SQLAlchemy connection pooling
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        logger.info(f"APIClient initialised | base_url={self.base_url}")

    @retry(**RETRY_CONFIG)
    def get(self, endpoint: str, params: dict = None) -> list | dict:
        """
        Make a GET request to the API.

        Args:
            endpoint: path to append to base_url
                      e.g. "/products"
            params:   optional query parameters
                      e.g. {"limit": 10}

        Returns:
            Parsed JSON response (list or dict)

        Raises:
            requests.HTTPError:  4xx or 5xx response
            requests.Timeout:    server took too long
            requests.ConnectionError: could not reach server

        WHY @retry decorator here?
        The decorator wraps this entire function
        If it raises ConnectionError or Timeout
        → tenacity automatically retries with backoff
        → caller never needs to know retries happened
        → clean separation of concerns ✅

        WHY raise_for_status()?
        requests does NOT raise errors for 4xx/5xx by default:
            response = requests.get("...404_url...")
            → no error raised ❌
            → response.status_code == 404 silently
        raise_for_status() converts bad status codes to exceptions:
            → 404 raises HTTPError immediately ✅
            → fail fast, don't process empty responses
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        logger.debug(f"GET {url} | params={params}")

        response = self.session.get(
            url,
            params=params,
            timeout=self.timeout,
        )

        # Raises HTTPError for 4xx and 5xx responses
        response.raise_for_status()

        data = response.json()

        logger.debug(
            f"GET {url} | "
            f"status={response.status_code} | "
            f"records={len(data) if isinstance(data, list) else 1}"
        )

        return data

    def close(self):
        """
        Close the underlying connection session.

        WHY close the session?
        Session holds an open network connection
        If we never close it → connection leak
        Same concept as closing a database connection

        In production this is called automatically
        via context manager (see __enter__ / __exit__ below)
        """
        self.session.close()
        logger.debug("APIClient session closed")

    def __enter__(self):
        """
        WHY __enter__ and __exit__?
        These two methods make APIClient a context manager
        So we can use it with the 'with' keyword:

        with APIClient(base_url="...") as client:
            data = client.get("/products")
        → session automatically closed when block exits ✅
        → even if an exception occurs ✅

        Without context manager:
        client = APIClient(base_url="...")
        data = client.get("/products")
        client.close()  ← easy to forget ❌
                          never called if exception occurs ❌
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Called automatically when 'with' block exits.
        Closes the session whether block succeeded or failed.

        exc_type, exc_val, exc_tb = exception info if one occurred
        We don't suppress exceptions → return None (default)
        → exception still propagates to caller
        """
        self.close()


# ─────────────────────────────────────────────────────────
# MODULE LEVEL CLIENT INSTANCE
#
# WHY create a shared instance here?
# Same singleton pattern we used in db.py and config.py
#
# Without singleton:
#   Every file that needs API access creates its own client:
#   client1 = APIClient(base_url=settings.api_base_url)
#   client2 = APIClient(base_url=settings.api_base_url)
#   → multiple sessions open ❌
#   → wasteful, inconsistent settings ❌
#
# With singleton:
#   from etl.extract.api_client import api_client
#   data = api_client.get("/products")
#   → one shared client across entire codebase ✅
#   → one session, consistent settings ✅
# ─────────────────────────────────────────────────────────
api_client = APIClient(base_url=settings.api_base_url)