# etl/extract/ — Extract Layer

This folder contains the extract layer of the ETL pipeline.
Its only job is to pull raw data from FakeStoreAPI and save
it exactly as received into the raw schema in PostgreSQL.

Extract does NOT clean data.
Extract does NOT transform data.
Extract ONLY pulls and saves raw.

---

## Why Extract Is Its Own Layer

The ETL pipeline is split into three layers:

    Extract   → pull data from source (this folder)
    Transform → clean and reshape (etl/transform/)
    Load      → write to warehouse (etl/load/)

Keeping them separate means:
- Each layer has one job and does it well
- A bug in transform never corrupts raw data
- You can reprocess from raw anytime without hitting the API again
- Each layer can be tested independently

---

## Files In This Folder

    extract/
    ├── api_client.py           → generic HTTP client (works with any API)
    ├── fakestore_extractor.py  → FakeStoreAPI specific extract logic
    └── README.md               → this file

---

## api_client.py

### What It Does
Handles all HTTP communication with any API.
Knows nothing about FakeStoreAPI specifically.
Knows nothing about our database.
Only responsibility: make request → retry if needed → return data.

### Key Concepts

**requests.Session**
Reuses the same network connection across all requests.
Without Session, each call opens and closes a new connection (slow).
With Session, one connection stays open and is reused (fast).

**Retry with exponential backoff (tenacity)**
If a request fails due to network issues, it retries automatically:

    Attempt 1 fails → wait 1 second  → retry
    Attempt 2 fails → wait 2 seconds → retry
    Attempt 3 fails → wait 4 seconds → retry
    Attempt 4 fails → stop, raise clear error

Only retries on ConnectionError and Timeout.
Does NOT retry on HTTP 404 or 400 (those are permanent errors).

**Context manager support**
APIClient supports the `with` keyword:

    with APIClient(base_url="...") as client:
        data = client.get("/products")

Session is automatically closed when the block exits,
even if an exception occurs. Prevents connection leaks.

**Singleton instance**
A shared instance is created at module level:

    api_client = APIClient(base_url=settings.api_base_url)

Import this instance anywhere in the codebase:

    from etl.extract.api_client import api_client

One shared client, one session, consistent settings.

---

## fakestore_extractor.py

### What It Does
Knows specifically about FakeStoreAPI endpoints.
Calls api_client to fetch data.
Saves raw JSON responses to raw schema in PostgreSQL.
Does not clean or transform anything.

### Endpoints

    /products  → saved to raw.products
    /carts     → saved to raw.carts
    /users     → saved to raw.users

### Key Concepts

**Why save raw JSON to database?**
Each API record is stored exactly as received in a JSONB column.

    API response:  {"id": 1, "title": "Backpack", "price": 109.95}
    Stored as:     JSONB in raw.products.raw_data column

Benefits:
- API adds new field tomorrow? Captured automatically
- Transform bug corrupts staging? Replay from raw
- Need audit trail? Raw has every original record

**Why JSONB not TEXT?**
JSONB is stored as binary in PostgreSQL:
- Faster to query than plain TEXT
- Can be indexed
- Supports JSON operators for querying specific fields

**Why UTC timestamps?**
All loaded_at timestamps are stored in UTC:
- UTC has no daylight saving time
- No ambiguity when clocks change
- Convert to local time only when displaying to users

**Why one record per INSERT?**
Each record is inserted inside a single transaction.
If any record fails the entire batch rolls back.
Either all records are saved or none are. No partial saves.

**Observability via return counts**
extract_all() returns a dict of record counts:

    {"products": 20, "carts": 7, "users": 10}

Caller can check counts to detect problems without reading logs.
Zero records returned = something is wrong.

---

## How To Run

Always run as a module from the project root.
Never run the file directly with python path/to/file.py.

    # Activate venv first
    venv\Scripts\activate

    # Run full extraction
    python -m etl.extract.fakestore_extractor

### Why -m flag?

    # Wrong — Python loses sight of project root
    python etl/extract/fakestore_extractor.py  ❌

    # Correct — Python starts from project root
    python -m etl.extract.fakestore_extractor  ✅

The -m flag tells Python to run the file as a module
starting from the current directory (project root).
All imports resolve correctly.

---

## Expected Output

    INFO | etl.utils.db         | Database engine created ...
    INFO | etl.extract...       | Extracting products from API
    INFO | etl.extract...       | Extracted 20 products records
    INFO | etl.extract...       | Saved 20 records to raw.products
    INFO | etl.extract...       | Extracting carts from API
    INFO | etl.extract...       | Extracted 7 carts records
    INFO | etl.extract...       | Saved 7 records to raw.carts
    INFO | etl.extract...       | Extracting users from API
    INFO | etl.extract...       | Extracted 10 users records
    INFO | etl.extract...       | Saved 10 records to raw.users

    ════════════════════════════════════════
    EXTRACTION RESULTS
    ════════════════════════════════════════
      products        20 records saved to raw.products
      carts            7 records saved to raw.carts
      users           10 records saved to raw.users
    ════════════════════════════════════════

---

## Verify In Database

Run this query in SQLTools after extraction:
```sql