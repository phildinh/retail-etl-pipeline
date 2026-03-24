# Retail ETL Pipeline
### Production-grade ETL pipeline — Python · PostgreSQL · Apache Airflow · Docker · GitHub Actions

---

## What This Project Does

Retail businesses generate thousands of transactions daily across products, users, and orders — but raw API data is messy, nested, and impossible to analyse directly.

This project builds a **production-grade ETL pipeline** that:
- Extracts retail data from FakeStoreAPI automatically
- Cleans and flattens nested JSON into structured, typed tables
- Loads data into a star schema warehouse with full historical tracking via SCD Type 2
- Runs daily on a schedule via Apache Airflow
- Tests every code change automatically via GitHub Actions CI/CD

The result: a fully automated pipeline that delivers clean, reliable retail data every day — with dimension history preserved so analysts can answer questions like *"what did this product cost in January?"*

---

## Live Stats

| Metric | Value |
|---|---|
| Data sources | 3 endpoints (products, carts, users) |
| Records processed | 37 raw records → 14 fact rows |
| Total tests | 43 (39 unit + 4 integration) ✅ |
| CI/CD | Automated on every push |
| Warehouse pattern | Star schema with SCD Type 2 |
| Environments | Dev + Test + Prod |

---

## Tech Stack

| Layer | Tool | Why I chose it |
|---|---|---|
| Language | Python 3.11 | Industry standard for data engineering |
| Database | PostgreSQL 15 | Reliable, production-grade open-source warehouse |
| ORM / connections | SQLAlchemy 2.0 | Connection pooling, context managers, no raw connection leaks |
| Config validation | Pydantic v2 | Fail-fast at startup if any env variable is missing |
| Retry logic | Tenacity | Automatic API retries with exponential backoff |
| Orchestration | Apache Airflow | Industry-standard scheduler with visual DAG monitoring |
| Containerisation | Docker + Compose | Runs identically on any machine — no "works on my laptop" |
| CI/CD | GitHub Actions | Automated unit tests on every push |
| Testing | pytest | 43 tests covering transforms, loads, and integration |

---

## Architecture Overview
```
┌─────────────────────────────────────────────────┐
│               DATA SOURCE                        │
│         FakeStoreAPI (REST API)                  │
│   /products    /carts    /users                  │
└─────────────────┬───────────────────────────────┘
                  │ Python extraction scripts
                  ▼
┌─────────────────────────────────────────────────┐
│              RAW LAYER                           │
│   PostgreSQL raw schema                          │
│   Stores original API responses as JSONB         │
│   Nothing is modified — full audit trail         │
└─────────────────┬───────────────────────────────┘
                  │ Python transform scripts
                  ▼
┌─────────────────────────────────────────────────┐
│            STAGING LAYER                         │
│   PostgreSQL staging schema                      │
│   Flattened, typed, cleaned                      │
│   Nested dicts exploded to rows                  │
└─────────────────┬───────────────────────────────┘
                  │ Python load scripts
                  ▼
┌─────────────────────────────────────────────────┐
│            WAREHOUSE LAYER                       │
│   PostgreSQL warehouse schema — star schema      │
│   dim_products (SCD Type 2)                      │
│   dim_users    (SCD Type 2)                      │
│   fact_orders  (append only)                     │
└─────────────────────────────────────────────────┘
                  │
        Orchestrated daily by Airflow
        Tested on every push by GitHub Actions
```

---

## Data Model

### Three-Layer Architecture (Raw → Staging → Warehouse)

**Why three layers?** Raw data is stored untouched as JSONB so nothing is ever lost and the original API response is always recoverable. Staging cleans and flattens it into a usable structure. The warehouse builds a star schema optimised for analytics queries.

| Layer | Schema | Tables | Purpose |
|---|---|---|---|
| Raw | `raw` | `products`, `carts`, `users` | Original JSONB responses, unchanged |
| Staging | `staging` | `products`, `carts`, `users` | Flat, typed, cleaned |
| Warehouse | `warehouse` | `dim_products`, `dim_users`, `fact_orders` | Star schema, business-ready |

### Star Schema
```
dim_products (SCD Type 2)    dim_users (SCD Type 2)
         ↘                   ↙
              fact_orders
           (append only)
```

`fact_orders` joins to both dimensions via surrogate keys, enabling queries like:
*"What was the total revenue by product category this week?"*

---

## Engineering Decisions

### SCD Type 2 for Dimensions
Dimensions track the full history of changes — not just the current state.
```
Product price changes: $109.95 → $89.95

Old row: valid_to = today,  is_current = FALSE  ← preserved forever
New row: valid_from = today, is_current = TRUE  ← current version

Business question now answerable:
"What did this product cost when the order was placed in January?" ✅
```

Without SCD Type 2, a price update would silently overwrite history — making historical revenue analysis impossible.

### Append-Only for Facts
Orders are immutable historical events — they happened and cannot be changed.
```
"User 1 bought 4 units of product 1 on March 2"
This fact never changes → append only, no updates ever ✅
```

### Connection Pooling
SQLAlchemy `QueuePool` with `pool_size=5`, `max_overflow=10`, `pool_pre_ping=True`. Reuses existing connections instead of opening and closing a new database connection per query — critical for performance in a pipeline processing many inserts.

### Fail Fast on Config
Pydantic validates all environment variables at startup. If `DB_PASSWORD` or `API_BASE_URL` is missing, the pipeline crashes immediately with a clear error — rather than failing silently halfway through a load.

### Password Never Stored
User passwords from the API are excluded at the transform layer and never reach staging or warehouse. Verified by automated tests — not just convention.

### Parallel Transforms in Airflow
Transform tasks run in parallel after extraction, reducing total pipeline runtime:
```
extract
    ↓
transform_products  transform_carts  transform_users  ← parallel
    ↓                                      ↓
load_dim_products                    load_dim_users   ← parallel
         ↘                           ↙
           load_fact_orders
```

---

## Testing Strategy

43 tests across unit and integration layers:

| File | Tests | What It Covers |
|---|---|---|
| `test_products_transform.py` | 9 | Flatten nested rating dict, rename columns, type casting |
| `test_carts_transform.py` | 13 | Explode products list, date parsing, key validation |
| `test_users_transform.py` | 17 | Flatten name/address, password excluded, bad geolocation handling |
| `test_pipeline_integration.py` | 4 | Full read/write cycle, no duplicate rows on re-run |
| **Total** | **43** | |

Unit tests require no database — run in under 1 second. Integration tests use a dedicated `retail_etl_test` database to verify real read/write behaviour.

---

## CI/CD Pipeline

Every push to GitHub triggers this workflow automatically:
```
Code pushed
    ↓
GitHub Actions spins up Ubuntu runner
    ↓
Installs Python 3.11 + dependencies
    ↓
Runs pytest tests/unit/ (39 tests, no database needed)
    ↓
✅ Pass → merge allowed
❌ Fail → merge blocked
```

Integration tests run locally against the test database before merging.

---

## Airflow DAG

**Schedule:** Daily at 2:00am
```
extract
    ↓
transform_products  transform_carts  transform_users  (parallel)
    ↓                                      ↓
load_dim_products                    load_dim_users   (parallel)
         ↘                           ↙
           load_fact_orders
```

Retries: 2 attempts with 5 minute delay — handles transient API failures gracefully.

---

## Project Structure
```
retail_etl/
├── etl/
│   ├── extract/
│   │   ├── api_client.py              ← HTTP client with tenacity retry
│   │   └── fakestore_extractor.py     ← pulls products, carts, users
│   ├── transform/
│   │   ├── products_transform.py      ← flatten nested rating dict
│   │   ├── carts_transform.py         ← explode products list to rows
│   │   └── users_transform.py         ← flatten name + address
│   ├── load/
│   │   ├── products_load.py           ← SCD Type 2 → dim_products
│   │   ├── users_load.py              ← SCD Type 2 → dim_users
│   │   └── orders_load.py             ← append only → fact_orders
│   └── utils/
│       ├── config.py                  ← pydantic settings, fail fast
│       ├── db.py                      ← connection pool + context manager
│       └── logger.py                  ← structured logging
├── dags/
│   └── retail_etl_dag.py              ← Airflow DAG, daily at 2am
├── sql/ddl/
│   ├── create_schemas.sql             ← raw, staging, warehouse
│   └── create_tables.sql              ← all 9 tables
├── tests/
│   ├── unit/                          ← 39 tests, no database needed
│   └── integration/                   ← 4 tests, uses test database
├── run_pipeline.py                    ← single entry point
├── Dockerfile
├── docker-compose.yml
└── .github/workflows/ci.yml           ← GitHub Actions CI
```

---

## Local Setup

### Prerequisites
- Python 3.11+
- PostgreSQL 15
- Docker Desktop
- Git

### Steps
```bash
# 1. Clone the repo
git clone https://github.com/phildinh/retail-etl-pipeline.git
cd retail-etl-pipeline

# 2. Create and activate virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure credentials
cp .env.example .env.dev
# Fill in your database credentials in .env.dev

# 5. Set up databases
createdb retail_etl_dev
createdb retail_etl_test

# 6. Run DDL scripts
psql -U etl_user -d retail_etl_dev  -f sql/ddl/create_schemas.sql
psql -U etl_user -d retail_etl_dev  -f sql/ddl/create_tables.sql
psql -U etl_user -d retail_etl_test -f sql/ddl/create_schemas.sql
psql -U etl_user -d retail_etl_test -f sql/ddl/create_tables.sql

# 7. Run the pipeline
python run_pipeline.py
```

### Expected Output
```
═══════════════════════════════════════════════════════
  RETAIL ETL PIPELINE STARTED
═══════════════════════════════════════════════════════
  ✅  Extract — FakeStoreAPI      (products=20, carts=7, users=10)
  ✅  Transform — Products
  ✅  Transform — Carts
  ✅  Transform — Users
  ✅  Load — dim_products (SCD2)  (inserted=20)
  ✅  Load — dim_users (SCD2)     (inserted=10)
  ✅  Load — fact_orders          (inserted=14)
═══════════════════════════════════════════════════════
  Total time: 2.7 seconds
═══════════════════════════════════════════════════════
```

---

## Run With Docker
```bash
# Build and start everything (PostgreSQL + pipeline)
docker compose up

# Connect to the database
# host: localhost | port: 5433 | db: retail_etl_dev
# user: etl_user  | password: 2011

# Stop everything
docker compose down
```

---

## Run Tests
```bash
# Unit tests — no database needed, runs in ~1 second
pytest tests/unit/ -v

# Integration tests — requires retail_etl_test database
$env:ENV="test"; pytest tests/integration/ -v   # Windows
ENV=test pytest tests/integration/ -v           # Mac/Linux
```

---

## Analytics Query

After the pipeline runs, query the warehouse directly:
```sql
SELECT
    fo.order_date,
    dp.category,
    COUNT(DISTINCT fo.cart_source_id)  AS total_orders,
    SUM(fo.quantity)                   AS total_units,
    ROUND(SUM(fo.total_price), 2)      AS total_revenue
FROM warehouse.fact_orders fo
JOIN warehouse.dim_products dp ON fo.product_sk = dp.product_sk
JOIN warehouse.dim_users    du ON fo.user_sk     = du.user_sk
GROUP BY fo.order_date, dp.category
ORDER BY total_revenue DESC;
```

---

## Environment Variables

| Variable | Description | Example |
|---|---|---|
| `ENV` | Environment name | `dev` / `test` / `prod` |
| `DB_SERVER` | Database host | `localhost` |
| `DB_PORT` | Database port | `5432` |
| `DB_NAME` | Database name | `retail_etl_dev` |
| `DB_USER` | Database user | `etl_user` |
| `DB_PASSWORD` | Database password | — |
| `API_BASE_URL` | FakeStoreAPI base URL | `https://fakestoreapi.com` |

Copy `.env.example` and fill in your values. Never commit `.env.*` files.

---

## Author

**Phil Dinh**
Data Analyst transitioning to Data Engineer — Sydney, Australia

[GitHub](https://github.com/phildinh) · [LinkedIn](https://linkedin.com/in/YOUR_PROFILE)
