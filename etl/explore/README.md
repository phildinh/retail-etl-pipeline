# etl/explore/ — Data Discovery

This folder contains one-time exploration tools used to understand
the API data BEFORE writing any pipeline or schema code.

This is not production code. These files run once manually
to answer the question: "what does the data actually look like?"

---

## Why Data Discovery Comes Before Schema Design

A common mistake is designing the schema based on assumptions,
then discovering the real data does not match.

The correct order is:

    Step 1 — Pull sample data from the API
    Step 2 — Profile it (types, nulls, nested fields)
    Step 3 — Ask business questions based on what we found
    Step 4 — Design schema to serve those questions
    Step 5 — Build the pipeline

Discovery is 30 minutes of work that prevents weeks of rework.

---

## Files In This Folder

    explore/
    ├── api_exploration.ipynb   → interactive notebook (run this)
    ├── samples/                → raw JSON saved from API (gitignored)
    └── README.md               → this file

---

## How To Run The Notebook

Step 1 — Make sure venv is activated:

    venv\Scripts\activate

Step 2 — Open the notebook in VS Code:

    Click api_exploration.ipynb in the sidebar
    Select kernel: venv (top right corner)

Step 3 — Run cells one at a time with Shift+Enter

    Read each output before moving to the next cell
    The goal is understanding, not just running

---

## Notebook Cell Guide

    Cell 1  — Setup and imports
              Verifies correct virtual environment
              Loads requests, pandas, json

    Cell 2  — Pull raw products
              Hits /products endpoint
              Shows first record as raw JSON
              Question to answer: what fields exist? anything nested?

    Cell 3  — Products into DataFrame
              Converts raw JSON to pandas DataFrame
              Uses json_normalize to flatten nested dicts
              Question to answer: what did json_normalize create?

    Cell 4  — Profile products
              Shows data types, null counts, unique value counts
              Question to answer: what SQL types do we need?
                                  which columns need NOT NULL?
                                  which columns are categories?

    Cell 5  — Pull raw carts
              Hits /carts endpoint
              Shows first record as raw JSON
              Question to answer: how are products stored inside a cart?

    Cell 6  — Carts into DataFrame
              Converts raw JSON to pandas DataFrame
              Note: products column stays as a list (not flattened)
              Question to answer: what columns does a cart have?

    Cell 7  — Profile carts + explore nested products
              Shows types, nulls, how many products per cart
              Key finding: products is a nested list
              Must be EXPLODED in staging (one row per product per cart)

    Cell 8  — Pull raw users
              Hits /users endpoint
              Shows first record as raw JSON
              Question to answer: how deep does the nesting go?

    Cell 9  — Users into DataFrame
              json_normalize flattens ALL nested levels:
              name.firstname, name.lastname
              address.street, address.city, address.zipcode
              address.geolocation.lat, address.geolocation.long

    Cell 10 — Profile users
              Shows types, nulls, unique counts
              Key finding: email and username are unique identifiers
              Key decision: password column is NEVER stored

    Cell 11 — Discovery summary
              Documents all findings in one place
              This becomes the input to schema design decisions

---

## Key Findings From Discovery

### Products (/products)

    Records:  20
    Columns:  id, title, price, description, category, image,
              rating.rate, rating.count

    Nested:   rating object → flattens to two columns
              rating.rate  (float)
              rating.count (integer)

    Nulls:    none found → all columns can be NOT NULL

    Notable:  category has only 4 unique values → good for grouping
              price is float → use NUMERIC(10,2) not FLOAT in SQL
              id is unique → natural key from source system

    SQL type decisions:
              id            INTEGER
              title         TEXT NOT NULL
              price         NUMERIC(10,2) NOT NULL
              category      TEXT NOT NULL
              description   TEXT
              image         TEXT
              rating.rate   NUMERIC(3,1)
              rating.count  INTEGER

### Carts (/carts)

    Records:  7
    Columns:  id, userId, date, products

    Nested:   products is a LIST of dicts
              each item: { productId, quantity }
              one cart has multiple products

    Key decision:
              Cannot store products list as a flat column
              Must EXPLODE in staging:
              → one row per product per cart
              → staging.carts has columns:
                 cart_id, user_id, product_id, quantity, date

    Why explode?
              Business question: how many times was product X ordered?
              → impossible if products stay as a list ❌
              → easy after exploding to one row per product ✅

    SQL type decisions:
              id            INTEGER
              userId        INTEGER
              date          DATE
              productId     INTEGER (after exploding)
              quantity      INTEGER (after exploding)

### Users (/users)

    Records:  10
    Columns:  id, email, username, password,
              name.firstname, name.lastname,
              address.street, address.city, address.zipcode,
              address.geolocation.lat, address.geolocation.long,
              phone

    Nested:   name → firstname + lastname
              address → street, city, zipcode, geolocation
              geolocation → lat + long (nested inside address)

    Nulls:    none found

    Notable:  email is unique → business identifier
              username is unique → business identifier
              password EXISTS but we never store it (security rule)
              geolocation needs high precision → NUMERIC(9,6)

    SQL type decisions:
              id              INTEGER
              email           TEXT NOT NULL (unique identifier)
              username        TEXT NOT NULL
              password        NEVER STORED ← security decision
              name.firstname  TEXT
              name.lastname   TEXT
              address.street  TEXT
              address.city    TEXT
              address.zipcode TEXT
              address.lat     NUMERIC(9,6)
              address.lng     NUMERIC(9,6)
              phone           TEXT

---

## Schema Decisions Driven By This Discovery

### Raw Layer (store everything, touch nothing)

    raw.products → JSONB column (exact API response)
    raw.carts    → JSONB column (exact API response)
    raw.users    → JSONB column (exact API response)

    Why JSONB?
    Preserves the exact API response forever.
    If the API adds new fields tomorrow, they are captured.
    If something breaks in staging, raw data is the safety net.

### Staging Layer (flat, typed, cleaned)

    staging.products → flat columns, rating flattened
    staging.carts    → exploded (one row per product per cart)
    staging.users    → flat columns, all nesting resolved
                       password column excluded entirely

### Warehouse Layer (business-ready, history preserved)

    warehouse.dim_products → SCD Type 2
                             price and category can change over time
                             we need to track those changes

    warehouse.dim_users    → SCD Type 2
                             address can change over time
                             we need point-in-time accuracy

    warehouse.fact_orders  → one row per product per cart
                             references dim_products and dim_users
                             stores quantity and unit_price at time of order

---

## What Is NOT In This Folder

    Pipeline code    → lives in etl/extract/, etl/transform/, etl/load/
    Schema SQL       → lives in sql/ddl/
    Production code  → never in explore/

---

## Important Notes

    1. samples/ folder is gitignored
       Raw JSON samples are saved locally for reference
       They are never committed to GitHub
       They may contain personally identifiable information

    2. This notebook runs against the real API
       It needs an internet connection
       It needs API_BASE_URL set in .env.dev

    3. Run the notebook again anytime the API changes
       Re-profile to catch new fields or structural changes
       Update this README with new findings

---
