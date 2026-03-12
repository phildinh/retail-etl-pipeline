# etl/transform/ — Transform Layer

This folder contains the transform layer of the ETL pipeline.
Its only job is to read raw JSONB data from the raw schema,
clean and reshape it, then write flat typed rows to staging.

Transform does NOT pull from the API.
Transform does NOT write to the warehouse.
Transform ONLY reads from raw and writes to staging.

---

## Why Transform Is Its Own Layer
```
raw schema     → exact API response, never touched
                 ↓
transform      → clean, flatten, type cast, explode
                 ↓
staging schema → flat, typed, ready for warehouse
```

Keeping transform separate means:
- Raw data is never modified or lost
- Transform logic can be fixed and replayed from raw
- Each transform can be tested without an API connection
- Staging is always a clean fresh copy on every run

---

## Files In This Folder
```
transform/
├── products_transform.py  → flatten rating, clean types
├── carts_transform.py     → explode products nested list
├── users_transform.py     → flatten name + address,
│                            exclude password
└── README.md              → this file
```

---

## How Each Transform Works

Every transform file follows the same pattern:
```
Step 1 — Read raw JSONB from raw schema
Step 2 — Parse and transform each record in memory
Step 3 — TRUNCATE staging table
Step 4 — INSERT transformed records into staging
```

### Why truncate before insert?

Staging is a temporary workspace not a history store.
Every pipeline run wipes staging clean and reloads it.
```
Run 1: truncate → insert 20 products ✅
Run 2: truncate → insert 20 products ✅ (fresh copy)

Without truncate:
Run 1: insert 20 products  → 20 rows
Run 2: insert 20 products  → 40 rows ❌ duplicates
Run 3: insert 20 products  → 60 rows ❌ keeps growing
```

History is preserved in the warehouse layer using SCD Type 2.
Staging does not need to keep history.

### Why transform in memory before truncating?
```
Wrong order:
truncate staging → transform fails → staging is empty ❌
pipeline is broken, analysts see no data

Correct order:
read raw → transform in memory → truncate → insert
if insert fails → staging empty but raw is intact ✅
re-run pipeline → raw replays into staging cleanly
```

---

## products_transform.py

### What It Does

Reads from raw.products, writes to staging.products.

### Key Transformations

**Flatten rating nested dict**
```
Raw:     "rating": {"rate": 3.9, "count": 120}
Staging: rating_rate  = 3.9
         rating_count = 120
```

**Rename image to image_url**
```
Raw:     "image": "https://..."
Staging: image_url = "https://..."

Why rename?
"image" is ambiguous — could be binary data, filename, URL
"image_url" is unambiguous ✅
```

**Rename id to source_id**
```
Raw:     "id": 1
Staging: source_id = 1

Why rename?
Makes clear this id came from the source system (API)
Not a key we generated ourselves
```

**price cast to float**
```
API sends float → Python float → PostgreSQL NUMERIC(10,2)
NUMERIC(10,2) is used instead of FLOAT for money:
  FLOAT:         109.95 + 0.10 = 110.05000000000001 ❌
  NUMERIC(10,2): 109.95 + 0.10 = 110.05             ✅
```

### Run
```
python -m etl.transform.products_transform
```

### Expected Result
```
Records written to staging.products: 20
```

---

## carts_transform.py

### What It Does

Reads from raw.carts, explodes products list,
writes to staging.carts.

### Key Transformation — Exploding Nested List

This is the most important transform in the pipeline.

**Raw structure:**
```json
{
    "id": 1,
    "userId": 1,
    "date": "2020-03-02T00:00:00.000Z",
    "products": [
        {"productId": 1, "quantity": 4},
        {"productId": 2, "quantity": 1},
        {"productId": 3, "quantity": 6}
    ]
}
```

**Staging structure (3 rows from 1 cart):**
```
cart_id | user_id | product_id | quantity | cart_date
1       | 1       | 1          | 4        | 2020-03-02
1       | 1       | 2          | 1        | 2020-03-02
1       | 1       | 3          | 6        | 2020-03-02
```

**Why explode?**
```
Business question: how many times was product 1 ordered?

With nested list:
SELECT ??? FROM staging.carts  ← impossible ❌

After exploding:
SELECT COUNT(*) FROM staging.carts
WHERE product_source_id = 1    ← easy ✅
```

**Date parsing**
```
API sends:  "2020-03-02T00:00:00.000Z"  (ISO string)
Staging:    2020-03-02                  (DATE only)

Why DATE not TIMESTAMP?
Time is always T00:00:00 (midnight) — no real information
Storing as DATE is more accurate than storing fake precision
```

**extend() not append()**
```python
# transform_cart() returns a LIST of rows
# extend adds all items from that list into transformed
# append would add the list itself (wrong — nested list)

transformed.extend(clean_rows)  ✅
transformed.append(clean_rows)  ❌
```

### Run
```
python -m etl.transform.carts_transform
```

### Expected Result
```
Raw carts in source:           7
Rows written to staging.carts: 20  ← more than 7 = explode worked ✅
```

---

## users_transform.py

### What It Does

Reads from raw.users, flattens all nested objects,
excludes password, writes to staging.users.

### Key Transformations

**Flatten name nested dict**
```
Raw:     "name": {"firstname": "John", "lastname": "Doe"}
Staging: first_name = "John"
         last_name  = "Doe"
```

**Flatten address and geolocation**
```
Raw:     "address": {
             "street": "7835 new road",
             "city": "Ann Arbor",
             "zipcode": "48100",
             "geolocation": {
                 "lat": "-37.3159",
                 "long": "81.1496"
             }
         }

Staging: address_street = "7835 new road"
         address_city   = "Ann Arbor"
         address_zip    = "48100"
         address_lat    = -37.3159
         address_lng    = 81.1496
```

**Geolocation strings cast to float**
```
API sends lat/long as strings: "-37.3159"
We cast to float: -37.3159
PostgreSQL stores as NUMERIC(9,6) for precision

Why NUMERIC(9,6)?
Geolocation needs 6 decimal places for street-level accuracy
NUMERIC(9,6) = up to 9 digits, exactly 6 decimal places
```

**Password excluded entirely**
```
Raw:     "password": "m38rmF$"
Staging: (not present)

Two reasons:
1. Business: passwords are useless for analysis
2. Security: data warehouse is accessible to many people
             storing passwords there = security breach ❌
             exclude at the earliest possible point ✅
```

**Defensive .get() calls**
```python
# Crash-safe nested access:
name    = raw_record.get("name", {})
address = raw_record.get("address", {})
geo     = address.get("geolocation", {})

# vs crash-prone direct access:
name = raw_record["name"]  ← KeyError if missing ❌

Why defensive coding?
APIs change without warning
A missing field should log a warning, not crash the pipeline
```

### Run
```
python -m etl.transform.users_transform
```

### Expected Result
```
Records written to staging.users: 10
password field excluded ✅
```

---

## Running All Three Transforms

Run each individually in order:
```
python -m etl.transform.products_transform
python -m etl.transform.carts_transform
python -m etl.transform.users_transform
```

In Stage 8 (Pipeline Runner) all three will be called
automatically in the correct order by run_pipeline.py.

---

## Verify All Staging Tables In Database
```sql
SELECT
    'staging.products' AS table_name,
    COUNT(*)           AS records
FROM staging.products
UNION ALL
SELECT
    'staging.carts',
    COUNT(*)
FROM staging.carts
UNION ALL
SELECT
    'staging.users',
    COUNT(*)
FROM staging.users;
```

Expected:
```
table_name          records
───────────────────────────
staging.products    20
staging.carts       20
staging.users       10
```

---

## What Does NOT Belong Here
```
API calls          → etl/extract/
Database schema    → sql/ddl/
Warehouse loading  → etl/load/
Pipeline runner    → run_pipeline.py
```

---

## Important Rules
```
1. Never modify raw schema from transform layer
   Transform reads from raw, never writes to it

2. Always truncate staging before inserting
   Prevents duplicate rows building up over runs

3. Transform in memory before truncating
   Protects against empty staging if insert fails

4. Never store password at any point
   Exclude it in transform_user() before it goes anywhere

5. Always use .get() for nested field access
   Defensive coding prevents crashes on missing fields

6. Always run as module from project root
   python -m etl.transform.products_transform
```

---

*Last updated: Stage 6 — Transform Layer*
*Previous: Stage 5 — Extract Layer (etl/extract/)*
*Next: Stage 7 — Load Layer (etl/load/)*