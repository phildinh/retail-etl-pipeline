# etl/load/ — Load Layer

This folder contains the load layer of the ETL pipeline.
Its only job is to read clean data from staging and write
it into the warehouse using the correct loading pattern
for each table type.

Load does NOT pull from the API.
Load does NOT clean or transform data.
Load ONLY reads from staging and writes to warehouse.

---

## Why Load Is Its Own Layer
```
staging schema  → flat, typed, clean, temporary
                  ↓
load layer      → apply correct pattern per table type
                  ↓
warehouse schema → business-ready, history preserved forever
```

Keeping load separate means:
- Staging and warehouse have clearly defined responsibilities
- Loading logic can be changed without touching transform
- Each loader can be tested and run independently
- Warehouse history is never accidentally overwritten

---

## Files In This Folder
```
load/
├── products_load.py  → staging.products → dim_products (SCD2)
├── users_load.py     → staging.users    → dim_users    (SCD2)
├── orders_load.py    → staging.carts    → fact_orders  (append)
└── README.md         → this file
```

---

## Two Loading Patterns Used Here

### Pattern 1 — SCD Type 2 (dimensions)

Used for: dim_products, dim_users
```
Dimensions describe the WHO and WHAT:
  dim_products → WHAT was sold
  dim_users    → WHO bought it

These change over time and history matters:
  Product price drops from $109.95 to $89.95
  User moves from Sydney to Melbourne

SCD Type 2 preserves every version forever:
  Old version → expired (valid_to=today, is_current=FALSE)
  New version → inserted (valid_from=today, is_current=TRUE)

Business question answered:
  "What did this product cost in January?"
  → find version WHERE order_date BETWEEN valid_from AND valid_to
  → $109.95 ✅ (even if current price is now $89.95)
```

Three cases handled per record:
```
Case 1 — New record (never seen source_id before):
  → INSERT with valid_from=today, valid_to=9999-12-31

Case 2 — Changed record (tracked field differs):
  → UPDATE old row: valid_to=today, is_current=FALSE
  → INSERT new row: valid_from=today, valid_to=9999-12-31

Case 3 — Unchanged record (nothing different):
  → Do nothing at all
```

### Pattern 2 — Append Only (facts)

Used for: fact_orders
```
Facts record immutable historical events:
  "User 1 bought 4 units of product 1 on March 2"
  This event happened — it never changes

New pipeline run → only insert orders not yet loaded
Already loaded orders → skip (no duplicates)

WHY no SCD2 for facts?
An order cannot be "updated"
If it happened, it happened
We only ever add new orders, never modify old ones
```

---

## products_load.py

### What It Does

Reads staging.products, applies SCD2, writes to dim_products.

### Tracked Fields

Only these fields trigger a new version:
```
title     → product identity changed
price     → revenue calculations affected
category  → grouping and filtering affected
```

Not tracked (no analytical impact):
```
image_url    → cosmetic
description  → cosmetic
rating_rate  → constantly changing float
rating_count → constantly changing integer
```

### How It Detects Changes
```python
# Compares as strings to avoid type mismatch:
# PostgreSQL Decimal vs Python float comparisons
# can be unreliable with == operator
# String comparison is always reliable

str(Decimal("109.95")) == str(float(109.95))
"109.95" == "109.95"  ✅
```

### Why Dict Keyed By source_id?
```python
# O(1) lookup — instant regardless of warehouse size
current_products = {1: {...}, 2: {...}, 20: {...}}
current_products[source_id]  ← instant ✅

# vs O(n) list scan — slower as warehouse grows
for row in current_list:
    if row["source_id"] == source_id  ← slow ❌
```

### Run
```
python -m etl.load.products_load
```

### Expected Result (first run)
```
New products inserted:     20
Existing products updated:  0
Unchanged products:         0
```

### Expected Result (subsequent runs, no API changes)
```
New products inserted:      0
Existing products updated:  0
Unchanged products:        20
```

---

## users_load.py

### What It Does

Reads staging.users, applies SCD2, writes to dim_users.

### Tracked Fields
```
email          → user identity
username       → user identity
address_street → location history
address_city   → location history
address_zip    → location history
```

Not tracked:
```
phone     → low analytical value
first_name/last_name → rarely changes meaningfully
```

### Password Is Never Stored

Password was excluded in transform layer (users_transform.py).
It never reaches this file.
It never reaches the warehouse.
This is by design — security rule applied at earliest point.

### Run
```
python -m etl.load.users_load
```

### Expected Result (first run)
```
New users inserted:     10
Existing users updated:  0
Unchanged users:         0
```

---

## orders_load.py

### What It Does

Reads staging.carts (exploded rows), writes to fact_orders.
Append only — never updates or expires existing orders.

### How Duplicates Are Prevented
```python
# Fetch all cart_source_ids already in fact_orders
loaded_ids = {1, 2, 3, 4, 5}  ← set for O(1) lookup

# For each staging row:
if cart_source_id in loaded_ids:
    skip  ← already loaded
else:
    insert  ← new order
```

### Where unit_price Comes From
```
Price comes from dim_products not staging.products

WHY dim_products?
dim_products has SCD2 history
staging.products only has current state

We look up is_current=TRUE price at load time
This gives us the active price for each product ✅
```

### How total_price Is Calculated
```python
unit_price  = price from dim_products
total_price = round(unit_price * quantity, 2)

Example:
unit_price = 109.95
quantity   = 4
total_price = round(109.95 * 4, 2) = 439.80
```

### Error Handling
```
If product_source_id not found in dim_products:
→ log WARNING, skip row, count as error

If user_source_id not found in dim_users:
→ log WARNING, skip row, count as error

Pipeline continues — one bad row does not crash everything
errors=0 is expected on a clean run ✅
errors>0 means load order was wrong or data is corrupt ⚠
```

### Load Order Is Critical
```
fact_orders has foreign keys:
  product_sk → REFERENCES dim_products(product_sk)
  user_sk    → REFERENCES dim_users(user_sk)

MUST load in this order:
  Step 1: products_load  ← dim_products populated first
  Step 2: users_load     ← dim_users populated first
  Step 3: orders_load    ← can now reference both ✅

Wrong order causes foreign key violation errors ❌
```

### Run
```
python -m etl.load.orders_load
```

### Expected Result (first run)
```
Orders inserted:  20
Orders skipped:    0
Errors:            0
```

---

## Verify Full Warehouse In Database
```sql
-- Row counts across all warehouse tables
SELECT
    'dim_products' AS table_name,
    COUNT(*)       AS total_rows,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows
FROM warehouse.dim_products
UNION ALL
SELECT
    'dim_users',
    COUNT(*),
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END)
FROM warehouse.dim_users
UNION ALL
SELECT
    'fact_orders',
    COUNT(*),
    COUNT(*)
FROM warehouse.fact_orders;
```

Expected:
```
table_name     total_rows  current_rows
────────────────────────────────────────
dim_products   20          20
dim_users      10          10
fact_orders    20          20
```

---

## The Star Schema In Action
```sql
-- Full join across all three warehouse tables
-- This is what analysts and BI tools query
SELECT
    fo.order_sk,
    fo.order_date,
    fo.quantity,
    fo.unit_price,
    fo.total_price,
    dp.title      AS product_name,
    dp.category   AS product_category,
    du.email      AS customer_email,
    du.address_city AS customer_city
FROM warehouse.fact_orders fo
JOIN warehouse.dim_products dp
    ON fo.product_sk = dp.product_sk
JOIN warehouse.dim_users du
    ON fo.user_sk = du.user_sk
ORDER BY fo.order_date;
```

---

## Important Rules
```
1. Always load dimensions before facts
   dim_products and dim_users must exist before fact_orders
   Foreign key constraints enforce this at database level

2. Never truncate warehouse tables
   Truncate destroys SCD2 history forever
   Staging truncates — warehouse never does

3. SCD2 dimensions: compare tracked fields only
   Not every field change needs a new version
   Only fields with analytical impact are tracked

4. fact_orders: append only
   Orders are immutable historical events
   Never update or delete order facts

5. Always run as module from project root
   python -m etl.load.products_load
   python -m etl.load.users_load
   python -m etl.load.orders_load
```

---

*Last updated: Stage 7 — Load Layer*
*Previous: Stage 6 — Transform Layer (etl/transform/)*
*Next: Stage 8 — Pipeline Runner (run_pipeline.py)*