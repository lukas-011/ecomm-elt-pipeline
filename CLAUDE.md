# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

An ELT pipeline that pulls e-commerce data from a Shopify store (the "gym-whale"
dev store), lands it raw in Snowflake, transforms it with dbt, and forecasts order
volume with Prophet for display in Metabase. The pipeline is split into numbered
stage directories that run in sequence:

- `00-Generate-Fake-Data/` — seeds the Shopify store with fake orders via the Admin API (one-time/dev setup).
- `01-Extract/` — pulls orders/products/inventory from the Shopify Admin API.
- `02-Load/` — upserts raw records into Snowflake's RAW schema.
- `03-Transform/my_elt_project/` — dbt project that models RAW into staging views + marts.
- `04-Forecast/` — Prophet script that forecasts the `order_volume` mart.

`elt_pipeline.py` at the repo root is the EL entrypoint: it wires stage 01 to
stage 02 in one run. Transform and Forecast are invoked separately (see Commands).

## Architecture notes that span multiple files

**Cross-stage imports use `sys.path` injection, not a package.** The numbered
directories are not importable packages (their names aren't valid identifiers), so
modules reach into sibling stages at the top of the file: `elt_pipeline.py` appends
both `01-Extract` and `02-Load`, and `04-Forecast/forecast.py` appends `02-Load` to
reuse `load_private_key()`. `.vscode/settings.json` mirrors these paths in
`python.analysis.extraPaths` so the editor resolves them. When adding a new
cross-stage import, update both the `sys.path` call and the VS Code extraPaths.

**One store domain, one resolver.** `extract.get_store_url()` reads
`SHOPIFY_STORE_URL` and is the single source of truth for which store the pipeline
talks to. This matters because a client-credentials token is minted *for a specific
domain* and is rejected by any other, so the token request and every subsequent API
call must agree. Callers (including `elt_pipeline.py`) should take the domain from
`get_store_url()` rather than reading an env var of their own.

**Extract and load are shaped to fit each other with no adapter in between.**
There is one public extractor per RAW table, and each returns dicts whose keys are
exactly what that table's row-mapper in the loader reads:

| Extractor (`01-Extract/extract.py`) | Loader (`02-Load/load_snowflake.py`) | Table |
| --- | --- | --- |
| `extract_orders` | `load_orders` / `_map_order` | `raw.orders` |
| `extract_products` | `load_products` / `_map_product` | `raw.products` |
| `extract_inventory` | `load_inventory` / `_map_inventory` | `raw.inventory` |

Changing a field on one side means changing its counterpart on the other.

**The loader is a generic engine, and loads are idempotent.** `TABLE_SCHEMAS` +
`MERGE_STATEMENTS` + per-entity row-mappers feed `_load_records()`, which does
create-table-if-not-exists, a row-by-row upsert, and commit/rollback in one
transaction. Writes are `MERGE` on each entity's natural key, **not** `INSERT`:
Snowflake treats `PRIMARY KEY` as metadata only and does not enforce it, so an
insert-only load would duplicate every record on a re-run and double-count the
downstream marts. Re-running over an overlapping window is therefore safe. Two
subtleties live in that file's comments — JSON columns bind as text wrapped in
`PARSE_JSON()` (Snowflake won't coerce a VARCHAR bind into a VARIANT column), and
`raw.inventory` matches on `(inventory_item_id, location_id)` using `EQUAL_NULL()`
because plain `=` against a NULL `location_id` would never match and would insert a
duplicate on every run.

**`paramstyle="qmark"` is load-bearing.** Every statement in the loader uses
positional `?` placeholders; the connector's default `pyformat` would pass those
through to Snowflake as literal text and fail to compile.

**Products are extracted once and reused.** Inventory levels are derived from
product variants, so `extract_inventory(token, store, products=products)` accepts
the already-fetched product list to avoid a second products pull. `elt_pipeline.py`
depends on this ordering: products, then orders, then inventory.

**Pagination** is handled by parsing the Shopify `Link` response header for
`rel="next"`. `_paginate()` deliberately drops the original query params after page
one, because the cursor URL already encodes them and re-sending them is an error in
Shopify.

**The extract layer is duplicated.** `extract_products()` exists in both
`01-Extract/extract.py` and `00-Generate-Fake-Data/shopify-faker.py`.

### Known rough edges

- `01-Extract/test_extract.py` hardcodes the store domain
  (`gym-whale-rxzfdcpx.myshopify.com`) instead of calling `get_store_url()`, and its
  `sys.path.insert` points at a nonexistent `01-Extract/01-Extract` (harmless —
  Python already puts the script's own directory on the path).
- `02-Load/__pycache__/load.cpython-313.pyc` is a tracked leftover from the removed
  Postgres loader. That whole path (`elt_script.py`, `load.py`, `init.sql`,
  `docker-compose.yaml`) is gone from source; Snowflake is the only load target.
- `FAKER_STORE_URL` / `FAKER_CLIENT_ID` / `FAKER_CLIENT_SECRET` exist in `.env` but
  no current code reads them — the faker uses `SHOPIFY_STORE_URL` like everything
  else. Left in place intentionally; don't "clean them up" unprompted.
- The `seeds/` CSVs (`raw_customers`, `raw_items`, `raw_stores`, `raw_supplies`, …)
  are scaffold leftovers: no model `ref()`s them. Every model reads from the
  `shopify` source instead, so `dbt seed` is not needed to build this project.

## Commands

Python stages are run directly as scripts; there is no installed package.
**Use the `py` launcher, not `python`** — on this machine `python` resolves to the
Microsoft Store app-execution alias stub and fails with "Python was not found".

```bash
py elt_pipeline.py                         # EL: extract from Shopify -> load into RAW
py 00-Generate-Fake-Data/shopify-faker.py  # seed the dev store with fake orders
py 01-Extract/extract.py                   # extract only (logs per-entity counts)
py 01-Extract/test_extract.py              # smoke-check extract against the live store (script, not pytest)
```

`02-Load/load_snowflake.py` is an importable module with no `__main__` runner; drive
it through `elt_pipeline.py`.

Full pipeline, in order:

```bash
py elt_pipeline.py                                # 01 + 02  -> RAW
cd 03-Transform/my_elt_project && dbt build       # 03       -> staging views + marts
py 04-Forecast/forecast.py                        # 04       -> order_volume_forecast
```

`requirements.txt` covers the active stack (requests, python-dotenv,
snowflake-connector-python, cryptography, ShopifyAPI, faker, prophet, pandas,
matplotlib). dbt itself is installed separately: `pip install dbt-snowflake`.

### dbt (run from `03-Transform/my_elt_project/`)

```bash
dbt deps      # install packages from packages.yml (dbt_utils)
dbt run       # build staging (views) + marts (tables)
dbt test      # run schema tests
dbt build     # run + test in dependency order
```

dbt uses a `my_elt_project` profile (defined in `~/.dbt/profiles.yml`, not in this
repo). Staging models materialize as views, marts as tables. The source is declared
in `models/staging/gym_whale/_src_gym_whale.yml` as `shopify` -> database
`SHOPIFY_ELT`, schema `RAW` — i.e. exactly where the loader writes.

The `order_volume` mart is built for Prophet specifically: it LEFT JOINs daily order
counts onto a `dbt_utils.date_spine` so a day with no orders is a real `0` rather
than a missing row, which Prophet requires. `04-Forecast/forecast.py` then renames
`order_date`/`order_count` to `ds`/`y`.

### Forecast

```bash
py 04-Forecast/forecast.py                 # read mart from Snowflake, write back + CSV + PNG
py 04-Forecast/forecast.py --from-csv      # develop offline from the output-files/ CSV
py 04-Forecast/forecast.py --no-write-back # skip the Snowflake write-back
py 04-Forecast/forecast.py --horizon 60    # forecast 60 days ahead
```

Outputs land in `output-files/` (`order_volume_forecast.csv` for Metabase/sharing,
`order_volume_forecast.png` for a quick visual check). The write-back uses
`write_pandas(..., overwrite=True)`, so `order_volume_forecast` is replaced each run
and Metabase always reads one current forecast.

## Configuration

All config comes from env vars loaded via `python-dotenv` from `.env`.

- **Shopify** (OAuth client-credentials flow, token valid ~24h): `SHOPIFY_STORE_URL`,
  `SHOPIFY_CLIENT_ID`, `SHOPIFY_CLIENT_SECRET`. The API version is pinned in
  `extract.py` as `API_VERSION` — bump it deliberately, not by drift.
- **Snowflake** (key-pair auth): `SNOWFLAKE_USER`, `SNOWFLAKE_ACCOUNT`,
  `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_PRIVATE_KEY_PATH`, and
  optional `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`. The loader pins its active schema to
  `RAW`; the forecast uses `SNOWFLAKE_SCHEMA` (default `PUBLIC`) to find the marts.
- **Forecast overrides** (optional): `FORECAST_SOURCE_TABLE` (default
  `order_volume`), `FORECAST_TARGET_TABLE` (default `order_volume_forecast`).

`load_private_key()` in `02-Load/load_snowflake.py` reads the PEM and re-serializes
it to the DER/PKCS8 form the connector expects. It is the only place that touches
key material — `04-Forecast` imports it rather than repeating the logic.

## Secrets

`.env`, `secrets/`, and `*.pem` are gitignored. Snowflake key-pair auth expects a
PEM private key on disk pointed to by `SNOWFLAKE_PRIVATE_KEY_PATH`.


# How I want you to act when making changes to this project
I want you to act in accordance with modern software engineering principles and always make the best design choice. If you are unsure what is the best, pick one that is the most widely used for the scenario you're coding

Always leave a detailed description of what a function does

Always set return types and define argument types

If there is complex logic, leave a note explaining what it is doing

## Tech Stack
- **Data Source**: Shopify Admin API
- **Orchestration**: Apache Airflow
- **Extract**: Python (requests, logging)
- **Warehouse**: Snowflake
- **Transform**: dbt
- **Forecasting**: Prophet
- **Visualization**: Metabase
- **Version Control**: Git/GitHub
