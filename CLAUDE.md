# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

An ELT pipeline that pulls e-commerce data from a Shopify store (the "gym-whale"
dev store), lands it raw in a warehouse, and transforms it with dbt. The pipeline
is split into numbered stage directories that run in sequence:

- `00-Generate-Fake-Data/` — seeds the Shopify store with fake orders via the Admin API (one-time/dev setup).
- `01-Extract/` — pulls orders/products from the Shopify Admin API.
- `02-Load/` — writes raw records into Snowflake.
- `03-Transform/my_elt_project/` — dbt project that models the raw data into marts.
- `04-Forecast` — A prophet forecasting python script that will display on Metabase

## Architecture notes that span multiple files

**Cross-stage imports use `sys.path` injection, not a package.** The numbered
directories are not importable packages. Modules reach into sibling stages at the
top of the file, e.g. `02-Load` and `elt_script.py` do
`sys.path.append(.../"01-Extract")` then `from extract import ...`. `.vscode/settings.json`
mirrors these paths in `python.analysis.extraPaths` so the editor resolves them.
When adding a new cross-stage import, update both the `sys.path` call and the
VS Code extraPaths.

**There are two load targets, and they are at different lifecycle stages.**
- Current/active: `02-Load/load_snowflake.py` loads into Snowflake using key-pair
  auth (`load_private_key()` reads a PEM and re-serializes to DER/PKCS8). It is a
  generic engine: `TABLE_SCHEMAS` + `INSERT_STATEMENTS` + per-entity row-mappers
  feed `_load_records()`, which does create-table-if-not-exists, row-by-row insert,
  and commit/rollback in one transaction.
- Legacy: `elt_script.py` imports `from load import load_to_postgres` (a `load.py`
  that no longer exists in source — only a stale `.pyc`), `docker-compose.yaml`
  spins up Postgres, and `init.sql` is the Postgres DDL. This Postgres path is
  superseded by the Snowflake loader and does not currently run end-to-end.

**The extract layer is duplicated.** `extract_products()` exists in both
`01-Extract/extract.py` and `00-Generate-Fake-Data/shopify-faker.py`. Pagination
is handled by parsing the Shopify `Link` response header for `rel="next"`.

**Auth and store identity come entirely from env vars** (loaded via
`python-dotenv` from `.env`). Note the two stores: the extractor reads
`FAKER_STORE_URL` / `FAKER_CLIENT_ID` / `FAKER_CLIENT_SECRET` (OAuth client-credentials
flow, token valid ~24h), while the faker also references `SHOPIFY_STORE_URL`.
Snowflake config uses `SNOWFLAKE_USER`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`,
`SNOWFLAKE_DATABASE`, `SNOWFLAKE_PRIVATE_KEY_PATH`, and optional
`SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`. The loader pins the active schema to `RAW`.

Note: hardcoded store strings (`gym-whale-rxzfdcpx.myshopify.com`) appear in
`elt_script.py` and `01-Extract/test_extract.py`, diverging from the env-var pattern
used elsewhere.

## Commands

Python stages are run directly as scripts (each has a `__main__` block); there is
no central entrypoint or installed package.

```bash
python 00-Generate-Fake-Data/shopify-faker.py   # seed the dev store with fake orders
python 01-Extract/extract.py            # extract orders (prints/logs results)
python 01-Extract/test_extract.py       # smoke-check extract against the live store (script, not pytest)
python 02-Load/load_snowflake.py        # importable loader module; no __main__ runner
```

`requirements.txt` is out of date relative to the current code — it lists the
Postgres/Prophet stack but not `snowflake-connector-python`, `requests`,
`python-dotenv`, or `cryptography` that the active modules import. Verify the
installed environment rather than trusting the file.

### dbt (run from `03-Transform/my_elt_project/`)

```bash
dbt deps      # install packages from packages.yml
dbt seed      # load seeds/ CSVs into the raw schema
dbt run       # build staging (views) + marts (tables)
dbt test      # run schema tests
dbt build     # seed + run + test in dependency order
```

dbt uses a `my_elt_project` profile (defined in `~/.dbt/profiles.yml`, not in this
repo). Staging models materialize as views, marts as tables.

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