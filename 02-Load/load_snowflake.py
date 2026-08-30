"""
Snowflake loader for the Shopify ELT pipeline.

Responsible for the "Load" stage: taking raw records pulled from the Shopify
Admin API and persisting them into the RAW schema in Snowflake. Each public
loader (orders, products, inventory) creates its target table if needed and
upserts the supplied records inside a single transaction.

Loads are idempotent: records are written with MERGE on each entity's natural
key, so re-running the pipeline over an overlapping window updates the existing
rows instead of duplicating them.
"""

import os
import json
import logging
from typing import Any, Callable, Iterable, Optional, Sequence

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)

load_dotenv()

# Repo root, one level up from this 02-Load directory. Used to resolve relative
# paths (e.g. SNOWFLAKE_PRIVATE_KEY_PATH) so they mean the same thing no matter
# which working directory the pipeline is launched from.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------
# Kept together so the table contracts live in one place. Note: Snowflake only
# enforces NOT NULL — PRIMARY KEY is metadata-only, so it will NOT stop a
# duplicate insert. Uniqueness is enforced by the MERGE statements below (see
# MERGE_STATEMENTS), which key on the natural key declared here; the dbt layer
# still owns the broader data-quality tests.
TABLE_SCHEMAS: dict[str, str] = {
    "raw.orders": """
        CREATE TABLE IF NOT EXISTS raw.orders (
            id BIGINT NOT NULL PRIMARY KEY,
            created_at TIMESTAMP NOT NULL,
            total_price NUMERIC(10,2) NOT NULL,
            order_number INT,
            financial_status VARCHAR(50),
            line_items VARIANT,
            customer_id BIGINT,
            currency VARCHAR(3) DEFAULT 'USD',
            updated_at TIMESTAMP,
            processed_at TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "raw.products": """
        CREATE TABLE IF NOT EXISTS raw.products (
            id BIGINT NOT NULL PRIMARY KEY,
            title VARCHAR(255),
            product_type VARCHAR(100),
            vendor VARCHAR(255),
            handle VARCHAR(255),
            created_at TIMESTAMP,
            tags VARIANT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "raw.inventory": """
        CREATE TABLE IF NOT EXISTS raw.inventory (
            inventory_item_id BIGINT NOT NULL,
            variant_id BIGINT,
            available INT,
            location_id BIGINT,
            created_at TIMESTAMP,
            tracked BOOLEAN,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """,
}

# Per-table load contract used to generate the bulk-load SQL. Rather than store
# one hand-written MERGE per table, we describe each table's shape once and build
# the staging INSERT and the set-based MERGE from it (see the SQL builders
# below). This keeps the column list in exactly one place, so adding a column
# (e.g. processed_at) is a single edit here plus the schema and row-mapper.
#
#   - columns:        the columns loaded, in the SAME order the row-mapper emits
#                     them. Order is load-bearing: it lines up the "?" binds.
#   - json_columns:   VARIANT columns. They ride through staging as text and are
#                     wrapped in PARSE_JSON() during the MERGE, because Snowflake
#                     will not implicitly coerce a VARCHAR into a VARIANT.
#   - merge_keys:     the natural key the upsert matches on.
#   - null_safe_keys: key columns compared with EQUAL_NULL() instead of "=". A
#                     nullable key (inventory.location_id) needs this: plain
#                     NULL = NULL is NULL, so an un-located row would look "not
#                     matched" and insert a duplicate on every run.
#
# Why MERGE and not INSERT: Snowflake does not enforce PRIMARY KEY, so a plain
# INSERT would duplicate every record on a re-run and silently double-count the
# downstream marts. MERGE makes the load idempotent and also picks up
# Shopify-side mutations (an order's financial_status moving "pending" -> "paid").
LOAD_CONFIG: dict[str, dict[str, Any]] = {
    "raw.orders": {
        "columns": [
            "id", "customer_id", "total_price", "created_at", "order_number",
            "financial_status", "currency", "updated_at", "processed_at",
            "line_items",
        ],
        "json_columns": ["line_items"],
        "merge_keys": ["id"],
        "null_safe_keys": [],
    },
    "raw.products": {
        "columns": [
            "id", "title", "product_type", "vendor", "handle", "created_at",
            "tags",
        ],
        "json_columns": ["tags"],
        "merge_keys": ["id"],
        "null_safe_keys": [],
    },
    "raw.inventory": {
        "columns": [
            "inventory_item_id", "variant_id", "available", "location_id",
            "created_at", "tracked",
        ],
        "json_columns": [],
        "merge_keys": ["inventory_item_id", "location_id"],
        "null_safe_keys": ["location_id"],
    },
}

# Type alias: a row-mapper turns one raw API record (a dict) into the positional
# tuple expected by that table's staging INSERT.
RowMapper = Callable[[dict[str, Any]], tuple]


# ---------------------------------------------------------------------------
# Bulk-load SQL builders
# ---------------------------------------------------------------------------
# The load is done set-based: all rows are bulk-inserted into a session-scoped
# staging table in one round trip, then a SINGLE MERGE upserts staging into the
# target. This replaces the old row-by-row MERGE loop, which cost one warehouse
# round trip per record (~minutes for a few hundred orders).
def _staging_name(table_name: str) -> str:
    """Derive the transient staging table name for a target, e.g. raw_orders_stg."""
    return table_name.replace(".", "_") + "_stg"


def _staging_ddl(table_name: str, stg: str, cfg: dict[str, Any]) -> list[str]:
    """
    Build the statements that create the staging table for one load.

    The staging table is a TEMPORARY clone of the target (CREATE ... LIKE), so it
    inherits the exact column types with zero duplication. JSON columns are then
    swapped to VARCHAR: staging holds the raw JSON text, and the MERGE parses it
    with PARSE_JSON() on the way into the VARIANT target column. This lets the
    bulk INSERT use a plain positional-bind form the connector can batch.

    Returns:
        list[str]: DDL statements to run in order.
    """
    stmts = [f"CREATE OR REPLACE TEMPORARY TABLE {stg} LIKE {table_name}"]
    for col in cfg["json_columns"]:
        stmts.append(f"ALTER TABLE {stg} DROP COLUMN {col}")
        stmts.append(f"ALTER TABLE {stg} ADD COLUMN {col} VARCHAR")
    return stmts


def _insert_sql(stg: str, cfg: dict[str, Any]) -> str:
    """Build the parameterized bulk INSERT into staging (columns in mapper order)."""
    cols = cfg["columns"]
    placeholders = ", ".join(["?"] * len(cols))
    return f"INSERT INTO {stg} ({', '.join(cols)}) VALUES ({placeholders})"


def _source_expr(col: str, cfg: dict[str, Any]) -> str:
    """Reference a staging column in the MERGE, parsing JSON columns to VARIANT."""
    return f"PARSE_JSON(source.{col})" if col in cfg["json_columns"] else f"source.{col}"


def _merge_sql(table_name: str, stg: str, cfg: dict[str, Any]) -> str:
    """
    Build the single set-based MERGE that upserts staging into the target.

    Matches on the table's natural key (EQUAL_NULL for nullable keys), updates
    every non-key column on a match, and inserts otherwise. loaded_at is set
    explicitly in both branches: the column DEFAULT only fires on INSERT, so
    without setting it in UPDATE an upserted row would keep its original
    load timestamp.
    """
    cols = cfg["columns"]
    keys = cfg["merge_keys"]
    null_safe = set(cfg["null_safe_keys"])

    on_clause = " AND ".join(
        f"EQUAL_NULL(target.{k}, source.{k})" if k in null_safe
        else f"target.{k} = source.{k}"
        for k in keys
    )
    update_cols = [c for c in cols if c not in keys]
    set_clause = ",\n            ".join(
        f"target.{c} = {_source_expr(c, cfg)}" for c in update_cols
    )
    insert_cols = ", ".join(cols + ["loaded_at"])
    insert_vals = ", ".join(_source_expr(c, cfg) for c in cols) + ", CURRENT_TIMESTAMP()"

    return f"""
        MERGE INTO {table_name} AS target
        USING {stg} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET
            {set_clause},
            target.loaded_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
        VALUES ({insert_vals})
    """


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------
def load_private_key() -> bytes:
    """
    Load and serialize the Snowflake private key for key-pair authentication.

    Reads the PEM key file pointed to by SNOWFLAKE_PRIVATE_KEY_PATH, decrypts it
    with the optional passphrase, and re-serializes it to the DER/PKCS8 byte
    format that the Snowflake connector expects.

    A relative SNOWFLAKE_PRIVATE_KEY_PATH is resolved against the repo root, not
    the current working directory, so the pipeline finds the key whether it is
    launched from the repo root, from 02-Load/, or from a scheduler.

    Returns:
        bytes: The private key encoded as unencrypted DER/PKCS8.

    Raises:
        KeyError: If SNOWFLAKE_PRIVATE_KEY_PATH is not set.
        FileNotFoundError: If the key file does not exist at the given path.
        ValueError: If the key cannot be parsed or the passphrase is wrong.
    """
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    # Anchor a relative path to the repo root so CWD doesn't decide whether the
    # key is found. Absolute paths are left untouched.
    if not os.path.isabs(key_path):
        key_path = os.path.join(_REPO_ROOT, key_path)

    with open(key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _get_connection() -> SnowflakeConnection:
    """
    Open a Snowflake connection using key-pair auth and env-based config.

    All connection parameters are read from environment variables so no
    credentials are hard-coded. The active schema is set to RAW, the landing
    zone for untransformed data.

    paramstyle is pinned to "qmark" because the statements in this module use
    positional "?" placeholders. The connector's default is "pyformat" (%s),
    under which those "?" would be passed through to Snowflake as literal text
    and fail to compile.

    login_timeout caps how long connect() will block trying to reach Snowflake.
    Without it, an unreachable host, a bad account identifier, or a blocked OCSP
    check hangs the whole EL run indefinitely at the "Connecting to ... Snowflake
    domain" log line; with it, the connector gives up and raises a real error we
    can act on.

    autocommit is turned off so the bulk INSERT into staging and the MERGE into
    the target commit together as one unit, and a failure can be rolled back.
    (DDL such as the staging CREATE still auto-commits in Snowflake regardless.)

    Returns:
        SnowflakeConnection: An open connection. The caller owns it and is
        responsible for closing it.

    Raises:
        Exception: Re-raised after logging if the connection cannot be opened.
    """
    try:
        return snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            private_key=load_private_key(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema="RAW",
            paramstyle="qmark",
            login_timeout=20,
            autocommit=False,
        )
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        raise


# ---------------------------------------------------------------------------
# Generic load engine
# ---------------------------------------------------------------------------
def _load_records(
    table_name: str,
    records: Sequence[dict[str, Any]],
    row_mapper: RowMapper,
) -> int:
    """
    Create a target table (if needed) and upsert records in one transaction.

    This is the shared engine behind every public loader. It centralizes the
    create-table, bulk-load, commit/rollback, and cleanup logic so the per-entity
    functions only need to declare *what* to load, not *how*.

    The load is set-based rather than row-by-row: every record is bulk-inserted
    into a session-scoped staging table in one round trip, then a SINGLE MERGE
    upserts staging into the target. This is what makes a few-hundred-order load
    finish in seconds instead of minutes — the old approach paid one warehouse
    round trip per record.

    The MERGE keys on each table's natural key, so the load stays idempotent:
    loading the same record twice updates the existing row rather than creating a
    second copy. Re-running the pipeline over an overlapping extract window is
    therefore safe.

    The INSERT and MERGE are one transaction (autocommit is off): if the MERGE
    fails, the staged rows are rolled back and nothing is persisted, so a partial
    load never leaves the target in an inconsistent state.

    Args:
        table_name: Key into TABLE_SCHEMAS / LOAD_CONFIG, e.g. "raw.orders".
        records: Raw API records to load. An empty sequence is a no-op.
        row_mapper: Function mapping one record to the positional tuple that
            matches this table's staging INSERT (columns in LOAD_CONFIG order).

    Returns:
        int: The number of records processed (0 if the input was empty). This
        counts records sent to Snowflake, not rows newly inserted — an upserted
        record that already existed still counts.

    Raises:
        KeyError: If table_name has no registered schema/load config.
        Exception: Re-raised after rollback if the load fails.
    """
    # Guard clause: skip the connection overhead entirely if there's nothing
    # to do. This keeps empty API responses from being treated as errors.
    if not records:
        logger.warning(f"No records to load into {table_name}, skipping.")
        return 0

    cfg = LOAD_CONFIG[table_name]
    create_sql = TABLE_SCHEMAS[table_name]
    stg = _staging_name(table_name)

    conn = _get_connection()
    cur = conn.cursor()
    try:
        # Idempotent: CREATE TABLE IF NOT EXISTS is safe to run every load.
        cur.execute(create_sql)

        # Fresh staging table for this load (temporary, so it's private to this
        # session and dropped when the connection closes).
        for stmt in _staging_ddl(table_name, stg, cfg):
            cur.execute(stmt)

        # Bulk-insert all rows in one batched round trip. executemany on a plain
        # INSERT ... VALUES lets the connector bind the whole batch at once
        # instead of sending one statement per record.
        rows = [row_mapper(record) for record in records]
        cur.executemany(_insert_sql(stg, cfg), rows)

        # One set-based MERGE from staging into the target does the whole upsert.
        cur.execute(_merge_sql(table_name, stg, cfg))

        # Commit so the INSERT + MERGE land as a single atomic unit.
        conn.commit()
        logger.info(f"Successfully loaded {len(records)} rows into {table_name}")
        return len(records)

    except Exception as e:
        # Undo every merge from this run so we never leave a half-loaded table.
        conn.rollback()
        logger.error(f"Error loading into {table_name}: {e}", exc_info=True)
        raise

    finally:
        # Always release resources, even if commit or an insert raised.
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Row mappers
# ---------------------------------------------------------------------------
# Each mapper converts one raw Shopify record into a positional tuple aligned
# with its table's INSERT statement. They use .get() for optional fields so a
# missing key yields NULL rather than raising KeyError.
def _map_order(order: dict[str, Any]) -> tuple:
    """Map one Shopify order record to a raw.orders row tuple."""
    # customer can be null on some orders (e.g. test/guest), so guard the
    # nested lookup before reaching for its id.
    customer = order.get("customer")
    customer_id = customer.get("id") if customer else None

    return (
        order["id"],
        customer_id,
        float(order["total_price"]),               # API sends price as a string
        order["created_at"],
        order.get("order_number"),
        order.get("financial_status"),
        order.get("currency"),
        order.get("updated_at"),
        order.get("processed_at"),                  # when Shopify processed the order
        json.dumps(order.get("line_items", [])),   # nested list -> JSON for VARIANT
    )


def _map_product(product: dict[str, Any]) -> tuple:
    """Map one Shopify product record to a raw.products row tuple."""
    return (
        product["id"],
        product.get("title"),
        product.get("product_type"),
        product.get("vendor"),
        product.get("handle"),
        product.get("created_at"),
        json.dumps(product.get("tags", [])),       # tags -> JSON for VARIANT
    )


def _map_inventory(item: dict[str, Any]) -> tuple:
    """Map one Shopify inventory record to a raw.inventory row tuple."""
    return (
        item["inventory_item_id"],
        item.get("variant_id"),
        item.get("available"),
        item.get("location_id"),
        item.get("created_at"),
        item.get("tracked"),
    )


# ---------------------------------------------------------------------------
# Public loaders
# ---------------------------------------------------------------------------
def load_orders(orders: Sequence[dict[str, Any]]) -> int:
    """
    Upsert Shopify orders into raw.orders, keyed on the order id.

    Args:
        orders: Order records as returned by the Shopify Admin API.

    Returns:
        int: Number of orders loaded (inserted or updated).
    """
    return _load_records("raw.orders", orders, _map_order)


def load_products(products: Sequence[dict[str, Any]]) -> int:
    """
    Upsert Shopify products into raw.products, keyed on the product id.

    Args:
        products: Product records as returned by the Shopify Admin API.

    Returns:
        int: Number of products loaded (inserted or updated).
    """
    return _load_records("raw.products", products, _map_product)


def load_inventory(inventory_items: Sequence[dict[str, Any]]) -> int:
    """
    Upsert Shopify inventory levels into raw.inventory.

    Keyed on (inventory_item_id, location_id), since an inventory item has one
    stock level per location.

    Args:
        inventory_items: Inventory-level records from the Shopify Admin API.

    Returns:
        int: Number of inventory records loaded (inserted or updated).
    """
    return _load_records("raw.inventory", inventory_items, _map_inventory)