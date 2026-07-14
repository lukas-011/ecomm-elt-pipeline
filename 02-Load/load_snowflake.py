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

# Parameterized MERGE (upsert) statements, one per target table. The "?"
# placeholders are positional and must line up with the tuples produced by each
# row-mapper.
#
# Why MERGE and not INSERT: Snowflake does not enforce PRIMARY KEY, so a plain
# INSERT would duplicate every record on a re-run and silently double-count the
# downstream marts. MERGE makes the load idempotent — matching on the natural
# key, it updates the row if it already exists and inserts it if it does not.
# This also picks up Shopify-side mutations (an order's financial_status moving
# from "pending" to "paid", say), which an insert-only load would never see.
#
# Two details worth knowing:
#   - JSON columns are bound as text and wrapped in PARSE_JSON(), because
#     Snowflake will not implicitly coerce a VARCHAR bind into a VARIANT column.
#   - loaded_at is set explicitly in the UPDATE branch; the column DEFAULT only
#     fires on INSERT, so without this an updated row would keep its original
#     load timestamp.
MERGE_STATEMENTS: dict[str, str] = {
    "raw.orders": """
        MERGE INTO raw.orders AS target
        USING (
            SELECT
                ? AS id,
                ? AS customer_id,
                ? AS total_price,
                ? AS created_at,
                ? AS order_number,
                ? AS financial_status,
                ? AS currency,
                ? AS updated_at,
                PARSE_JSON(?) AS line_items
        ) AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET
            target.customer_id      = source.customer_id,
            target.total_price      = source.total_price,
            target.created_at       = source.created_at,
            target.order_number     = source.order_number,
            target.financial_status = source.financial_status,
            target.currency         = source.currency,
            target.updated_at       = source.updated_at,
            target.line_items       = source.line_items,
            target.loaded_at        = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (id, customer_id, total_price, created_at, order_number,
             financial_status, currency, updated_at, line_items, loaded_at)
        VALUES
            (source.id, source.customer_id, source.total_price,
             source.created_at, source.order_number, source.financial_status,
             source.currency, source.updated_at, source.line_items,
             CURRENT_TIMESTAMP())
    """,
    "raw.products": """
        MERGE INTO raw.products AS target
        USING (
            SELECT
                ? AS id,
                ? AS title,
                ? AS product_type,
                ? AS vendor,
                ? AS handle,
                ? AS created_at,
                PARSE_JSON(?) AS tags
        ) AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET
            target.title        = source.title,
            target.product_type = source.product_type,
            target.vendor       = source.vendor,
            target.handle       = source.handle,
            target.created_at   = source.created_at,
            target.tags         = source.tags,
            target.loaded_at    = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (id, title, product_type, vendor, handle, created_at, tags, loaded_at)
        VALUES
            (source.id, source.title, source.product_type, source.vendor,
             source.handle, source.created_at, source.tags, CURRENT_TIMESTAMP())
    """,
    # Inventory has no single unique column: a given inventory item exists once
    # per location, so the natural key is (inventory_item_id, location_id).
    # location_id is nullable, and NULL = NULL is NULL in SQL — which would make
    # every un-located row look "not matched" and insert a duplicate on each run.
    # EQUAL_NULL() is Snowflake's null-safe comparison and treats NULL = NULL as
    # true, closing that hole.
    "raw.inventory": """
        MERGE INTO raw.inventory AS target
        USING (
            SELECT
                ? AS inventory_item_id,
                ? AS variant_id,
                ? AS available,
                ? AS location_id,
                ? AS created_at,
                ? AS tracked
        ) AS source
        ON target.inventory_item_id = source.inventory_item_id
           AND EQUAL_NULL(target.location_id, source.location_id)
        WHEN MATCHED THEN UPDATE SET
            target.variant_id = source.variant_id,
            target.available  = source.available,
            target.created_at = source.created_at,
            target.tracked    = source.tracked,
            target.loaded_at  = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (inventory_item_id, variant_id, available, location_id, created_at,
             tracked, loaded_at)
        VALUES
            (source.inventory_item_id, source.variant_id, source.available,
             source.location_id, source.created_at, source.tracked,
             CURRENT_TIMESTAMP())
    """,
}

# Type alias: a row-mapper turns one raw API record (a dict) into the positional
# tuple expected by that table's INSERT statement.
RowMapper = Callable[[dict[str, Any]], tuple]


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------
def load_private_key() -> bytes:
    """
    Load and serialize the Snowflake private key for key-pair authentication.

    Reads the PEM key file pointed to by SNOWFLAKE_PRIVATE_KEY_PATH, decrypts it
    with the optional passphrase, and re-serializes it to the DER/PKCS8 byte
    format that the Snowflake connector expects.

    Returns:
        bytes: The private key encoded as unencrypted DER/PKCS8.

    Raises:
        KeyError: If SNOWFLAKE_PRIVATE_KEY_PATH is not set.
        FileNotFoundError: If the key file does not exist at the given path.
        ValueError: If the key cannot be parsed or the passphrase is wrong.
    """
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

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
    create-table, merge-loop, commit/rollback, and cleanup logic so the
    per-entity functions only need to declare *what* to load, not *how*.

    Each record is written with a MERGE on its natural key, which makes the load
    idempotent: loading the same record twice updates the existing row rather
    than creating a second copy. Re-running the pipeline over an overlapping
    extract window is therefore safe.

    The whole operation is atomic: if any single merge fails, the transaction is
    rolled back and nothing is persisted, so a partial load never leaves the
    table in an inconsistent state.

    Args:
        table_name: Key into TABLE_SCHEMAS / MERGE_STATEMENTS, e.g. "raw.orders".
        records: Raw API records to load. An empty sequence is a no-op.
        row_mapper: Function mapping one record to the positional tuple that
            matches this table's MERGE statement.

    Returns:
        int: The number of records processed (0 if the input was empty). This
        counts records sent to Snowflake, not rows newly inserted — an upserted
        record that already existed still counts.

    Raises:
        KeyError: If table_name has no registered schema/merge statement.
        Exception: Re-raised after rollback if any merge fails.
    """
    # Guard clause: skip the connection overhead entirely if there's nothing
    # to do. This keeps empty API responses from being treated as errors.
    if not records:
        logger.warning(f"No records to load into {table_name}, skipping.")
        return 0

    create_sql = TABLE_SCHEMAS[table_name]
    merge_sql = MERGE_STATEMENTS[table_name]

    conn = _get_connection()
    cur = conn.cursor()
    try:
        # Idempotent: CREATE TABLE IF NOT EXISTS is safe to run every load.
        cur.execute(create_sql)

        # Row-by-row merge. Fine for the current volume; for large batches, stage
        # the records into a temp table and run a single set-based MERGE against
        # it, which collapses N round trips into one.
        for record in records:
            cur.execute(merge_sql, row_mapper(record))

        # Commit once at the end so the merges land as a single atomic unit.
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