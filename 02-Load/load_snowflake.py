"""
Snowflake loader for the Shopify ELT pipeline.

Responsible for the "Load" stage: taking raw records pulled from the Shopify
Admin API and persisting them into the RAW schema in Snowflake. Each public
loader (orders, products, inventory) creates its target table if needed and
inserts the supplied records inside a single transaction.
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
# enforces NOT NULL — PRIMARY KEY is metadata-only — so real uniqueness/quality
# checks belong in the dbt layer downstream.
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

# Parameterized INSERT statements, one per target table. The "?" placeholders
# are positional and must line up with the tuples produced by each row-mapper.
INSERT_STATEMENTS: dict[str, str] = {
    "raw.orders": """
        INSERT INTO raw.orders
        (id, customer_id, total_price, created_at, order_number,
         financial_status, currency, updated_at, line_items)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    "raw.products": """
        INSERT INTO raw.products
        (id, title, product_type, vendor, handle, created_at, tags)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
    "raw.inventory": """
        INSERT INTO raw.inventory
        (inventory_item_id, variant_id, available, location_id, created_at, tracked)
        VALUES (?, ?, ?, ?, ?, ?)
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
    Create a target table (if needed) and insert records in one transaction.

    This is the shared engine behind every public loader. It centralizes the
    create-table, insert-loop, commit/rollback, and cleanup logic so the
    per-entity functions only need to declare *what* to load, not *how*.

    The whole operation is atomic: if any single insert fails, the transaction
    is rolled back and nothing is persisted, so a partial load never leaves the
    table in an inconsistent state.

    Args:
        table_name: Key into TABLE_SCHEMAS / INSERT_STATEMENTS, e.g. "raw.orders".
        records: Raw API records to load. An empty sequence is a no-op.
        row_mapper: Function mapping one record to the positional tuple that
            matches this table's INSERT statement.

    Returns:
        int: The number of records inserted (0 if the input was empty).

    Raises:
        KeyError: If table_name has no registered schema/insert statement.
        Exception: Re-raised after rollback if any insert fails.
    """
    # Guard clause: skip the connection overhead entirely if there's nothing
    # to do. This keeps empty API responses from being treated as errors.
    if not records:
        logger.warning(f"No records to load into {table_name}, skipping.")
        return 0

    create_sql = TABLE_SCHEMAS[table_name]
    insert_sql = INSERT_STATEMENTS[table_name]

    conn = _get_connection()
    cur = conn.cursor()
    try:
        # Idempotent: CREATE TABLE IF NOT EXISTS is safe to run every load.
        cur.execute(create_sql)

        # Row-by-row insert. Fine for the current volume; for large batches
        # switch to cur.executemany(insert_sql, [row_mapper(r) for r in records])
        # or Snowflake's write_pandas for far fewer round trips.
        for record in records:
            cur.execute(insert_sql, row_mapper(record))

        # Commit once at the end so the inserts land as a single atomic unit.
        conn.commit()
        logger.info(f"Successfully loaded {len(records)} rows into {table_name}")
        return len(records)

    except Exception as e:
        # Undo every insert from this run so we never leave a half-loaded table.
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
    Load Shopify orders into raw.orders.

    Args:
        orders: Order records as returned by the Shopify Admin API.

    Returns:
        int: Number of orders inserted.
    """
    return _load_records("raw.orders", orders, _map_order)


def load_products(products: Sequence[dict[str, Any]]) -> int:
    """
    Load Shopify products into raw.products.

    Args:
        products: Product records as returned by the Shopify Admin API.

    Returns:
        int: Number of products inserted.
    """
    return _load_records("raw.products", products, _map_product)


def load_inventory(inventory_items: Sequence[dict[str, Any]]) -> int:
    """
    Load Shopify inventory levels into raw.inventory.

    Args:
        inventory_items: Inventory-level records from the Shopify Admin API.

    Returns:
        int: Number of inventory records inserted.
    """
    return _load_records("raw.inventory", inventory_items, _map_inventory)