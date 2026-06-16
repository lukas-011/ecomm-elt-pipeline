import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "02-Load"))
from sqlalchemy import text
import snowflake.connector
import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

load_dotenv()

def load_private_key():
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]   # the path from .env
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    with open(key_path, "rb") as key_file:                # open the file at that path
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )

    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

def _get_connection():
    """Establish a connection to Snowflake using environment variables."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            private_key=load_private_key(),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema="RAW"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise

def load_orders(orders):

    conn = _get_connection()

    """Create ORDERS table if it doesn't exist."""
    try:        
        with conn.cursor() as cur:
            cur.execute(text("""
                CREATE TABLE IF NOT EXISTS raw.orders (
                    id BIGINT PRIMARY KEY,
                    created_at TIMESTAMP,
                    total_price NUMERIC,
                    order_number INT,
                    financial_status VARCHAR,
                    line_items VARIANT,
                    customer_id BIGINT,
                    currency VARCHAR(3),
                    updated_at TIMESTAMP
                    );
            """))
    except Exception as e:
        print(f"Error creating table: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    """Load orders into Snowflake."""
    try:
        with conn.cursor() as cur:
            for order in orders:
                cur.execute(
                    text("""
                        INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, TOTAL_PRICE, CREATED_AT)
                        VALUES (:order_id, :customer_id, :total_price, :created_at)
                    """),
                    {
                        "order_id": order["id"],
                        "customer_id": order["customer"]["id"] if order.get("customer") else None,
                        "total_price": order["total_price"],
                        "created_at": order["created_at"]
                    }
                )
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


def load_products(products):

    conn = _get_connection()

    """Create PRODUCTS table if it doesn't exist."""
    try:        
        with conn.cursor() as cur:
            cur.execute(text("""
                CREATE TABLE raw.products (
                    id BIGINT PRIMARY KEY,
                    title VARCHAR,
                    product_type VARCHAR,
                    vendor VARCHAR,
                    handle VARCHAR,
                    created_at TIMESTAMP,
                    tags VARIANT
                    );
            """))
    except Exception as e:
        print(f"Error creating table: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    """Load products into Snowflake."""
    try:
        with conn.cursor() as cur:
            for product in products:
                cur.execute(
                    text("""
                        INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, TOTAL_PRICE, CREATED_AT)
                        VALUES (:order_id, :customer_id, :total_price, :created_at)
                    """),
                    {
                        "order_id": order["id"],
                        "customer_id": order["customer"]["id"] if order.get("customer") else None,
                        "total_price": order["total_price"],
                        "created_at": order["created_at"]
                    }
                )
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


def load_inventory(inventory_items):

    conn = _get_connection()

    """Create INVENTORY table if it doesn't exist."""
    try:        
        with conn.cursor() as cur:
            cur.execute(text("""
                CREATE TABLE raw.inventory (
                    inventory_item_id BIGINT,
                    variant_id BIGINT,
                    available INT,
                    location_id BIGINT,
                    created_at TIMESTAMP,
                    tracked BOOLEAN
                    );
            """))
    except Exception as e:
        print(f"Error creating table: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    """Load inventory items into Snowflake."""
    try:
        with conn.cursor() as cur:
            for item in inventory_items:
                cur.execute(
                    text("""
                        INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, TOTAL_PRICE, CREATED_AT)
                        VALUES (:order_id, :customer_id, :total_price, :created_at)
                    """),
                    {
                        "order_id": order["id"],
                        "customer_id": order["customer"]["id"] if order.get("customer") else None,
                        "total_price": order["total_price"],
                        "created_at": order["created_at"]
                    }
                )
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()