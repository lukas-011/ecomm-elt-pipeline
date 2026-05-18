import sys
from pathlib import Path
import logging
import pandas as pd
import json

sys.path.insert(0, str(Path(__file__).parent / "01-Extract"))
sys.path.insert(0, str(Path(__file__).parent / "02-Load"))

from extract import get_access_token, extract_orders, extract_products
from load import load_to_postgres

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def flatten_orders(orders_list):
    flattened = []
    for order in orders_list:
        flat_order = {
            'id': order.get('id'),
            'created_at': order.get('created_at'),
            'total_price': float(order.get('total_price', 0)),
            'currency': order.get('currency'),
            'financial_status': order.get('financial_status'),
            'line_items': json.dumps(order.get('line_items', [])),
            'order_number': order.get('order_number'),
        }
        flattened.append(flat_order)
    return flattened

def flatten_products(products_list):
    flattened = []
    for product in products_list:
        flat_product = {
            'id': product.get('id'),
            'title': product.get('title'),
            'vendor': product.get('vendor'),
            'product_type': product.get('product_type'),
            'created_at': product.get('created_at'),
            'handle': product.get('handle'),
            'variants': json.dumps(product.get('variants', [])),
            'options': json.dumps(product.get('options', [])),
        }
        flattened.append(flat_product)
    return flattened

if __name__ == "__main__":
    store = "gym-whale-rxzfdcpx.myshopify.com"
    
    logger.info("Starting extraction...")
    token = get_access_token()
    orders_list = extract_orders(token, store)
    products_list = extract_products(token, store)
    
    orders_flat = flatten_orders(orders_list)
    products_flat = flatten_products(products_list)
    
    orders_df = pd.DataFrame(orders_flat)
    products_df = pd.DataFrame(products_flat)
    
    logger.info(f"Orders DF columns: {orders_df.columns.tolist()}")
    logger.info(f"Orders DF shape: {orders_df.shape}")
    logger.info(f"First order row:\n{orders_df.iloc[0]}")
    
    logger.info("Starting load...")
    try:
        load_to_postgres(orders_df, "raw.orders", if_exists='append', chunksize=500)
        logger.info("✓ Orders loaded successfully")
    except Exception as e:
        logger.error(f"✗ Orders load failed: {e}", exc_info=True)
    
    try:
        load_to_postgres(products_df, "raw.products", if_exists='append', chunksize=500)
        logger.info("✓ Products loaded successfully")
    except Exception as e:
        logger.error(f"✗ Products load failed: {e}", exc_info=True)
    
    logger.info("Pipeline complete!")