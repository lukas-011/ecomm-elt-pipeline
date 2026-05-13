from datetime import datetime, timedelta
import logging
import os
import random
import requests
import time

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "01-Extract"))

from extract import get_access_token, get_headers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def extract_products(token, store):
    """Extract all products and their variants from Shopify API."""
    url = f"https://{store}/admin/api/2023-10/products.json"
    params = {"limit": 250}
    all_products = []

    while url:
        response = requests.get(url, headers=get_headers(token), params=params)
        response.raise_for_status()
        batch = response.json().get("products", [])
        all_products.extend(batch)
        logger.info(f"Fetched {len(batch)} products (total: {len(all_products)})")

        link_header = response.headers.get("Link", "")
        if 'rel="next"' in link_header:
            url = link_header.split("<")[1].split(">")[0]
            params = {}
        else:
            url = None

    return all_products

def flatten_variants(products):
    """Pull out all variants from products so we can sample from them."""
    variants = []
    for product in products:
        for variant in product["variants"]:
            variants.append({
                "product_id": product["id"],
                "variant_id": variant["id"],
                "title": product["title"],
                "variant_title": variant["title"],
                "price": variant["price"],
                "sku": variant["sku"],
            })
    return variants

def seed_test_orders(token, store, count=50):
    logger.info("Fetching real products to sample from...")
    products = extract_products(token, store)
    variants = flatten_variants(products)
    
    if not variants:
        logger.error("No variants found")
        return
    
    logger.info(f"Seeding {count} orders...")
    start_date = datetime.now() - timedelta(days=730)
    
    for i in range(count):
        random_days = random.randint(0, 730)
        order_date = start_date + timedelta(days=random_days)
        
        sampled = random.sample(variants, k=min(random.randint(5, 10), len(variants)))
        line_items = [
            {
                "variant_id": v["variant_id"],
                "quantity": random.randint(1, 3)
            }
            for v in sampled
        ]
        
        url = f"https://{store}/admin/api/2023-10/orders.json"
        fake_order = {
            "order": {
                "created_at": order_date.isoformat(),
                "line_items": line_items,
                "financial_status": "paid",
            }
        }
        
        while True:
            response = requests.post(url, headers=get_headers(token), json=fake_order)
            
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited. Waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                response.raise_for_status()
                logger.info(f"Created order {i+1}/{count}")
                break
    
    logger.info("Seeding complete.")

if __name__ == "__main__":
    token = get_access_token()
    store = os.getenv('SHOPIFY_STORE_URL')
    seed_test_orders(token, store)