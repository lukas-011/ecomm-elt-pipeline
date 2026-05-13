import json
import logging
import os
import requests
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

def get_access_token():
    """Fetch a fresh access token using client credentials. Valid for 24 hours."""
    store = os.getenv('FAKER_STORE_URL')
    logger.info(f"Requesting access token for store: {store}")
    try:
        response = requests.post(
            f"https://{store}/admin/oauth/access_token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": os.getenv('FAKER_CLIENT_ID'),
                "client_secret": os.getenv('FAKER_CLIENT_SECRET'),
            }
        )
        response.raise_for_status()
        logger.info("Access token retrieved successfully")
        return response.json()["access_token"]
    except requests.exceptions.HTTPError as e:
        logger.error(f"Failed to get access token: {e} | Response: {response.text}")
        raise

def get_headers(token):
    return {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token
    }

def extract_orders(token, store):
    """Extract all orders from Shopify API with pagination."""
    url = f"https://{store}/admin/api/2023-10/orders.json"
    params = {"status": "any", "limit": 250}
    all_orders = []
    logger.info("Starting order extraction...")

    try:
        while url:
            response = requests.get(url, headers=get_headers(token), params=params)
            response.raise_for_status()
            batch = response.json().get("orders", [])
            all_orders.extend(batch)
            logger.info(f"Fetched {len(batch)} orders (total so far: {len(all_orders)})")

            link_header = response.headers.get("Link", "")
            if 'rel="next"' in link_header:
                url = link_header.split("<")[1].split(">")[0]
                params = {}
            else:
                url = None

    except requests.exceptions.HTTPError as e:
        logger.error(f"Failed to extract orders: {e} | Response: {response.text}")
        raise

    logger.info(f"Order extraction complete. Total orders: {len(all_orders)}")
    return all_orders

if __name__ == "__main__":
    token = get_access_token()
    store = os.getenv('FAKER_STORE_URL')
    orders = extract_orders(token, store)