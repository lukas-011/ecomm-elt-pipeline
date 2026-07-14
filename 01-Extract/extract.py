"""
Shopify extractor for the ELT pipeline.

Responsible for the "Extract" stage: pulling raw records from the Shopify Admin
API so the Load stage can persist them into Snowflake's RAW schema. There is one
public extractor per RAW table that 02-Load/load_snowflake.py loads:

    extract_orders     -> raw.orders
    extract_products   -> raw.products
    extract_inventory  -> raw.inventory

Each extractor returns a list of dicts whose keys line up with the fields the
matching row-mapper in the loader reads, so extract -> load needs no reshaping
in between.
"""

import logging
import os
from typing import Any, Optional

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

# Pinning the API version keeps responses stable; bump deliberately, not by drift.
API_VERSION = "2026-04"


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def get_access_token() -> str:
    """Fetch a fresh access token using client credentials. Valid for 24 hours."""
    store = os.getenv("SHOPIFY_STORE_URL")
    logger.info(f"Requesting access token for store: {store}")
    try:
        response = requests.post(
            f"https://{store}/admin/oauth/access_token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": os.getenv("SHOPIFY_CLIENT_ID"),
                "client_secret": os.getenv("SHOPIFY_CLIENT_SECRET"),
            },
        )
        response.raise_for_status()
        logger.info("Access token retrieved successfully")
        return response.json()["access_token"]
    except requests.exceptions.HTTPError as e:
        logger.error(f"Failed to get access token: {e} | Response: {response.text}")
        raise


def get_headers(token: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
    }


# ---------------------------------------------------------------------------
# Pagination engine
# ---------------------------------------------------------------------------
def _next_page_url(link_header: str) -> Optional[str]:
    """
    Return the cursor URL for the next page, or None if there isn't one.

    Shopify uses cursor pagination: the Link response header carries a
    rel="next" (and possibly rel="previous") URL. We scan each comma-separated
    segment so a leading "previous" link can't shadow the "next" one.
    """
    for part in link_header.split(","):
        if 'rel="next"' in part:
            return part[part.index("<") + 1 : part.index(">")]
    return None


def _paginate(
    token: str,
    store: str,
    endpoint: str,
    result_key: str,
    params: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    """
    Fetch every page of a paginated Shopify list endpoint.

    This is the shared engine behind the public extractors so each one only
    declares *what* to fetch (endpoint + result key + filters), not *how* to
    walk the pages.

    Args:
        token: Shopify Admin API access token.
        store: Store domain, e.g. "gym-whale-rxzfdcpx.myshopify.com".
        endpoint: Resource path under the API version, e.g. "orders.json".
        result_key: Top-level key holding the records, e.g. "orders".
        params: Query filters for the first request. After page one, only the
            cursor URL is used (it already carries the limit/filters), so these
            are not re-sent.

    Returns:
        Every record across all pages, in order.

    Raises:
        requests.exceptions.HTTPError: Re-raised after logging on a failed call.
    """
    url = f"https://{store}/admin/api/{API_VERSION}/{endpoint}"
    request_params = dict(params or {})
    request_params.setdefault("limit", 250)

    records: list[dict[str, Any]] = []
    while url:
        try:
            response = requests.get(url, headers=get_headers(token), params=request_params)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Failed to fetch {endpoint}: {e} | Response: {response.text}")
            raise

        batch = response.json().get(result_key, [])
        records.extend(batch)
        logger.info(f"Fetched {len(batch)} {result_key} (total: {len(records)})")

        # Subsequent pages follow the cursor URL, which already encodes its own
        # params; sending the originals alongside it is an error in Shopify.
        url = _next_page_url(response.headers.get("Link", ""))
        request_params = {}

    return records


# ---------------------------------------------------------------------------
# Public extractors  (one per RAW table)
# ---------------------------------------------------------------------------
def extract_orders(token: str, store: str) -> list[dict[str, Any]]:
    """
    Extract all orders -> raw.orders.

    Returns order records whose fields cover the loader's _map_order:
    id, customer.id, total_price, created_at, order_number, financial_status,
    currency, updated_at, line_items.
    """
    logger.info("Starting order extraction...")
    orders = _paginate(token, store, "orders.json", "orders", {"status": "any"})
    logger.info(f"Order extraction complete. Total orders: {len(orders)}")
    return orders


def extract_products(token: str, store: str) -> list[dict[str, Any]]:
    """
    Extract all products and their variants -> raw.products.

    Returns product records whose fields cover the loader's _map_product:
    id, title, product_type, vendor, handle, created_at, tags. Variants are
    retained on each record so extract_inventory can reuse them.
    """
    logger.info("Starting product extraction...")
    products = _paginate(token, store, "products.json", "products")
    logger.info(f"Product extraction complete. Total products: {len(products)}")
    return products


def _get_locations(token: str, store: str) -> list[dict[str, Any]]:
    """Fetch the store's locations (small, unpaginated list)."""
    url = f"https://{store}/admin/api/{API_VERSION}/locations.json"
    response = requests.get(url, headers=get_headers(token))
    if not response.ok:
        logger.error(f"Error fetching locations: {response.status_code} - {response.text}")
    response.raise_for_status()
    return response.json().get("locations", [])


def extract_inventory(
    token: str,
    store: str,
    products: Optional[list[dict[str, Any]]] = None,
) -> list[dict[str, Any]]:
    """
    Extract inventory levels -> raw.inventory.

    Shopify spreads this data across three endpoints, so we join them on
    inventory_item_id to produce records matching the loader's _map_inventory
    (inventory_item_id, variant_id, available, location_id, created_at, tracked):

      - product variants   -> variant_id, created_at, and the tracking flag
      - locations          -> which location_ids to pull levels for
      - inventory_levels   -> available quantity per (item, location)

    Args:
        token: Shopify Admin API access token.
        store: Store domain.
        products: Optional pre-fetched product list. Passing the result of
            extract_products avoids a second products pull.

    Returns:
        One record per (inventory_item, location) level.
    """
    logger.info("Starting inventory extraction...")
    if products is None:
        products = extract_products(token, store)

    # Variants hold the fields the levels endpoint can't give us; key them by
    # inventory_item_id, which is the join key against inventory_levels.
    variant_by_item: dict[int, dict[str, Any]] = {}
    for product in products:
        for variant in product.get("variants", []):
            item_id = variant.get("inventory_item_id")
            if item_id is None:
                continue
            variant_by_item[item_id] = {
                "variant_id": variant.get("id"),
                "created_at": variant.get("created_at"),
                # Shopify only tracks quantity when inventory_management == "shopify".
                "tracked": variant.get("inventory_management") == "shopify",
            }

    location_ids = [loc["id"] for loc in _get_locations(token, store)]
    if not location_ids:
        logger.warning("No locations found; cannot fetch inventory levels.")
        return []

    levels = _paginate(
        token,
        store,
        "inventory_levels.json",
        "inventory_levels",
        {"location_ids": ",".join(map(str, location_ids))},
    )

    inventory: list[dict[str, Any]] = []
    for level in levels:
        item_id = level.get("inventory_item_id")
        variant = variant_by_item.get(item_id, {})
        inventory.append({
            "inventory_item_id": item_id,
            "variant_id": variant.get("variant_id"),
            "available": level.get("available"),
            "location_id": level.get("location_id"),
            "created_at": variant.get("created_at"),
            "tracked": variant.get("tracked"),
        })

    logger.info(f"Inventory extraction complete. Total levels: {len(inventory)}")
    return inventory


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    token = get_access_token()
    store = os.getenv("SHOPIFY_STORE_URL")

    resp = requests.get(
    f"https://{store}/admin/oauth/access_scopes.json",
    headers={"X-Shopify-Access-Token": token}
    )
    logger.info(f"Access scopes: {resp.json()}")
    # Pull products once and reuse them for the inventory join.
    products = extract_products(token, store)
    orders = extract_orders(token, store)
    inventory = extract_inventory(token, store, products=products)

    logger.info(
        f"Extraction summary -> orders: {len(orders)}, "
        f"products: {len(products)}, inventory levels: {len(inventory)}"
    )