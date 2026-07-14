"""
EL orchestrator for the Shopify -> Snowflake pipeline.

Single entrypoint for the Extract + Load stages: it pulls orders, products, and
inventory from the Shopify Admin API (01-Extract) and lands them in the Snowflake
RAW schema (02-Load). The Transform (dbt) and Forecast (Prophet) stages run after
this and are invoked separately:

    python elt_pipeline.py                       # extract + load into RAW
    (cd 03-Transform/my_elt_project && dbt build)  # transform RAW -> marts
    python 04-Forecast/forecast.py               # forecast order_volume mart

This replaces the legacy Postgres `elt_script.py`. The numbered stage
directories are not importable packages, so we inject them onto sys.path the same
way 02-Load does (mirrored in .vscode/settings.json's python.analysis.extraPaths).
"""

import logging
import os
import sys

from dotenv import load_dotenv

# Cross-stage imports: make the Extract and Load stage modules importable.
_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(_ROOT, "01-Extract"))
sys.path.append(os.path.join(_ROOT, "02-Load"))

from extract import (  # noqa: E402  (import after sys.path injection)
    extract_inventory,
    extract_orders,
    extract_products,
    get_access_token,
)
from load_snowflake import (  # noqa: E402
    load_inventory,
    load_orders,
    load_products,
)

logger = logging.getLogger(__name__)

load_dotenv()


def run_el() -> dict[str, int]:
    """
    Run the full Extract + Load stage end to end.

    Authenticates against Shopify, extracts each entity, and loads it into the
    matching RAW table. Products are extracted once and reused for the inventory
    join (inventory levels are derived from product variants), avoiding a second
    products pull.

    Returns:
        dict[str, int]: Row counts loaded per table, keyed by entity name
        ("orders", "products", "inventory").

    Raises:
        Exception: Propagated from any extract or load step. The load engine is
            transactional per table, so a failure leaves that table unchanged.
    """
    store = os.getenv("FAKER_STORE_URL")
    if not store:
        raise RuntimeError("FAKER_STORE_URL is not set; check your .env file.")

    logger.info("Authenticating with Shopify...")
    token = get_access_token()

    # Extract. Pull products first so inventory can reuse them for its join.
    logger.info("Extracting from Shopify...")
    products = extract_products(token, store)
    orders = extract_orders(token, store)
    inventory = extract_inventory(token, store, products=products)

    # Load each entity into its RAW table.
    logger.info("Loading into Snowflake RAW schema...")
    counts = {
        "products": load_products(products),
        "orders": load_orders(orders),
        "inventory": load_inventory(inventory),
    }

    logger.info(
        "EL complete -> orders: %s, products: %s, inventory: %s",
        counts["orders"], counts["products"], counts["inventory"],
    )
    return counts


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    run_el()