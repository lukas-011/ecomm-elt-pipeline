"""
ETL Pipeline: Customer Purchase Data → Prophet-Ready Time Series
================================================================
Source tables : df_Orders, df_Payments, df_Products, df_Customers, df_OrderItems
Target        : Daily purchase volume (order count) ready for Prophet
Prophet cols  : ds (date), y (order count)

Run:
    python etl_prophet_pipeline.py
    # Reads CSVs locally; swap load_data() for SQLAlchemy to pull from PostgreSQL.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
# Statuses that represent a genuine completed purchase
VALID_STATUSES = {"delivered", "shipped", "invoiced", "approved", "processing"}

# Paths – change to your own paths or replace load_data() with SQLAlchemy
INPUT_DIR = Path(f"C:\\Users\\Lukanator\\Documents\\VisualStudioCodeProjects\\ecomm-etl-pipeline\\00-Test-Data\\Ecommerce Order Dataset\\train")
OUTPUT_PATH = Path(f"C:\\Users\\Lukanator\\Documents\\VisualStudioCodeProjects\\ecomm-etl-pipeline\\output-files\\prophet_order_volume.csv")
QUARANTINE_PATH = Path(f"C:\\Users\\Lukanator\\Documents\\VisualStudioCodeProjects\\ecomm-etl-pipeline\\output-files\\quarantine_orders.csv")


# =============================================================================
# STEP 1 – Extract (load raw data)
# =============================================================================
def load_data(input_dir: Path) -> dict[str, pd.DataFrame]:
    """
    Load raw CSVs.  Swap this function for a SQLAlchemy version to pull
    directly from PostgreSQL:

        engine = create_engine("postgresql://user:pass@host/db")
        return {
            "orders":     pd.read_sql("SELECT * FROM orders", engine),
            "payments":   pd.read_sql("SELECT * FROM payments", engine),
            ...
        }
    """
    tables = {
        "orders":     "df_Orders.csv",
        "payments":   "df_Payments.csv",
        "products":   "df_Products.csv",
        "customers":  "df_Customers.csv",
        "order_items":"df_OrderItems.csv",
    }
    raw = {}
    for name, filename in tables.items():
        path = input_dir / filename
        raw[name] = pd.read_csv(path)
        log.info(f"Loaded  {name:12s}  {len(raw[name]):>6,} rows")
    return raw


# =============================================================================
# STEP 2 – Validate & quarantine (Fail Fast)
# =============================================================================
def validate_orders(orders: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Principle 1 – Validate early, fail fast.
    Separate clearly bad rows into a quarantine table; never silently drop.
    """
    bad_mask = pd.Series(False, index=orders.index)

    # Must have both primary keys
    bad_mask |= orders["order_id"].isna()
    bad_mask |= orders["customer_id"].isna()

    # Purchase timestamp must parse as a date
    parsed = pd.to_datetime(orders["order_purchase_timestamp"], errors="coerce")
    bad_mask |= parsed.isna()

    # Flag implausible dates (before 2010 or in the future)
    bad_mask |= parsed.lt(pd.Timestamp("2010-01-01"))
    bad_mask |= parsed.gt(pd.Timestamp.now())

    bad = orders[bad_mask].copy()
    good = orders[~bad_mask].copy()

    log.info(
        f"Validation – {len(good):,} good rows | "
        f"{len(bad):,} quarantined (bad timestamp or missing key)"
    )
    return good, bad


# =============================================================================
# STEP 3 – Clean Orders
# =============================================================================
def clean_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Principles 2-5:
      • Never mutate source (we work on a copy)
      • Handle nulls explicitly
      • Standardise formats
      • Deduplicate intentionally
    """
    df = orders.copy()
    print(df.columns.to_list())

    # -- 3a. Parse & standardise timestamps (Principle 4) --------------------
    for col in ["order_purchase_timestamp", "order_approved_at",
                 "order_delivered_timestamp", "order_estimated_delivery_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # -- 3b. Standardise status strings (Principle 4) ------------------------
    df["order_status"] = df["order_status"].str.strip().str.lower()

    # -- 3c. Filter to valid purchase statuses (Principle 9 – business rule) -
    before = len(df)
    df = df[df["order_status"].isin(VALID_STATUSES)].copy()
    log.info(
        f"Status filter – kept {len(df):,} / {before:,} rows "
        f"(removed {before - len(df):,} canceled / unavailable)"
    )

    # -- 3d. Null handling: flag orders missing a delivery timestamp ----------
    #        (Principle 3 – flag, don't silently ignore)
    df["is_delivery_missing"] = df["order_delivered_timestamp"].isna()
    log.info(
        f"Null delivery timestamps flagged: "
        f"{df['is_delivery_missing'].sum():,} rows"
    )

    # -- 3e. Deduplication on business key (Principle 5) ---------------------
    before = len(df)
    df = df.drop_duplicates(subset="order_id", keep="first")
    log.info(
        f"Dedup orders – {before - len(df):,} duplicate order_ids removed"
    )

    return df


# =============================================================================
# STEP 4 – Clean Products
# =============================================================================
def clean_products(products: pd.DataFrame) -> pd.DataFrame:
    """
    Products table has ~62k duplicate rows (same product_id repeated once per
    order-item join).  Deduplicate to one row per unique product.
    Null category → 'unknown'.
    """
    df = products.copy()

    # Principle 3 – impute nulls with a sentinel rather than dropping
    df["product_category_name"] = (
        df["product_category_name"]
        .str.strip()
        .str.lower()
        .fillna("unknown")
    )

    # Principle 5 – deduplicate on product_id
    before = len(df)
    df = df.drop_duplicates(subset="product_id", keep="first")
    log.info(
        f"Dedup products – {before - len(df):,} duplicate product_ids removed "
        f"| {len(df):,} unique products remain"
    )

    # Impute missing physical dimensions with median (Principle 3)
    for col in ["product_weight_g", "product_length_cm",
                 "product_height_cm", "product_width_cm"]:
        median_val = df[col].median()
        nulls = df[col].isna().sum()
        if nulls:
            df[col] = df[col].fillna(median_val)
            log.info(f"Imputed {nulls} nulls in '{col}' with median {median_val:.1f}")

    return df


# =============================================================================
# STEP 5 – Clean Customers
# =============================================================================
def clean_customers(customers: pd.DataFrame) -> pd.DataFrame:
    df = customers.copy()

    # Standardise string fields (Principle 4)
    df["customer_city"] = df["customer_city"].str.strip().str.title()
    df["customer_state"] = df["customer_state"].str.strip().str.upper()

    return df


# =============================================================================
# STEP 6 – Join into a flat analytical table
# =============================================================================
def build_flat_table(
    orders: pd.DataFrame,
    payments: pd.DataFrame,
    products: pd.DataFrame,
    customers: pd.DataFrame,
    order_items: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join all cleaned tables into one analytical flat table.
    Uses LEFT JOIN from orders so we never silently lose orders.
    """
    df = (
        orders
        .merge(order_items, on="order_id", how="left")
        .merge(products,   on="product_id", how="left")
        .merge(payments,   on="order_id", how="left")
        .merge(customers,  on="customer_id", how="left")
    )

    log.info(f"Flat table built  –  {len(df):,} rows × {df.shape[1]} columns")
    return df


# =============================================================================
# STEP 7 – Aggregate to Prophet format (ds, y)
# =============================================================================
def build_prophet_series(flat: pd.DataFrame) -> pd.DataFrame:
    """
    Prophet requires exactly two columns:
      ds – datestamp (daily)
      y  – the value to forecast (here: distinct order count per day)

    We count distinct order_ids per calendar day so that multi-item orders
    aren't double-counted.

    Principle 7 – idempotent: running this twice on the same flat table
    always produces the same output.
    """
    daily = (
        flat
        .groupby(flat["order_purchase_timestamp"].dt.date)["order_id"]
        .nunique()
        .reset_index()
        .rename(columns={"order_purchase_timestamp": "ds", "order_id": "y"})
    )
    daily["ds"] = pd.to_datetime(daily["ds"])
    daily = daily.sort_values("ds").reset_index(drop=True)

    # Fill any missing calendar days with 0 (Prophet needs a continuous series)
    full_range = pd.date_range(daily["ds"].min(), daily["ds"].max(), freq="D")
    daily = (
        daily
        .set_index("ds")
        .reindex(full_range, fill_value=0)
        .rename_axis("ds")
        .reset_index()
    )

    log.info(
        f"Prophet series ready – {len(daily):,} daily rows | "
        f"range {daily['ds'].min().date()} → {daily['ds'].max().date()} | "
        f"avg {daily['y'].mean():.1f} orders/day"
    )
    return daily


# =============================================================================
# STEP 8 – Data quality report
# =============================================================================
def log_quality_report(raw_orders: pd.DataFrame, prophet_df: pd.DataFrame) -> None:
    """
    Principle 8 – Log everything quantitatively.
    Prints a summary of key quality metrics.
    """
    zero_days = (prophet_df["y"] == 0).sum()
    log.info("=" * 55)
    log.info("DATA QUALITY REPORT")
    log.info(f"  Raw orders ingested      : {len(raw_orders):>7,}")
    log.info(f"  Prophet rows (days)      : {len(prophet_df):>7,}")
    log.info(f"  Zero-order days (filled) : {zero_days:>7,}")
    log.info(f"  Max orders in a day      : {int(prophet_df['y'].max()):>7,}")
    log.info(f"  Mean orders / day        : {prophet_df['y'].mean():>10.1f}")
    log.info("=" * 55)


# =============================================================================
# MAIN
# =============================================================================
def main():
    log.info("── ETL START ──────────────────────────────────────────")

    # 1. Extract
    raw = load_data(INPUT_DIR)

    # 2. Validate & quarantine
    clean_ord_raw, quarantine = validate_orders(raw["orders"])
    if len(quarantine):
        quarantine.to_csv(QUARANTINE_PATH, index=False)
        log.warning(f"Quarantined rows saved → {QUARANTINE_PATH}")

    # 3–5. Clean each table
    orders    = clean_orders(clean_ord_raw)
    products  = clean_products(raw["products"])
    customers = clean_customers(raw["customers"])
    payments  = raw["payments"].copy()     # already clean
    items     = raw["order_items"].copy()  # already clean

    # 6. Join
    flat = build_flat_table(orders, payments, products, customers, items)

    # 7. Aggregate → Prophet format
    prophet_df = build_prophet_series(flat)

    # 8. Quality report
    log_quality_report(raw["orders"], prophet_df)

    # 9. Save
    prophet_df.to_csv(OUTPUT_PATH, index=False)
    log.info(f"Prophet CSV saved → {OUTPUT_PATH}")
    log.info("── ETL END ────────────────────────────────────────────")

    return prophet_df


if __name__ == "__main__":
    main()