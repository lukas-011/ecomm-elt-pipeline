"""
extract.py
----------
Phase 1 of the gym brand data pipeline.

Responsibilities:
  - Load train and test CSVs from disk
  - Validate expected columns are present
  - Report null counts per table (no dropping yet — that's Transform's job)
  - Return a clean dictionary of DataFrames ready for the next phase

Usage (standalone):
  python extract.py

Usage (from main.py):
  from extract import run
  raw = run()
"""

import os
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATA_DIR = os.getenv("DATA_DIR", "00-Test-Data\Ecommerce Order Dataset")  # override via env var if needed

# Each table maps to the columns required in BOTH splits.
# Train-only columns (outcomes withheld from test) are listed separately
# so validation doesn't fail on the test split.
TABLES = {
    "customers": ["customer_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    "orders":    ["order_id", "customer_id", "order_purchase_timestamp", "order_approved_at"],
    "order_items": ["order_id", "product_id", "seller_id", "price", "shipping_charges"],
    "payments":  ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
    "products":  ["product_id", "product_category_name", "product_weight_g",
                  "product_length_cm", "product_height_cm", "product_width_cm"],
}

# Columns that only exist in train (outcome/label columns withheld from test).
# These are expected in train but their absence in test is not an error.
TRAIN_ONLY_COLUMNS = {
    "orders": ["order_status", "order_delivered_timestamp", "order_estimated_delivery_date"],
}

# Map table name -> CSV filename stem (handles the "OrderItems" vs "order_items" mismatch)
FILE_STEMS = {
    "customers":   "Customers",
    "orders":      "Orders",
    "order_items": "OrderItems",
    "payments":    "Payments",
    "products":    "Products",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_csv(split: str, table: str) -> pd.DataFrame:
    """Load a single CSV file. split is 'train' or 'test'."""
    stem = FILE_STEMS[table]
    filename = f"df_{stem}.csv"
    filepath = os.path.join(DATA_DIR, split, filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Expected file not found: {filepath}")

    df = pd.read_csv(filepath)
    print(f"  [OK] {filename} — {len(df):,} rows loaded")
    return df


def _validate_columns(df: pd.DataFrame, table: str, split: str) -> None:
    """
    Raise immediately if any required column is missing.
    Train-only columns are checked only on the train split.
    """
    expected = set(TABLES[table])
    if split == "train" and table in TRAIN_ONLY_COLUMNS:
        expected |= set(TRAIN_ONLY_COLUMNS[table])
    actual = set(df.columns)
    missing = expected - actual
    if missing:
        raise ValueError(f"[{table}] Missing columns in {split} split: {missing}")


def _null_report(df: pd.DataFrame, table: str) -> None:
    """Print a null summary. Nulls are logged here but NOT dropped — that's Transform's job."""
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]

    if nulls.empty:
        print(f"  [OK] {table} — no nulls detected")
    else:
        print(f"  [WARN] {table} — nulls found:")
        for col, count in nulls.items():
            pct = count / len(df) * 100
            print(f"         {col}: {count:,} ({pct:.1f}%)")


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def extract_split(split: str) -> dict[str, pd.DataFrame]:
    """
    Load and validate all tables for one split ('train' or 'test').
    Returns a dict: { table_name: DataFrame }
    """
    if split not in ("train", "test"):
        raise ValueError(f"split must be 'train' or 'test', got: {split!r}")

    print(f"\n{'='*50}")
    print(f"  EXTRACT — {split.upper()} split")
    print(f"{'='*50}")

    result = {}

    print("\n>> Loading files...")
    for table in TABLES:
        df = _load_csv(split, table)
        _validate_columns(df, table, split)
        result[table] = df

    print("\n>> Null report...")
    for table, df in result.items():
        _null_report(df, table)

    print(f"\n>> {split.upper()} extract complete. Tables loaded: {list(result.keys())}")
    return result


def run() -> dict[str, dict[str, pd.DataFrame]]:
    """
    Entry point called by main.py.
    Returns: { 'train': {table: df, ...}, 'test': {table: df, ...} }
    """
    return {
        "train": extract_split("train"),
        "test":  extract_split("test"),
    }


# ---------------------------------------------------------------------------
# Standalone run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    data = run()

    print("\n\n>> SUMMARY")
    print("-" * 40)
    for split, tables in data.items():
        for table, df in tables.items():
            print(f"  {split}.{table}: {df.shape}")
            