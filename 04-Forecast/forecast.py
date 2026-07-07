"""
Prophet forecast stage for the Shopify ELT pipeline.

Final stage: takes the daily order-volume series produced by the dbt
`order_volume` mart (03-Transform) and fits a Prophet model to project future
order volume. Results are written back to Snowflake and to a CSV so Metabase can
chart actuals vs. forecast on one timeline.

Data flow:
    Snowflake  order_volume (order_date, order_count)   [source]
        -> Prophet (ds, y)                               [model]
        -> Snowflake  order_volume_forecast              [for Metabase]
        -> output-files/order_volume_forecast.csv        [for Metabase / sharing]
        -> output-files/order_volume_forecast.png        [quick visual check]

The source defaults to Snowflake but falls back to the CSV in output-files/ so the
forecast can be developed and verified without a live warehouse:

    python 04-Forecast/forecast.py                 # read mart from Snowflake
    python 04-Forecast/forecast.py --from-csv      # read output-files CSV instead
    python 04-Forecast/forecast.py --no-write-back # skip the Snowflake write-back
    python 04-Forecast/forecast.py --horizon 60    # forecast 60 days ahead
"""

import argparse
import logging
import os
import sys
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from prophet import Prophet

# Reuse the loader's key-pair helper so PEM/DER handling lives in exactly one place.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(_ROOT, "02-Load"))

import snowflake.connector  # noqa: E402
from snowflake.connector import SnowflakeConnection  # noqa: E402
from snowflake.connector.pandas_tools import write_pandas  # noqa: E402

from load_snowflake import load_private_key  # noqa: E402

logger = logging.getLogger(__name__)

load_dotenv()

# Where the order-volume series comes from / where the forecast goes. The source
# table is the dbt mart; both default to the dbt target schema and are overridable.
SOURCE_TABLE = os.getenv("FORECAST_SOURCE_TABLE", "order_volume")
TARGET_TABLE = os.getenv("FORECAST_TARGET_TABLE", "order_volume_forecast")
FORECAST_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")

# output-files/ lives at the repo root and already holds the sample daily series.
_OUTPUT_DIR = os.path.join(_ROOT, "output-files")
DEFAULT_CSV = os.path.join(_OUTPUT_DIR, "prophet_order_volume.csv")
FORECAST_CSV = os.path.join(_OUTPUT_DIR, "order_volume_forecast.csv")
FORECAST_PNG = os.path.join(_OUTPUT_DIR, "order_volume_forecast.png")

DEFAULT_HORIZON_DAYS = 90


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
def _get_connection(schema: str = FORECAST_SCHEMA) -> SnowflakeConnection:
    """
    Open a Snowflake connection using key-pair auth and env-based config.

    Mirrors 02-Load's connection helper but targets the analytics/dbt schema
    (where the marts live) instead of RAW, since the forecast reads a mart and
    writes its output alongside it.

    Args:
        schema: Schema to set as the session default (defaults to SNOWFLAKE_SCHEMA).

    Returns:
        SnowflakeConnection: An open connection the caller is responsible for closing.
    """
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=load_private_key(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=schema,
    )


# ---------------------------------------------------------------------------
# Read the order-volume series
# ---------------------------------------------------------------------------
def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce a raw order-volume frame to Prophet's expected (ds, y) shape.

    Accepts either the dbt mart's columns (order_date, order_count) or the CSV's
    (ds, y), case-insensitively, so the same model code serves both sources.

    Args:
        df: Frame containing a date column and a daily-count column.

    Returns:
        DataFrame with exactly two columns: ds (datetime) and y (float),
        sorted by ds with any null rows dropped.

    Raises:
        ValueError: If neither a recognized date nor count column is present.
    """
    lookup = {c.lower(): c for c in df.columns}

    date_col = lookup.get("ds") or lookup.get("order_date")
    value_col = lookup.get("y") or lookup.get("order_count")
    if date_col is None or value_col is None:
        raise ValueError(
            f"Could not find date/value columns in {list(df.columns)}; "
            "expected (ds, y) or (order_date, order_count)."
        )

    out = pd.DataFrame({
        "ds": pd.to_datetime(df[date_col]),
        "y": pd.to_numeric(df[value_col], errors="coerce"),
    })
    return out.dropna(subset=["ds", "y"]).sort_values("ds").reset_index(drop=True)


def read_from_snowflake(table: str = SOURCE_TABLE) -> pd.DataFrame:
    """
    Read the daily order-volume mart from Snowflake into a (ds, y) frame.

    Args:
        table: Mart table name (resolved within the connection's database/schema).

    Returns:
        Normalized (ds, y) DataFrame ready for Prophet.
    """
    logger.info("Reading order volume from Snowflake table %s.%s", FORECAST_SCHEMA, table)
    conn = _get_connection()
    try:
        df = pd.read_sql(
            f"select order_date, order_count from {table} order by order_date",
            conn,
        )
    finally:
        conn.close()
    logger.info("Read %d rows from Snowflake", len(df))
    return _normalize(df)


def read_from_csv(path: str = DEFAULT_CSV) -> pd.DataFrame:
    """Read the daily order-volume series from a CSV into a (ds, y) frame."""
    logger.info("Reading order volume from CSV %s", path)
    df = pd.read_csv(path)
    logger.info("Read %d rows from CSV", len(df))
    return _normalize(df)


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
def fit_and_forecast(history: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    """
    Fit Prophet on the historical daily series and forecast ahead.

    Uses weekly + yearly seasonality (the natural cycles in retail order volume).
    Predicted order counts are clipped at 0 because volume can't be negative,
    which Prophet's linear trend can otherwise dip into during quiet periods.

    Args:
        history: Historical (ds, y) frame.
        horizon_days: Number of future days to predict.

    Returns:
        Forecast frame (ds, yhat, yhat_lower, yhat_upper) covering history plus
        the future horizon, with predictions clipped to be non-negative.
    """
    logger.info("Fitting Prophet on %d days of history...", len(history))
    model = Prophet(weekly_seasonality=True, yearly_seasonality=True)
    model.fit(history)

    future = model.make_future_dataframe(periods=horizon_days, freq="D")
    forecast = model.predict(future)

    # Order volume is non-negative; clip the point and interval estimates.
    cols = ["yhat", "yhat_lower", "yhat_upper"]
    forecast[cols] = forecast[cols].clip(lower=0)

    logger.info(
        "Forecast complete: %d total days (%d future)",
        len(forecast), horizon_days,
    )
    return forecast[["ds"] + cols]


def build_output(history: pd.DataFrame, forecast: pd.DataFrame) -> pd.DataFrame:
    """
    Combine actuals and predictions into one Metabase-friendly frame.

    One row per day across the whole timeline. `actual` is populated only for
    historical days (null in the future), so a Metabase line chart can overlay
    the real series against the forecast band on a single axis.

    Args:
        history: Historical (ds, y) frame.
        forecast: Output of fit_and_forecast.

    Returns:
        DataFrame: ds, actual, yhat, yhat_lower, yhat_upper.
    """
    merged = forecast.merge(
        history.rename(columns={"y": "actual"}), on="ds", how="left"
    )
    return merged[["ds", "actual", "yhat", "yhat_lower", "yhat_upper"]]


# ---------------------------------------------------------------------------
# Write outputs
# ---------------------------------------------------------------------------
def write_to_snowflake(df: pd.DataFrame, table: str = TARGET_TABLE) -> None:
    """
    Replace the forecast table in Snowflake with the latest run.

    Uses write_pandas with auto-create + overwrite so Metabase always reads a
    single, current forecast table. Column names are uppercased to match
    Snowflake's default identifier casing.

    Args:
        df: Output of build_output.
        table: Destination table name.
    """
    out = df.copy()
    out.columns = [c.upper() for c in out.columns]

    logger.info("Writing %d rows to Snowflake table %s.%s", len(out), FORECAST_SCHEMA, table)
    conn = _get_connection()
    try:
        write_pandas(
            conn,
            out,
            table_name=table.upper(),
            auto_create_table=True,
            overwrite=True,
        )
    finally:
        conn.close()
    logger.info("Snowflake write-back complete.")


def write_csv(df: pd.DataFrame, path: str = FORECAST_CSV) -> None:
    """Write the combined actual/forecast frame to CSV for Metabase / sharing."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info("Wrote forecast CSV -> %s", path)


def save_chart(df: pd.DataFrame, path: str = FORECAST_PNG) -> None:
    """
    Save a quick actual-vs-forecast PNG for a visual sanity check.

    Not required by Metabase (which charts from the table/CSV), just a fast way to
    eyeball the fit after a run.
    """
    import matplotlib
    matplotlib.use("Agg")  # headless: no display needed
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(df["ds"], df["actual"], label="actual", linewidth=0.8)
    ax.plot(df["ds"], df["yhat"], label="forecast", color="C1")
    ax.fill_between(
        df["ds"], df["yhat_lower"], df["yhat_upper"],
        color="C1", alpha=0.2, label="uncertainty",
    )
    ax.set_title("Daily order volume: actual vs. forecast")
    ax.set_xlabel("date")
    ax.set_ylabel("orders")
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)
    logger.info("Wrote forecast chart -> %s", path)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def run(
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    from_csv: bool = False,
    write_back: bool = True,
    csv_path: Optional[str] = None,
) -> pd.DataFrame:
    """
    Run the forecast stage end to end and return the combined output frame.

    Args:
        horizon_days: Days to forecast beyond the last observation.
        from_csv: Read history from the local CSV instead of Snowflake.
        write_back: Write the forecast table back to Snowflake.
        csv_path: Override the input CSV path (only used when from_csv is True).

    Returns:
        The combined actual/forecast DataFrame (also written to CSV/PNG).
    """
    history = (
        read_from_csv(csv_path or DEFAULT_CSV) if from_csv else read_from_snowflake()
    )
    forecast = fit_and_forecast(history, horizon_days)
    output = build_output(history, forecast)

    write_csv(output)
    save_chart(output)
    if write_back:
        write_to_snowflake(output)

    return output


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Forecast daily order volume with Prophet.")
    parser.add_argument(
        "--horizon", type=int, default=DEFAULT_HORIZON_DAYS,
        help=f"Days to forecast ahead (default: {DEFAULT_HORIZON_DAYS}).",
    )
    parser.add_argument(
        "--from-csv", action="store_true",
        help="Read history from output-files CSV instead of Snowflake.",
    )
    parser.add_argument(
        "--csv-path", default=None,
        help="Override input CSV path (implies --from-csv data shape).",
    )
    parser.add_argument(
        "--no-write-back", action="store_true",
        help="Skip writing the forecast table back to Snowflake.",
    )
    args = parser.parse_args(argv)

    run(
        horizon_days=args.horizon,
        from_csv=args.from_csv or args.csv_path is not None,
        write_back=not args.no_write_back,
        csv_path=args.csv_path,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    main()