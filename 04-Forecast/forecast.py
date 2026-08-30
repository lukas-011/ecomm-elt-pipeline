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




if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    main()