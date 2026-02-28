"""
pipeline.py – Entry point for the FX rate ingestion pipeline.

Usage
-----
# Run with defaults (Jan 1 of current year → today)
    uv run python pipeline.py

# Run for a specific date range
    uv run python pipeline.py --start-date 2025-01-01 --end-date 2025-12-31

Flow
----
    Extract  →  fetch daily FX rates from Frankfurter API
    Transform → compute all 42 cross-pairs via EUR triangulation (Polars)
    Load      → write star schema into DuckDB (fx_warehouse.duckdb)
"""

import argparse
import logging
import sys
import time

import os

from config import LOAD_END_DATE, LOAD_START_DATE
from etl.extract import fetch_fx_rates
from etl.transform import compute_cross_pairs

# Auto-detect environment:
# If ADLS_CONNECTION_STRING is set we are running inside Azure Functions
# and should write Parquet to ADLS Gen2.
# Otherwise we are running locally and write to DuckDB.
if os.environ.get("ADLS_CONNECTION_STRING"):
    from etl.load_azure import load_azure as load
else:
    from etl.load import load

# ---------------------------------------------------------------------------
# Logging – structured, timestamped output to stdout
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run(start_date: str, end_date: str) -> None:
    logger.info("=" * 60)
    logger.info("FX Pipeline starting | %s → %s", start_date, end_date)
    logger.info("=" * 60)

    t0 = time.perf_counter()

    # --- Extract ---
    logger.info("[1/3] Extracting FX rates from Frankfurter API...")
    raw = fetch_fx_rates(start_date, end_date)

    # --- Transform ---
    logger.info("[2/3] Computing cross-pairs...")
    df = compute_cross_pairs(raw)

    # --- Load ---
    logger.info("[3/3] Loading into DuckDB warehouse...")
    load(df)

    elapsed = time.perf_counter() - t0
    logger.info("=" * 60)
    logger.info("Pipeline complete in %.2fs | %d records loaded", elapsed, len(df))
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FX Rate ingestion pipeline – fetches ECB rates and loads into DuckDB."
    )
    parser.add_argument(
        "--start-date",
        default=LOAD_START_DATE,
        help=f"Start date in YYYY-MM-DD format (default: {LOAD_START_DATE})",
    )
    parser.add_argument(
        "--end-date",
        default=LOAD_END_DATE,
        help=f"End date in YYYY-MM-DD format (default: {LOAD_END_DATE})",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.start_date, args.end_date)
