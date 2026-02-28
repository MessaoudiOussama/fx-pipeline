"""
etl/load_azure.py – Azure load layer.

Writes the transformed DataFrame to ADLS Gen2 as Parquet files.
Synapse Serverless SQL Pool reads these files via external tables.

File structure in ADLS (container: fx-data)
--------------------------------------------
    dim/
        dim_currency.parquet       ← 7 rows, overwritten each run
        dim_date.parquet           ← all trading dates, overwritten each run
    fact/
        fact_fx_rates/
            year=2026/
                month=01/
                    data.parquet
                month=02/
                    data.parquet

Why Parquet + Hive partitioning?
---------------------------------
Synapse Serverless understands the year=/month= folder structure natively.
When a query filters by year or month, Synapse skips irrelevant partitions
entirely (partition pruning) — only the needed files are read.
Example: a YTD query WHERE year = 2026 AND month <= 2 reads 2 files,
not the entire history.
"""

import io
import logging
import os

import polars as pl
from azure.storage.blob import BlobServiceClient

from config import CURRENCIES, CURRENCY_NAMES

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_client() -> BlobServiceClient:
    """Build a BlobServiceClient from the connection string in env vars."""
    conn_str = os.environ["ADLS_CONNECTION_STRING"]
    return BlobServiceClient.from_connection_string(conn_str)


def _upload_parquet(
    client: BlobServiceClient,
    container: str,
    blob_path: str,
    df: pl.DataFrame,
) -> None:
    """Serialize a Polars DataFrame to Parquet and upload to ADLS."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    client.get_container_client(container).upload_blob(
        name=blob_path, data=buf, overwrite=True
    )
    logger.info("Uploaded %-55s (%d rows)", blob_path, len(df))


# ---------------------------------------------------------------------------
# Dimension builders
# ---------------------------------------------------------------------------

def _build_dim_currency() -> pl.DataFrame:
    return pl.DataFrame({
        "currency_code": CURRENCIES,
        "currency_name": [CURRENCY_NAMES[c] for c in CURRENCIES],
    })


def _build_dim_date(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df["date"].unique().sort()
        .to_frame("full_date")
        .with_columns([
            pl.col("full_date").dt.year().alias("year"),
            pl.col("full_date").dt.month().alias("month"),
            pl.col("full_date").dt.quarter().alias("quarter"),
            pl.col("full_date").dt.day().alias("day"),
            (pl.col("full_date").dt.weekday() >= 5).alias("is_weekend"),
        ])
    )


def _build_fact(df: pl.DataFrame) -> pl.DataFrame:
    """
    Rename columns and add year/month for Hive partitioning.
    Natural keys (currency codes) are used instead of surrogate IDs
    — Synapse Serverless JOINs on string keys cleanly, and there is
    no sequence/autoincrement concept in a file-based warehouse.
    """
    return (
        df
        .rename({
            "date":          "full_date",
            "from_currency": "from_currency_code",
            "to_currency":   "to_currency_code",
        })
        .with_columns([
            pl.col("full_date").dt.year().alias("year"),
            pl.col("full_date").dt.month().alias("month"),
        ])
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def load_azure(df: pl.DataFrame) -> None:
    """
    Write all three tables to ADLS Gen2 as Parquet files.

    Parameters
    ----------
    df : pl.DataFrame
        Output of transform.compute_cross_pairs().
    """
    container = os.environ.get("ADLS_CONTAINER_NAME", "fx-data")
    client = _get_client()

    logger.info("Writing to ADLS Gen2 | container: %s", container)

    # Dimensions — overwrite each run (they are small and static/append-only)
    _upload_parquet(client, container, "dim/dim_currency.parquet", _build_dim_currency())
    _upload_parquet(client, container, "dim/dim_date.parquet",     _build_dim_date(df))

    # Fact — partitioned by year/month
    fact = _build_fact(df)
    for (year, month), group in fact.group_by(["year", "month"]):
        path = f"fact/fact_fx_rates/year={year}/month={month:02d}/data.parquet"
        _upload_parquet(client, container, path, group)

    logger.info("Azure load complete.")
