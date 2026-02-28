"""
etl/load.py – Load layer.

Writes the transformed Polars DataFrame into a DuckDB database
following a star schema designed for easy joining with other DWH tables.

Why DuckDB?
-----------
- Columnar, OLAP-oriented database — the right fit for a data warehouse.
- Natively reads Polars DataFrames with zero serialisation overhead.
- Produces a single portable file artifact (.duckdb), as requested by the brief.
- Locally mirrors what Azure Synapse Analytics does in production.
- Supports full SQL including window functions needed for YTD calculations.

Schema (star schema)
--------------------
    dim_currency          dim_date
    ─────────────         ──────────────────
    currency_id (PK)      date_id (PK)
    currency_code         full_date
    currency_name         year
                          month
                          quarter
                          day
                          is_weekend

                fact_fx_rates
                ──────────────────────────────
                rate_id (PK)
                date_id          (FK → dim_date)
                from_currency_id (FK → dim_currency)
                to_currency_id   (FK → dim_currency)
                rate
                created_at
"""

import logging

import duckdb
import polars as pl

from config import CURRENCIES, CURRENCY_NAMES, DB_PATH

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL = """
CREATE SEQUENCE IF NOT EXISTS seq_currency_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_date_id     START 1;
CREATE SEQUENCE IF NOT EXISTS seq_rate_id     START 1;

CREATE TABLE IF NOT EXISTS dim_currency (
    currency_id   INTEGER PRIMARY KEY DEFAULT nextval('seq_currency_id'),
    currency_code VARCHAR(3)  NOT NULL UNIQUE,
    currency_name VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id    INTEGER PRIMARY KEY DEFAULT nextval('seq_date_id'),
    full_date  DATE    NOT NULL UNIQUE,
    year       INTEGER NOT NULL,
    month      INTEGER NOT NULL,
    quarter    INTEGER NOT NULL,
    day        INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_fx_rates (
    rate_id          INTEGER PRIMARY KEY DEFAULT nextval('seq_rate_id'),
    date_id          INTEGER NOT NULL REFERENCES dim_date(date_id),
    from_currency_id INTEGER NOT NULL REFERENCES dim_currency(currency_id),
    to_currency_id   INTEGER NOT NULL REFERENCES dim_currency(currency_id),
    rate             DOUBLE  NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT current_timestamp,
    UNIQUE (date_id, from_currency_id, to_currency_id)
);
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_dim_currency(conn: duckdb.DuckDBPyConnection) -> None:
    """Insert currencies — skip if already present (idempotent)."""
    for code in CURRENCIES:
        conn.execute("""
            INSERT INTO dim_currency (currency_code, currency_name)
            VALUES (?, ?)
            ON CONFLICT (currency_code) DO NOTHING
        """, [code, CURRENCY_NAMES[code]])
    logger.info("dim_currency loaded (%d currencies)", len(CURRENCIES))


def _load_dim_date(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> None:
    """Insert one row per unique trading date — skip if already present."""
    dates = df["date"].unique().sort()
    for d in dates:
        conn.execute("""
            INSERT INTO dim_date (full_date, year, month, quarter, day, is_weekend)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (full_date) DO NOTHING
        """, [
            d,
            d.year,
            d.month,
            (d.month - 1) // 3 + 1,   # quarter
            d.day,
            d.weekday() >= 5,          # Saturday=5, Sunday=6
        ])
    logger.info("dim_date loaded (%d dates)", len(dates))


def _load_fact(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> None:
    """
    Load fact_fx_rates.

    DuckDB can query a Polars DataFrame directly via the 'df' variable —
    no need to convert to pandas or write to a file first.
    The INSERT ... ON CONFLICT makes the load idempotent (safe to re-run).
    """
    conn.execute("""
        INSERT INTO fact_fx_rates (date_id, from_currency_id, to_currency_id, rate)
        SELECT
            d.date_id,
            fc.currency_id,
            tc.currency_id,
            df.rate
        FROM df
        JOIN dim_date     d  ON d.full_date      = df.date
        JOIN dim_currency fc ON fc.currency_code = df.from_currency
        JOIN dim_currency tc ON tc.currency_code = df.to_currency
        ON CONFLICT (date_id, from_currency_id, to_currency_id) DO NOTHING
    """)
    logger.info("fact_fx_rates loaded (%d rows inserted)", len(df))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def load(df: pl.DataFrame) -> None:
    """
    Full load sequence: init schema → dim_currency → dim_date → fact.

    Parameters
    ----------
    df : pl.DataFrame
        Output of transform.compute_cross_pairs().
    """
    logger.info("Connecting to DuckDB at: %s", DB_PATH)

    with duckdb.connect(DB_PATH) as conn:
        conn.execute(DDL)
        _load_dim_currency(conn)
        _load_dim_date(conn, df)
        _load_fact(conn, df)

    logger.info("Load complete — DuckDB warehouse: %s", DB_PATH)
