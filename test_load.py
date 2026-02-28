"""
Quick test for etl/load.py
Runs the full E → T → L sequence and queries the DuckDB to verify.
"""

import duckdb
from etl.extract import fetch_fx_rates
from etl.transform import compute_cross_pairs
from etl.load import load
from config import DB_PATH

# --- Run E → T → L ---
raw = fetch_fx_rates(start_date="2026-02-17", end_date="2026-02-21")
df  = compute_cross_pairs(raw)
load(df)

# --- Verify what landed in DuckDB ---
with duckdb.connect(DB_PATH) as conn:

    print("=== dim_currency ===")
    print(conn.execute("SELECT * FROM dim_currency ORDER BY currency_id").pl())

    print("\n=== dim_date ===")
    print(conn.execute("SELECT * FROM dim_date ORDER BY full_date").pl())

    print("\n=== fact_fx_rates (first 10 rows) ===")
    print(conn.execute("""
        SELECT
            d.full_date,
            fc.currency_code AS from_currency,
            tc.currency_code AS to_currency,
            f.rate
        FROM fact_fx_rates f
        JOIN dim_date     d  ON d.date_id      = f.date_id
        JOIN dim_currency fc ON fc.currency_id = f.from_currency_id
        JOIN dim_currency tc ON tc.currency_id = f.to_currency_id
        ORDER BY d.full_date, fc.currency_code, tc.currency_code
        LIMIT 10
    """).pl())

    print("\n=== Row counts ===")
    print(conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM dim_currency)  AS currencies,
            (SELECT COUNT(*) FROM dim_date)       AS dates,
            (SELECT COUNT(*) FROM fact_fx_rates)  AS fx_rates
    """).pl())
