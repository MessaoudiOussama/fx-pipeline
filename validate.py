"""
validate.py – Runs all example queries against fx_warehouse.duckdb and prints results.

Usage
-----
    uv run python validate.py

Purpose
-------
Demonstrates that the warehouse is usable and answers the brief's requirements:
  - Lookups by date and currency pair
  - YTD average rate
  - YTD % change since first trading day of the year
"""

import duckdb
from config import DB_PATH

SEPARATOR = "-" * 70


def run_query(conn: duckdb.DuckDBPyConnection, title: str, sql: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")
    result = conn.execute(sql).pl()
    print(result)


def main() -> None:
    print(f"Connecting to: {DB_PATH}")

    with duckdb.connect(DB_PATH, read_only=True) as conn:

        # --- Q1: Specific date + pair lookup ---
        run_query(conn, "Q1 – NOK → PLN rate on 2026-02-17", """
            SELECT
                d.full_date,
                fc.currency_code  AS from_currency,
                tc.currency_code  AS to_currency,
                f.rate
            FROM fact_fx_rates   f
            JOIN dim_date        d  ON d.date_id      = f.date_id
            JOIN dim_currency    fc ON fc.currency_id = f.from_currency_id
            JOIN dim_currency    tc ON tc.currency_id = f.to_currency_id
            WHERE d.full_date        = '2026-02-17'
              AND fc.currency_code   = 'NOK'
              AND tc.currency_code   = 'PLN'
        """)

        # --- Q2: All pairs for a given date ---
        run_query(conn, "Q2 – All cross-pairs on 2026-02-20", """
            SELECT
                d.full_date,
                fc.currency_code  AS from_currency,
                tc.currency_code  AS to_currency,
                f.rate
            FROM fact_fx_rates   f
            JOIN dim_date        d  ON d.date_id      = f.date_id
            JOIN dim_currency    fc ON fc.currency_id = f.from_currency_id
            JOIN dim_currency    tc ON tc.currency_id = f.to_currency_id
            WHERE d.full_date = '2026-02-20'
            ORDER BY fc.currency_code, tc.currency_code
        """)

        # --- Q3: Latest rates for EUR ---
        run_query(conn, "Q3 – Latest available EUR rates", """
            SELECT
                d.full_date,
                fc.currency_code  AS from_currency,
                tc.currency_code  AS to_currency,
                f.rate
            FROM fact_fx_rates   f
            JOIN dim_date        d  ON d.date_id      = f.date_id
            JOIN dim_currency    fc ON fc.currency_id = f.from_currency_id
            JOIN dim_currency    tc ON tc.currency_id = f.to_currency_id
            WHERE d.full_date      = (SELECT MAX(full_date) FROM dim_date)
              AND fc.currency_code = 'EUR'
            ORDER BY tc.currency_code
        """)

        # --- Q4: YTD average rate ---
        run_query(conn, "Q4 – YTD average rate for all EUR pairs (2026)", """
            SELECT
                fc.currency_code          AS from_currency,
                tc.currency_code          AS to_currency,
                MIN(d.full_date)          AS ytd_start,
                MAX(d.full_date)          AS ytd_end,
                ROUND(AVG(f.rate), 6)     AS ytd_avg_rate
            FROM fact_fx_rates   f
            JOIN dim_date        d  ON d.date_id      = f.date_id
            JOIN dim_currency    fc ON fc.currency_id = f.from_currency_id
            JOIN dim_currency    tc ON tc.currency_id = f.to_currency_id
            WHERE d.year           = YEAR(CURRENT_DATE)
              AND fc.currency_code = 'EUR'
            GROUP BY fc.currency_code, tc.currency_code
            ORDER BY tc.currency_code
        """)

        # --- Q5: YTD % change ---
        run_query(conn, "Q5 – YTD % change for all EUR pairs (first day → latest)", """
            WITH first_rate AS (
                SELECT f.from_currency_id, f.to_currency_id, f.rate AS rate_on_first_day
                FROM fact_fx_rates f
                JOIN dim_date d ON d.date_id = f.date_id
                WHERE d.full_date = (
                    SELECT MIN(full_date) FROM dim_date WHERE year = YEAR(CURRENT_DATE)
                )
            ),
            last_rate AS (
                SELECT f.from_currency_id, f.to_currency_id, f.rate AS rate_on_last_day
                FROM fact_fx_rates f
                JOIN dim_date d ON d.date_id = f.date_id
                WHERE d.full_date = (SELECT MAX(full_date) FROM dim_date)
            )
            SELECT
                fc.currency_code                                         AS from_currency,
                tc.currency_code                                         AS to_currency,
                (SELECT MIN(full_date) FROM dim_date
                    WHERE year = YEAR(CURRENT_DATE))                     AS ytd_start,
                (SELECT MAX(full_date) FROM dim_date)                    AS ytd_end,
                ROUND(fr.rate_on_first_day, 6)                           AS first_rate,
                ROUND(lr.rate_on_last_day,  6)                           AS last_rate,
                ROUND(
                    (lr.rate_on_last_day - fr.rate_on_first_day)
                    / fr.rate_on_first_day * 100
                , 4)                                                     AS ytd_change_pct
            FROM first_rate      fr
            JOIN last_rate       lr ON lr.from_currency_id = fr.from_currency_id
                                   AND lr.to_currency_id   = fr.to_currency_id
            JOIN dim_currency    fc ON fc.currency_id      = fr.from_currency_id
            JOIN dim_currency    tc ON tc.currency_id      = fr.to_currency_id
            WHERE fc.currency_code = 'EUR'
            ORDER BY tc.currency_code
        """)


if __name__ == "__main__":
    main()
