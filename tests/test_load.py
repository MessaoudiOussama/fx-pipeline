"""
Tests for etl/load.py — uses a temporary DuckDB, no real database touched.
"""

import duckdb
from etl.transform import compute_cross_pairs
from etl.load import load
from config import CURRENCIES


def test_tables_created(raw_rates, tmp_path, monkeypatch):
    monkeypatch.setattr("etl.load.DB_PATH", str(tmp_path / "test.duckdb"))
    df = compute_cross_pairs(raw_rates)
    load(df)

    with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as conn:
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    assert {"dim_currency", "dim_date", "fact_fx_rates"}.issubset(tables)


def test_correct_row_counts(raw_rates, tmp_path, monkeypatch):
    monkeypatch.setattr("etl.load.DB_PATH", str(tmp_path / "test.duckdb"))
    df = compute_cross_pairs(raw_rates)
    load(df)

    with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as conn:
        currencies = conn.execute("SELECT COUNT(*) FROM dim_currency").fetchone()[0]
        facts = conn.execute("SELECT COUNT(*) FROM fact_fx_rates").fetchone()[0]

    assert currencies == len(CURRENCIES)
    assert facts == 84  # 2 days × 42 pairs


def test_idempotency(raw_rates, tmp_path, monkeypatch):
    """Running the load twice must not produce duplicate rows."""
    monkeypatch.setattr("etl.load.DB_PATH", str(tmp_path / "test.duckdb"))
    df = compute_cross_pairs(raw_rates)
    load(df)
    load(df)  # second run — same data

    with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as conn:
        facts = conn.execute("SELECT COUNT(*) FROM fact_fx_rates").fetchone()[0]
    assert facts == 84
