"""
Tests for etl/transform.py — uses fixture data, no API calls.
"""

import polars as pl
from etl.transform import compute_cross_pairs


def test_returns_polars_dataframe(raw_rates):
    df = compute_cross_pairs(raw_rates)
    assert isinstance(df, pl.DataFrame)


def test_correct_columns(raw_rates):
    df = compute_cross_pairs(raw_rates)
    assert set(df.columns) == {"date", "from_currency", "to_currency", "rate"}


def test_correct_number_of_rows(raw_rates):
    # 2 trading days × 42 cross-pairs (7×6 permutations, no A→A) = 84 rows
    df = compute_cross_pairs(raw_rates)
    assert len(df) == 84


def test_all_rates_positive(raw_rates):
    df = compute_cross_pairs(raw_rates)
    assert df["rate"].min() > 0


def test_cross_rate_formula(raw_rates):
    # rate(NOK→SEK) = EUR→SEK / EUR→NOK
    from datetime import date
    df = compute_cross_pairs(raw_rates)
    expected = raw_rates["2026-02-17"]["SEK"] / raw_rates["2026-02-17"]["NOK"]
    row = df.filter(
        (pl.col("date") == date(2026, 2, 17)) &
        (pl.col("from_currency") == "NOK") &
        (pl.col("to_currency") == "SEK")
    )
    assert abs(row["rate"][0] - expected) < 1e-6
