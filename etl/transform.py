"""
etl/transform.py – Computes all 42 directed cross-pairs from EUR-based rates.

Cross-rate formula: rate(A → B) = rate(EUR → B) / rate(EUR → A)
EUR is injected at 1.0 so pairs involving EUR go through the same formula.
"""

import logging
from itertools import permutations

import polars as pl

from config import BASE_CURRENCY, CURRENCIES

logger = logging.getLogger(__name__)


def compute_cross_pairs(raw_rates: dict[str, dict[str, float]]) -> pl.DataFrame:
    """
    Compute all directed cross-pairs from EUR-based raw rates.

    Parameters
    ----------
    raw_rates : dict[str, dict[str, float]]
        Output of extract.fetch_fx_rates().

    Returns
    -------
    pl.DataFrame with columns: date, from_currency, to_currency, rate.
    """
    records = []

    for date_str, rates_vs_eur in raw_rates.items():
        full_rates = {BASE_CURRENCY: 1.0, **rates_vs_eur}

        for from_ccy, to_ccy in permutations(CURRENCIES, 2):
            if from_ccy not in full_rates or to_ccy not in full_rates:
                logger.warning("Missing rate for %s or %s on %s — skipping", from_ccy, to_ccy, date_str)
                continue

            cross_rate = full_rates[to_ccy] / full_rates[from_ccy]

            records.append({
                "date": date_str,
                "from_currency": from_ccy,
                "to_currency": to_ccy,
                "rate": round(cross_rate, 6),
            })

    df = (
        pl.DataFrame(records)
        .with_columns(pl.col("date").str.to_date())
        .sort(["date", "from_currency", "to_currency"])
    )

    logger.info(
        "Transformation done | %d records | %d trading days | %d pairs per day",
        len(df),
        df["date"].n_unique(),
        len(df) // df["date"].n_unique(),
    )

    return df
