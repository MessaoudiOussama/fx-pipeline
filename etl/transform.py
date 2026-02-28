"""
etl/transform.py – Transformation layer.

Takes the raw EUR-based rates from the extractor and computes all 42
directed cross-pairs using triangulation via EUR as the common base.

Cross-rate formula
------------------
Given:  EUR → A = a   (e.g. EUR → NOK = 11.28)
        EUR → B = b   (e.g. EUR → SEK = 10.65)

Then:   A → B = b / a (e.g. NOK → SEK = 10.65 / 11.28 ≈ 0.9441)

EUR itself is injected with rate 1.0 so that pairs involving EUR
(e.g. EUR → NOK, NOK → EUR) are computed by the same formula without
any special-casing.
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
        Example: {"2026-01-02": {"NOK": 11.87, "SEK": 11.25, ...}, ...}

    Returns
    -------
    pl.DataFrame with columns:
        date           (Date)
        from_currency  (String)
        to_currency    (String)
        rate           (Float64)
    """
    records = []

    for date_str, rates_vs_eur in raw_rates.items():
        # Inject EUR = 1.0 so all 7 currencies are in the dict
        full_rates = {BASE_CURRENCY: 1.0, **rates_vs_eur}

        # Generate all ordered pairs (A → B) where A ≠ B  →  7 × 6 = 42 rows per day
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
