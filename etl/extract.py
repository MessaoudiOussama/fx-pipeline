"""
etl/extract.py – Calls the Frankfurter API to fetch daily EUR-based FX rates.
"""

import logging

import requests

from config import API_BASE_URL, API_TIMEOUT_SECONDS, BASE_CURRENCY, CURRENCIES

logger = logging.getLogger(__name__)


def fetch_fx_rates(start_date: str, end_date: str) -> dict[str, dict[str, float]]:
    """
    Fetch EUR-based daily FX rates for the given date range.

    Parameters
    ----------
    start_date : str – ISO format, e.g. "2026-01-01"
    end_date   : str – ISO format, e.g. "2026-02-27"

    Returns
    -------
    dict[str, dict[str, float]]
        Maps each trading-day date string to a dict of {currency: rate_vs_EUR}.
    """
    targets = ",".join(c for c in CURRENCIES if c != BASE_CURRENCY)

    url = f"{API_BASE_URL}/{start_date}..{end_date}"
    params = {"base": BASE_CURRENCY, "symbols": targets}

    logger.info("Calling Frankfurter API | %s | params=%s", url, params)

    try:
        response = requests.get(url, params=params, timeout=API_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        logger.error("Frankfurter API error: %s", exc)
        raise

    data = response.json()
    rates: dict[str, dict[str, float]] = data.get("rates", {})

    logger.info(
        "Extraction done | %d trading days fetched | %s → %s",
        len(rates),
        data.get("start_date"),
        data.get("end_date"),
    )

    return rates
