"""
etl/extract.py – Extraction layer.

Calls the Frankfurter API to fetch daily EUR-based FX rates for a date range.

Why Frankfurter?
- Free, no API key required.
- Backed by the European Central Bank (ECB) — an authoritative source.
- Supports date-range queries in a single HTTP call, keeping things efficient.
- Only returns business days (weekends/ECB holidays are automatically skipped).

API call we make:
  GET https://api.frankfurter.dev/v1/{start}..{end}?base=EUR&symbols=NOK,SEK,PLN,RON,DKK,CZK

Example response:
  {
    "base": "EUR",
    "start_date": "2026-01-02",
    "end_date":   "2026-02-27",
    "rates": {
      "2026-01-02": {"NOK": 11.87, "SEK": 11.25, "PLN": 4.27, ...},
      "2026-01-03": {"NOK": 11.90, "SEK": 11.28, "PLN": 4.29, ...},
      ...
    }
  }
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
        Example:
            {
              "2026-01-02": {"NOK": 11.87, "SEK": 11.25, ...},
              "2026-01-03": {"NOK": 11.90, "SEK": 11.28, ...},
            }
    """
    # Build target list — everything except the base (EUR vs EUR = 1.0, trivial)
    targets = ",".join(c for c in CURRENCIES if c != BASE_CURRENCY)

    url = f"{API_BASE_URL}/{start_date}..{end_date}"
    params = {"base": BASE_CURRENCY, "symbols": targets}

    logger.info("Calling Frankfurter API | %s | params=%s", url, params)

    try:
        response = requests.get(url, params=params, timeout=API_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        logger.error("HTTP error from Frankfurter API: %s", exc)
        raise
    except requests.exceptions.RequestException as exc:
        logger.error("Network error reaching Frankfurter API: %s", exc)
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
