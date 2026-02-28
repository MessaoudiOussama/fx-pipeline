"""
config.py – Central configuration for the FX pipeline.
All tuneable parameters live here so nothing is hard-coded elsewhere.
"""

import os
from datetime import date

# ---------------------------------------------------------------------------
# Currencies
# All cross-pairs among these 7 currencies will be computed (7×6 = 42 pairs).
# ---------------------------------------------------------------------------
CURRENCIES: list[str] = ["NOK", "EUR", "SEK", "PLN", "RON", "DKK", "CZK"]

# EUR is used as the triangulation base when calling the API.
# One API call per date range is enough to derive all 42 pairs.
BASE_CURRENCY: str = "EUR"

CURRENCY_NAMES: dict[str, str] = {
    "NOK": "Norwegian Krone",
    "EUR": "Euro",
    "SEK": "Swedish Krona",
    "PLN": "Polish Zloty",
    "RON": "Romanian Leu",
    "DKK": "Danish Krone",
    "CZK": "Czech Koruna",
}

# ---------------------------------------------------------------------------
# FX Data Source – Frankfurter API (https://www.frankfurter.app/)
# Free, no API key, backed by the European Central Bank (ECB).
# Rates are published on ECB business days only (weekends/holidays skipped).
# ---------------------------------------------------------------------------
API_BASE_URL: str = "https://api.frankfurter.app"
API_TIMEOUT_SECONDS: int = 30

# ---------------------------------------------------------------------------
# Default time window: full current calendar year up to today.
# Can be overridden at runtime via CLI args in pipeline.py.
# ---------------------------------------------------------------------------
LOAD_START_DATE: str = date(date.today().year, 1, 1).isoformat()
LOAD_END_DATE: str = date.today().isoformat()

# ---------------------------------------------------------------------------
# Database – SQLite file sitting next to this config.
# ---------------------------------------------------------------------------
DB_PATH: str = os.path.join(os.path.dirname(__file__), "fx_warehouse.duckdb")
