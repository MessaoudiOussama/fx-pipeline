"""
config.py – Central configuration for the FX pipeline.
All tuneable parameters live here so nothing is hard-coded elsewhere.
"""

import os
from datetime import date

CURRENCIES: list[str] = ["NOK", "EUR", "SEK", "PLN", "RON", "DKK", "CZK"]
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

API_BASE_URL: str = "https://api.frankfurter.dev/v1"
API_TIMEOUT_SECONDS: int = 30

# Default window: full current year up to today. Overridable via CLI args.
LOAD_START_DATE: str = date(date.today().year, 1, 1).isoformat()
LOAD_END_DATE: str = date.today().isoformat()

DB_PATH: str = os.path.join(os.path.dirname(__file__), "fx_warehouse.duckdb")
