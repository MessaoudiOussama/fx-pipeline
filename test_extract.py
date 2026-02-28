"""
Quick test for etl/extract.py
Fetches 5 days of data and prints the raw output.
"""

import json
from etl.extract import fetch_fx_rates

rates = fetch_fx_rates(start_date="2026-02-17", end_date="2026-02-21")

print(f"Number of trading days returned: {len(rates)}\n")
print(json.dumps(rates, indent=2))
