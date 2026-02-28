"""
Quick test for etl/transform.py
Fetches 1 week of data, transforms it, and prints the result.
"""

from datetime import date
import polars as pl

from etl.extract import fetch_fx_rates
from etl.transform import compute_cross_pairs

raw = fetch_fx_rates(start_date="2026-02-17", end_date="2026-02-21")
df = compute_cross_pairs(raw)

print(f"Total rows     : {len(df)}")
print(f"Trading days   : {df['date'].n_unique()}")
print(f"Pairs per day  : {len(df) // df['date'].n_unique()}")
print()

# Show all pairs for a single day so we can verify the math
print("All pairs for 2026-02-17:")
print(df.filter(pl.col("date") == date(2026, 2, 17)))
