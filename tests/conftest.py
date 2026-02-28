"""
Shared pytest fixtures for the FX pipeline test suite.
"""

import pytest


# A small hardcoded raw payload — same structure as what fetch_fx_rates() returns.
# Using a fixed dataset means tests are fast, deterministic, and don't hit the API.
@pytest.fixture
def raw_rates():
    return {
        "2026-02-17": {"NOK": 11.74, "SEK": 11.23, "PLN": 4.21, "RON": 4.98, "DKK": 7.46, "CZK": 25.10},
        "2026-02-18": {"NOK": 11.76, "SEK": 11.25, "PLN": 4.22, "RON": 4.97, "DKK": 7.46, "CZK": 25.12},
    }
