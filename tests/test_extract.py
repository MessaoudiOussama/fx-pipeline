"""
Tests for etl/extract.py — mocks the API to avoid network dependency.
"""

from unittest.mock import patch, Mock
from etl.extract import fetch_fx_rates


MOCK_RESPONSE = {
    "2026-02-17": {"NOK": 11.74, "SEK": 11.23, "PLN": 4.21, "RON": 4.98, "DKK": 7.46, "CZK": 25.10},
    "2026-02-18": {"NOK": 11.76, "SEK": 11.25, "PLN": 4.22, "RON": 4.97, "DKK": 7.46, "CZK": 25.12},
}


def _mock_get(*args, **kwargs):
    mock = Mock()
    mock.raise_for_status = Mock()
    mock.json.return_value = {
        "rates": {date: currencies for date, currencies in MOCK_RESPONSE.items()}
    }
    return mock


def test_returns_dict():
    with patch("etl.extract.requests.get", side_effect=_mock_get):
        rates = fetch_fx_rates(start_date="2026-02-17", end_date="2026-02-18")
    assert isinstance(rates, dict)


def test_returns_correct_dates():
    with patch("etl.extract.requests.get", side_effect=_mock_get):
        rates = fetch_fx_rates(start_date="2026-02-17", end_date="2026-02-18")
    assert set(rates.keys()) == {"2026-02-17", "2026-02-18"}


def test_rates_are_positive():
    with patch("etl.extract.requests.get", side_effect=_mock_get):
        rates = fetch_fx_rates(start_date="2026-02-17", end_date="2026-02-18")
    for date, currencies in rates.items():
        for currency, rate in currencies.items():
            assert rate > 0
