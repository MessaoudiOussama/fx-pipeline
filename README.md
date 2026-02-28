# FX Rate Ingestion Pipeline

A production-grade ETL pipeline that ingests daily foreign-exchange (FX) rates
for 7 European currencies, computes all 42 cross-pairs, and loads them into a
star-schema data warehouse — locally via DuckDB, and on Azure via Synapse Analytics.

---

## Currencies

`NOK` `EUR` `SEK` `PLN` `RON` `DKK` `CZK`
All directed cross-pairs are computed: **7 × 6 = 42 pairs per trading day**.

---

## Project Structure

```
fx_pipeline/
├── config.py                        ← All settings (currencies, dates, DB path)
├── pipeline.py                      ← Entry point: runs Extract → Transform → Load
├── validate.py                      ← Runs example queries to verify output
├── etl/
│   ├── extract.py                   ← Fetches rates from Frankfurter API (ECB)
│   ├── transform.py                 ← Computes cross-pairs via EUR triangulation (Polars)
│   └── load.py                      ← Writes star schema to DuckDB
├── queries/
│   └── example_queries.sql          ← Reference SQL: lookups, YTD average, YTD % change
├── orchestration/
│   ├── azure_function/
│   │   ├── __init__.py              ← Azure Function entry point (Timer Trigger)
│   │   └── function.json            ← Cron schedule: daily at 10:00 UTC
│   ├── adf_pipeline.json            ← Azure Data Factory pipeline definition
│   └── adf_trigger.json             ← ADF schedule trigger (weekdays only)
├── fx_warehouse.duckdb              ← Generated — the local fake DWH
└── README.md
```

---

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) (modern Python package manager)

---

## Quickstart

### 1. Install dependencies
```bash
uv sync
```

### 2. Run the pipeline (defaults to Jan 1 of current year → today)
```bash
uv run python pipeline.py
```

### 3. Run for a custom date range
```bash
uv run python pipeline.py --start-date 2025-01-01 --end-date 2025-12-31
```

### 4. Validate the output
```bash
uv run python -X utf8 validate.py
```

---

## How to Validate Outputs

`validate.py` runs 5 queries against `fx_warehouse.duckdb` and prints results:

| Query | What it checks |
|-------|----------------|
| Q1 | Rate for a specific date and currency pair |
| Q2 | All 42 cross-pairs for a given date |
| Q3 | Latest available rates for EUR |
| Q4 | YTD average rate (Jan 1 → latest date) |
| Q5 | YTD % change (first trading day → latest) |

Expected row counts after a full YTD run:
- `dim_currency` → 7 rows
- `dim_date` → ~N trading days (weekends and ECB holidays excluded)
- `fact_fx_rates` → N × 42 rows

---

## YTD Definition

**Year-to-Date** is defined as the period from the **first ECB trading day of the
current calendar year** to the **latest available date in the warehouse**.

Two YTD metrics are provided:
- `ytd_avg_rate` — average of all daily closing rates in the YTD window
- `ytd_change_pct` — percentage change from the first rate of the year to the last

---

## Data Source

**[Frankfurter API](https://www.frankfurter.app/)** — free, no API key required,
backed by the European Central Bank (ECB). Publishes one official reference rate
per currency per business day at ~16:00 CET. Weekends and ECB public holidays
are automatically excluded.

---

## Azure Deployment

See `orchestration/` for the production architecture:

```
Azure Data Factory (daily trigger, monitoring, alerts)
        │
        ▼
Azure Functions (runs ETL code, Timer Trigger: 10:00 UTC)
        │
        ├──► Azure Data Lake Storage Gen2 (raw JSON — Bronze layer)
        │
        └──► Azure Synapse Analytics (star schema — Gold layer)

Azure Key Vault — stores all secrets and connection strings
```

See `DESIGN_NOTE.md` for full architecture rationale and trade-offs.
