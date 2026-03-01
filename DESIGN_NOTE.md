# Design Note — FX Rate Ingestion Pipeline

This document explains the key decisions and trade-offs made in this pipeline.

---

## 1. Data Source — Frankfurter API

The pipeline uses the [Frankfurter API](https://frankfurter.dev/), which is free,
requires no API key, and is backed by the European Central Bank. It publishes one
official reference rate per currency per business day, which is exactly what a
reporting use case needs. Weekends and ECB holidays are automatically excluded, so
there is no need to filter them out manually.

The API supports date-range queries, so the entire YTD history is fetched in a single
HTTP call rather than looping day by day.

ECB rates are reference rates, not tradeable market rates. For a debt recovery company
reporting portfolio values across currencies, that is the right source. For real-time
trading or intraday exposure management, a paid provider (Bloomberg, Refinitiv) would
be needed instead.

**Time window:** Defaults to January 1 of the current year → today. Configurable via
`--start-date` / `--end-date`.

---

## 2. Cross-Pair Computation — EUR Triangulation

The API returns rates relative to a single base currency. By choosing EUR as that base,
all 42 cross-pairs are derived from one API call using:

```
rate(A → B) = rate(EUR → B) / rate(EUR → A)
```

The alternative — fetching each currency as base separately — would require 7 API calls
for identical results. EUR is also the natural base here given the company's eurozone
operations and closely-tracked currencies like DKK.

Cross-rates derived via triangulation can differ fractionally from directly quoted
interbank rates due to bid-ask spreads, but for reference/reporting purposes the
difference is negligible.

---

## 3. Transformation — Polars

Polars was chosen for the transformation layer. It is a columnar, Rust-backed DataFrame
library that integrates natively with DuckDB via Apache Arrow, meaning the DataFrame
passes to the load step without any serialisation overhead.

For a dataset of this size (~10K rows/year), pandas would work equally well. Polars
becomes meaningful at scale.

---

## 4. Warehouse — DuckDB (local) / Azure Synapse Analytics (production)

A data warehouse is an OLAP workload — aggregations, date-range scans, window
functions. DuckDB is a columnar analytical database and the correct local equivalent of
Synapse, Snowflake, or BigQuery. SQLite (row-oriented OLTP) would technically work but
is architecturally the wrong fit.

In production, data is stored as Parquet files in ADLS Gen2 and queried via Synapse —
Parquet is a storage format, not a database, so a query engine on top is always
required anyway.

```
Local:       DuckDB (fx_warehouse.duckdb)
Production:  ADLS Gen2 (Parquet) + Azure Synapse Analytics (serverless SQL pool)
```

---

## 5. Schema — Star Schema

The schema has two dimension tables (`dim_currency`, `dim_date`) and one fact table
(`fact_fx_rates`). The brief asks for data that is "easy to relate/join with the rest
of a DWH" — a star schema is the standard answer. Any other fact table (payments,
portfolios) can join on `date_id` or `currency_id` without touching the FX table.

`dim_date` pre-computes `year`, `month`, `quarter`, and `is_weekend`, making YTD
queries trivial (`WHERE year = 2026`) with no runtime date parsing.

Surrogate integer keys on all dimension tables follow DWH best practice and are more
join-efficient than natural string keys.

---

## 6. Idempotency

All load operations use `ON CONFLICT ... DO NOTHING`. Running the pipeline twice for
the same date range will not produce duplicate rows, which matters for safe daily
scheduling — if a run is retried after a partial failure, the data remains consistent.

---

## 7. Orchestration — Azure

The pipeline runs serverlessly via Azure Functions (HTTP trigger). At one execution per
day it stays within the permanent free tier. Azure Data Factory sits on top to provide
orchestration: visual monitoring, retry logic (2 retries at 5-minute intervals), failure
alerting, and an audit trail of every run. The trigger fires Monday–Friday only,
matching the ECB publication schedule.

ADF calls the Function App via an `AzureFunctionActivity` using a function key. This
keeps scheduling in one place (ADF) and avoids exposing the admin master key that
would be required to trigger a timer function externally.

For a single pipeline, a built-in timer on the Function App would also work. ADF is
justified when you need centralised monitoring across multiple pipelines, dependency
management between activities, or SLA alerting — typical in a multi-country production
platform.

---

## 8. What Would Change in a Full Production System

| Concern | Current approach | Production extension |
|---------|-----------------|----------------------|
| Raw data retention | Not stored | Raw JSON to ADLS Gen2 (Bronze layer) |
| DWH | DuckDB (local file) | Azure Synapse Analytics |
| Secrets | `config.py` | Azure Key Vault |
| Data quality | Basic logging | Great Expectations or dbt tests |
| Schema evolution | Manual DDL | dbt models + version-controlled migrations |
| Observability | stdout logging | Azure Monitor + Application Insights |
| CI/CD | GitHub Actions (lint + test + deploy on push to main) | Full CD via Azure DevOps pipeline |
| Scale | 7 currencies | Parameterise currencies list in config |
