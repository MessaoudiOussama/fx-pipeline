# Design Note — FX Rate Ingestion Pipeline

This document explains the key decisions and trade-offs made in this pipeline.

---

## 1. Data Source — Frankfurter API

**Choice:** [Frankfurter API](https://www.frankfurter.app/)

**Justification:**
- Free, no API key required — removes any setup friction for reviewers.
- Backed by the European Central Bank (ECB) — an authoritative, widely trusted
  source for reference rates used in accounting and financial reporting.
- Supports date-range queries in a single HTTP call, keeping extraction efficient.
- Returns exactly one rate per currency per business day, which matches the
  intended use case (daily FX for reporting, not intraday trading).

**Trade-off:** ECB rates are reference rates, not tradeable market rates. For a
debt recovery company reporting portfolio values in different currencies, this is
the appropriate source. For real-time trading or intraday exposure management,
a paid provider (Bloomberg, Refinitiv) would be required.

**Time window:** Defaults to January 1 of the current year → today, giving a
full YTD dataset on every run. Configurable via `--start-date` / `--end-date`.

---

## 2. Cross-Pair Computation — EUR Triangulation

**Choice:** Use EUR as a common base to derive all 42 cross-pairs.

**Formula:**
```
rate(A → B) = rate(EUR → B) / rate(EUR → A)
```

**Justification:**
- The Frankfurter API only returns rates relative to a chosen base currency.
- Using EUR as the base requires **one API call** per date range.
- Fetching each currency as base separately would require 7 API calls — 7× the
  network overhead, 7× the rate-limit risk, identical results.
- EUR is the natural base for this company (operating across the eurozone and
  pegged/closely-tracked currencies like DKK).

**Trade-off:** Cross-rates derived via triangulation may differ fractionally from
directly quoted interbank rates due to bid-ask spreads. For reference/reporting
purposes (the intended use), this is negligible.

---

## 3. Transformation — Polars

**Choice:** [Polars](https://pola.rs/) for the transformation layer.

**Justification:**
- Columnar, Rust-backed DataFrame library — significantly faster than pandas for
  the kind of row-generation and sorting this step performs.
- Native integration with DuckDB via Apache Arrow — no serialisation overhead
  when passing the DataFrame to the load layer.
- Explicitly listed in the job description as a desired skill.

**Trade-off:** Polars has a steeper learning curve than pandas and its API
differs in some areas (e.g. date comparisons require `pl.col()` expressions).
For a dataset of this size (~10K rows/year), pandas would work equally well.
Polars becomes meaningful at scale — which is where this pipeline would grow.

---

## 4. Warehouse — DuckDB (local) / Azure Synapse Analytics (production)

**Choice:** DuckDB locally; Azure Synapse Analytics in production.

**Why not SQLite?**
SQLite is a row-oriented OLTP database. A data warehouse is an OLAP workload —
aggregations, date-range scans, YTD window functions. DuckDB is architecturally
a columnar analytical database, making it the correct local equivalent of
Synapse, Snowflake, or BigQuery.

**Why not Parquet files?**
Parquet is a storage *format*, not a database. Without a query engine (DuckDB,
Spark, Synapse) sitting on top, you cannot run SQL against it. The two are
complementary, not competing: in production, data would be stored as Parquet
files in ADLS Gen2 and queried via Synapse — the Medallion architecture.

**Production path:**
```
Local:       DuckDB (fx_warehouse.duckdb)
Production:  Azure Synapse Analytics (dedicated or serverless SQL pool)
             + Azure Data Lake Storage Gen2 (Parquet — Bronze layer)
```

The only change required in code is swapping `DB_PATH` (a DuckDB file path) for
a Synapse connection string in `config.py`. The DDL and all queries are
standard SQL and run unchanged on Synapse.

---

## 5. Schema — Star Schema

**Choice:** Two dimension tables (`dim_currency`, `dim_date`) and one fact table
(`fact_fx_rates`).

**Justification:**
- The brief explicitly requires the data to be "easy to relate/join with the
  rest of a DWH." A star schema is the standard pattern for this — any other
  fact table (e.g. `fact_payments`, `fact_portfolios`) can join on `date_id` or
  `currency_id` without touching the FX fact table itself.
- `dim_date` pre-computes `year`, `month`, `quarter`, `is_weekend` as columns,
  making YTD queries trivial (`WHERE year = 2026`) with no runtime date parsing.
- Surrogate keys (integer IDs) on all dimension tables follow DWH best practice
  and are more join-efficient than natural keys (strings).

**Trade-off:** A fully normalised schema would store less data, but at the cost
of more complex queries. For a read-heavy analytical workload, denormalisation
via the star schema is the right trade-off.

---

## 6. Idempotency

All load operations use `ON CONFLICT ... DO NOTHING`. Running the pipeline twice
for the same date range will not produce duplicate rows. This is essential for
safe daily scheduling — if a run is retried after a partial failure, the data
remains consistent.

---

## 7. Orchestration — Azure

**Choice:** Azure Functions (Timer Trigger) + Azure Data Factory.

**Azure Functions** runs the Python ETL code serverlessly — no VM or container
to manage. At one execution per day, it falls entirely within the permanent free
tier.

**Azure Data Factory** provides orchestration on top: visual monitoring, retry
logic (2 retries with 5-minute intervals), failure alerting via webhook, and a
clear audit trail of every pipeline run. The trigger runs Monday–Friday only,
matching the ECB publication schedule.

**Why ADF if the Function already has a timer?**
For a single pipeline, the Azure Function timer alone is sufficient. ADF is
justified when you need centralised monitoring across multiple pipelines,
dependency management between activities, or SLA alerting — which is typical
in a production data platform serving multiple countries. At small scale, it
is an optional but low-cost addition (~$1–2/month).

**ADF → Azure Function integration:**
The Function App exposes a single **HTTP trigger** (`POST /api/fx_etl`),
called by ADF's `AzureFunctionActivity` using a function key. Scheduling
is owned entirely by ADF (Mon-Fri 10:00 UTC trigger) — the Function App
is a pure executor with no built-in timer. This keeps a single source of
truth for the schedule and avoids the security concern of exposing the
master key that would be required to trigger a timer function via the
admin endpoint.

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
| CI/CD | GitHub Actions (lint + test); manual deploy via `func` CLI | Full CD via Azure DevOps pipeline |
| Scale | 7 currencies | Parameterise currencies list in config |
