-- =============================================================================
-- synapse/setup.sql
-- One-time setup script for Synapse Serverless SQL Pool.
-- Run this once in Synapse Studio after the first pipeline execution.
--
-- What this does:
--   1. Creates a master key (required for external data sources)
--   2. Creates a credential using the Synapse managed identity
--      (no passwords — Synapse authenticates to ADLS via its Azure identity)
--   3. Creates an external data source pointing to our ADLS container
--   4. Creates a Parquet file format
--   5. Creates external tables for dim_currency, dim_date, fact_fx_rates
-- =============================================================================


-- 1. Master key — required once per database
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'FxP!peline2026';


-- 2. Credential — uses the Synapse workspace managed identity
--    No storage key or SAS token needed — Azure handles auth internally
CREATE DATABASE SCOPED CREDENTIAL adls_managed_identity
WITH IDENTITY = 'Managed Identity';


-- 3. External data source — points to our ADLS Gen2 container
CREATE EXTERNAL DATA SOURCE adls_fx_data
WITH (
    LOCATION   = 'https://stfxpipeline09cf8a69.dfs.core.windows.net/fx-data',
    CREDENTIAL = adls_managed_identity
);


-- 4. Parquet file format
CREATE EXTERNAL FILE FORMAT parquet_format
WITH (FORMAT_TYPE = PARQUET);


-- 5. dim_currency — 7 rows, one per currency
CREATE EXTERNAL TABLE dim_currency (
    currency_code VARCHAR(3),
    currency_name VARCHAR(50)
)
WITH (
    LOCATION    = 'dim/dim_currency.parquet',
    DATA_SOURCE = adls_fx_data,
    FILE_FORMAT = parquet_format
);


-- 6. dim_date — one row per ECB trading day
CREATE EXTERNAL TABLE dim_date (
    full_date  DATE,
    year       INT,
    month      INT,
    quarter    INT,
    day        INT,
    is_weekend BIT
)
WITH (
    LOCATION    = 'dim/dim_date.parquet',
    DATA_SOURCE = adls_fx_data,
    FILE_FORMAT = parquet_format
);


-- 7. fact_fx_rates — partitioned by year/month (Hive partitioning)
--    The /** wildcard tells Synapse to scan all subfolders recursively.
--    Synapse automatically uses the year=/month= folder names for
--    partition pruning when queries filter on those columns.
CREATE EXTERNAL TABLE fact_fx_rates (
    full_date          DATE,
    from_currency_code VARCHAR(3),
    to_currency_code   VARCHAR(3),
    rate               FLOAT,
    year               INT,
    month              INT
)
WITH (
    LOCATION    = 'fact/fact_fx_rates/**',
    DATA_SOURCE = adls_fx_data,
    FILE_FORMAT = parquet_format
);


-- =============================================================================
-- Validation queries — run after the first pipeline execution
-- =============================================================================

-- Row counts
SELECT 'dim_currency'  AS tbl, COUNT(*) AS rows FROM dim_currency
UNION ALL
SELECT 'dim_date',              COUNT(*)         FROM dim_date
UNION ALL
SELECT 'fact_fx_rates',         COUNT(*)         FROM fact_fx_rates;

-- Latest EUR rates
SELECT   d.full_date, f.from_currency_code, f.to_currency_code, f.rate
FROM     fact_fx_rates f
JOIN     dim_date d ON d.full_date = f.full_date
WHERE    d.full_date = (SELECT MAX(full_date) FROM dim_date)
  AND    f.from_currency_code = 'EUR'
ORDER BY f.to_currency_code;

-- YTD average rate for EUR pairs
SELECT
    from_currency_code,
    to_currency_code,
    MIN(full_date)        AS ytd_start,
    MAX(full_date)        AS ytd_end,
    ROUND(AVG(rate), 6)   AS ytd_avg_rate
FROM   fact_fx_rates
WHERE  year = YEAR(GETDATE())
  AND  from_currency_code = 'EUR'
GROUP BY from_currency_code, to_currency_code
ORDER BY to_currency_code;
