-- =============================================================================
-- example_queries.sql
-- Demonstrates how to use the fx_warehouse.duckdb for common lookups and YTD.
--
-- YTD definition used throughout
-- --------------------------------
-- "Year-to-Date" means the period from the FIRST ECB trading day of the
-- current calendar year up to the LATEST available date in the warehouse.
-- Two YTD metrics are provided:
--   1. ytd_avg_rate    – average of all daily closing rates in that window
--   2. ytd_change_pct  – % change from the first rate of the year to the last
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q1. Rate for a specific date and currency pair
--     "What was the NOK → PLN rate on 2026-02-17?"
-- -----------------------------------------------------------------------------
SELECT
    d.full_date,
    fc.currency_code  AS from_currency,
    tc.currency_code  AS to_currency,
    f.rate
FROM fact_fx_rates   f
JOIN dim_date        d  ON d.date_id      = f.date_id
JOIN dim_currency    fc ON fc.currency_id = f.from_currency_id
JOIN dim_currency    tc ON tc.currency_id = f.to_currency_id
WHERE d.full_date        = '2026-02-17'
  AND fc.currency_code   = 'NOK'
  AND tc.currency_code   = 'PLN';


-- -----------------------------------------------------------------------------
-- Q2. All rates for a given date
--     "Show every cross-pair on 2026-02-20"
-- -----------------------------------------------------------------------------
SELECT
    d.full_date,
    fc.currency_code  AS from_currency,
    tc.currency_code  AS to_currency,
    f.rate
FROM fact_fx_rates   f
JOIN dim_date        d  ON d.date_id      = f.date_id
JOIN dim_currency    fc ON fc.currency_id = f.from_currency_id
JOIN dim_currency    tc ON tc.currency_id = f.to_currency_id
WHERE d.full_date = '2026-02-20'
ORDER BY fc.currency_code, tc.currency_code;


-- -----------------------------------------------------------------------------
-- Q3. Latest available rates for EUR
--     "What is EUR worth against all other currencies today?"
-- -----------------------------------------------------------------------------
SELECT
    d.full_date,
    fc.currency_code  AS from_currency,
    tc.currency_code  AS to_currency,
    f.rate
FROM fact_fx_rates   f
JOIN dim_date        d  ON d.date_id      = f.date_id
JOIN dim_currency    fc ON fc.currency_id = f.from_currency_id
JOIN dim_currency    tc ON tc.currency_id = f.to_currency_id
WHERE d.full_date      = (SELECT MAX(full_date) FROM dim_date)
  AND fc.currency_code = 'EUR'
ORDER BY tc.currency_code;


-- -----------------------------------------------------------------------------
-- Q4. YTD average rate for all EUR pairs
--     "What has been the average EUR/X rate so far this year?"
-- -----------------------------------------------------------------------------
SELECT
    fc.currency_code                   AS from_currency,
    tc.currency_code                   AS to_currency,
    MIN(d.full_date)                   AS ytd_start,
    MAX(d.full_date)                   AS ytd_end,
    ROUND(AVG(f.rate), 6)              AS ytd_avg_rate
FROM fact_fx_rates   f
JOIN dim_date        d  ON d.date_id      = f.date_id
JOIN dim_currency    fc ON fc.currency_id = f.from_currency_id
JOIN dim_currency    tc ON tc.currency_id = f.to_currency_id
WHERE d.year           = YEAR(CURRENT_DATE)
  AND fc.currency_code = 'EUR'
GROUP BY fc.currency_code, tc.currency_code
ORDER BY tc.currency_code;


-- -----------------------------------------------------------------------------
-- Q5. YTD % change for all EUR pairs
--     "How much has EUR/X moved since the first trading day of the year?"
-- -----------------------------------------------------------------------------
WITH first_rate AS (
    SELECT
        f.from_currency_id,
        f.to_currency_id,
        f.rate          AS rate_on_first_day
    FROM fact_fx_rates  f
    JOIN dim_date       d ON d.date_id = f.date_id
    WHERE d.full_date = (
        SELECT MIN(full_date) FROM dim_date WHERE year = YEAR(CURRENT_DATE)
    )
),
last_rate AS (
    SELECT
        f.from_currency_id,
        f.to_currency_id,
        f.rate          AS rate_on_last_day
    FROM fact_fx_rates  f
    JOIN dim_date       d ON d.date_id = f.date_id
    WHERE d.full_date = (
        SELECT MAX(full_date) FROM dim_date
    )
)
SELECT
    fc.currency_code                                        AS from_currency,
    tc.currency_code                                        AS to_currency,
    (SELECT MIN(full_date) FROM dim_date
        WHERE year = YEAR(CURRENT_DATE))                    AS ytd_start,
    (SELECT MAX(full_date) FROM dim_date)                   AS ytd_end,
    ROUND(fr.rate_on_first_day, 6)                          AS first_rate,
    ROUND(lr.rate_on_last_day,  6)                          AS last_rate,
    ROUND(
        (lr.rate_on_last_day - fr.rate_on_first_day)
        / fr.rate_on_first_day * 100
    , 4)                                                    AS ytd_change_pct
FROM first_rate      fr
JOIN last_rate       lr ON lr.from_currency_id = fr.from_currency_id
                       AND lr.to_currency_id   = fr.to_currency_id
JOIN dim_currency    fc ON fc.currency_id      = fr.from_currency_id
JOIN dim_currency    tc ON tc.currency_id      = fr.to_currency_id
WHERE fc.currency_code = 'EUR'
ORDER BY tc.currency_code;
