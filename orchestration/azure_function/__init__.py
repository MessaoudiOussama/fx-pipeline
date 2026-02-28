"""
orchestration/azure_function/__init__.py
Azure Function entry point – wraps our pipeline for serverless execution.

Trigger  : Timer (cron "0 0 10 * * *" = every day at 10:00 UTC)
Schedule : ECB publishes rates at ~16:00 CET (15:00 UTC).
           Running at 10:00 UTC the following morning guarantees the
           previous business day's rate is always available.

In production this function would:
  1. Pull the Frankfurter API URL and Synapse connection string from
     Azure Key Vault (injected as environment variables by the Function App).
  2. Run Extract → Transform → Load against Azure Synapse Analytics
     instead of the local DuckDB file.
  3. Optionally write the raw API response to Azure Data Lake Storage
     Gen2 as a Bronze-layer JSON file before transforming.

Deployment
----------
  func azure functionapp publish <FUNCTION_APP_NAME>
"""

import logging
from datetime import date, timedelta

import azure.functions as func

# Re-use the same pipeline logic — only the storage backend changes in prod.
from pipeline import run

logger = logging.getLogger(__name__)


def main(timer: func.TimerRequest) -> None:
    """
    Entry point called by the Azure Functions runtime on each timer tick.

    Loads yesterday's FX rates (the most recently published ECB rates).
    For a full historical backfill, run pipeline.py locally with --start-date.
    """
    if timer.past_due:
        logger.warning("Timer is past due — running now to catch up.")

    # Target: yesterday (the latest complete ECB business day)
    target_date = (date.today() - timedelta(days=1)).isoformat()

    logger.info("Azure Function triggered | loading rates for %s", target_date)

    run(start_date=target_date, end_date=target_date)

    logger.info("Azure Function complete")
