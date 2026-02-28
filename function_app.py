"""
function_app.py – Azure Functions v2 entry point.

Uses the decorator-based programming model (v2), which is the modern
approach for Python Azure Functions — no function.json needed.

Scheduling is handled entirely by Azure Data Factory (Mon-Fri 10:00 UTC).
The Function App exposes a single HTTP trigger so ADF can call it using the
native AzureFunctionActivity with a function key — no master key required.
"""

import logging
from datetime import date, timedelta

import azure.functions as func

from pipeline import run

app = func.FunctionApp()


@app.route(route="fx_etl", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def fx_etl(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger called by Azure Data Factory.

    ADF's AzureFunctionActivity posts to /api/fx_etl with a function key.
    Scheduling (Mon-Fri 10:00 UTC) is owned by the ADF trigger, keeping
    a single source of truth for the pipeline schedule.
    """
    logging.info("FX ETL triggered via HTTP.")

    target_date = (date.today() - timedelta(days=1)).isoformat()
    run(start_date=target_date, end_date=target_date)

    logging.info("FX ETL complete.")
    return func.HttpResponse("FX ETL completed successfully.", status_code=200)
