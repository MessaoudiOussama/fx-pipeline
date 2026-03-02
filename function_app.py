"""
function_app.py – Azure Functions v2 entry point.

Uses the decorator-based programming model (v2), which is the modern
approach for Python Azure Functions — no function.json needed.

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
    """
    logging.info("FX ETL triggered via HTTP.")

    target_date = (date.today() - timedelta(days=1)).isoformat()
    run(start_date=target_date, end_date=target_date)

    logging.info("FX ETL complete.")
    return func.HttpResponse("FX ETL completed successfully.", status_code=200)
