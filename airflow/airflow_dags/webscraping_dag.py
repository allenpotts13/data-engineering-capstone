import os
import sys

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)
import logging
from datetime import datetime

import pendulum
from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG

logger = logging.getLogger(__name__)

PROJECT_ROOT_IN_AIRFLOW = "/opt/airflow"
if PROJECT_ROOT_IN_AIRFLOW not in sys.path:
    sys.path.append(PROJECT_ROOT_IN_AIRFLOW)

SRC_PATH_IN_AIRFLOW = "/opt/airflow/src"
if SRC_PATH_IN_AIRFLOW not in sys.path:
    sys.path.append(SRC_PATH_IN_AIRFLOW)


try:
    from src.html_scrap import main
except ImportError as e:
    print(f"Error importing modules: {e}")
    raise


def _run_webscraping(**kwargs):
    run_id = kwargs.get("dag_run").run_id if kwargs.get("dag_run") else "unknown"
    logger.info(f"Running web scraping tasks for {run_id}")

    webscraping = main()
    if not webscraping:
        logger.error("Web scraping failed, skipping subsequent tasks.")
        raise Exception("Web scraping failed")

    logger.info(f"Data acquisition tasks completed successfully for {run_id}")
    return True


with DAG(
    dag_id="webscraping_dag",
    schedule="@daily",
    start_date=pendulum.now("UTC").subtract(days=1),
    catchup=False,
    description="DAG for Webscraping tasks",
    tags=["data", "ingestion", "webscraping"],
) as dag:

    run_webscraping = PythonOperator(
        task_id="run_webscraping",
        python_callable=_run_webscraping,
    )

    run_webscraping
