import logging
import os
import sys

import pendulum
from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
)

logger = logging.getLogger(__name__)

PROJECT_ROOT_IN_AIRFLOW = "/opt/airflow"
if PROJECT_ROOT_IN_AIRFLOW not in sys.path:
    sys.path.append(PROJECT_ROOT_IN_AIRFLOW)

SRC_PATH_IN_AIRFLOW = "/opt/airflow/src"
if SRC_PATH_IN_AIRFLOW not in sys.path:
    sys.path.append(SRC_PATH_IN_AIRFLOW)


try:
    from src.ftp_ingestion import main
except ImportError as e:
    print(f"Error importing modules: {e}")
    raise


def _run_api_ingestion(**kwargs):
    run_id = kwargs.get("dag_run").run_id if kwargs.get("dag_run") else "unknown"
    logger.info(f"Running API ingestion tasks for {run_id}")

    api_results = main()
    if not api_results:
        logger.error("API call failed, skipping subsequent tasks.")
        raise Exception("API call failed")

    logger.info(f"Data acquisition tasks completed successfully for {run_id}")
    return True


with DAG(
    dag_id="ftp_ingestion_dag",
    schedule="@yearly",
    start_date=pendulum.now("UTC").subtract(days=1),
    catchup=False,
    description="DAG for FTP ingestion tasks",
    tags=["data", "ingestion"],
) as dag:

    run_ftp_ingestion = PythonOperator(
        task_id="run_ftp_ingestion",
        python_callable=_run_api_ingestion,
    )

    run_ftp_ingestion
