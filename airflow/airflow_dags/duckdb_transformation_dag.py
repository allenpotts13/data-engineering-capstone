import logging
import os
import sys

import pendulum
from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
)

logger = logging.getLogger(__name__)

PROJECT_ROOT_IN_AIRFLOW = "/opt/airflow"
if PROJECT_ROOT_IN_AIRFLOW not in sys.path:
    sys.path.append(PROJECT_ROOT_IN_AIRFLOW)

SRC_PATH_IN_AIRFLOW = "/opt/airflow/data"
if SRC_PATH_IN_AIRFLOW not in sys.path:
    sys.path.append(SRC_PATH_IN_AIRFLOW)


try:
    from data.duckdb_transformation import main
except ImportError as e:
    print(f"Error importing modules: {e}")
    raise


def _run_duckdb_transformation(**kwargs):
    run_id = kwargs.get("dag_run").run_id if kwargs.get("dag_run") else "unknown"
    logger.info(f"Running DuckDB transformation tasks for {run_id}")

    duckdb_silver_transformation = main()
    if not duckdb_silver_transformation:
        logger.error("DuckDB transformation failed, skipping subsequent tasks.")
        raise Exception("DuckDB transformation failed")

    logger.info(f"DuckDB transformation tasks completed successfully for {run_id}")
    return True


with DAG(
    dag_id="duckdb_transformation_dag",
    schedule="@yearly",
    start_date=pendulum.now("UTC").subtract(days=1),
    catchup=False,
    description="DAG for DuckDB transformation tasks",
    tags=["data", "transformation"],
) as dag:

    run_duckdb_transformation = PythonOperator(
        task_id="run_duckdb_transformation",
        python_callable=_run_duckdb_transformation,
    )

    run_duckdb_transformation
