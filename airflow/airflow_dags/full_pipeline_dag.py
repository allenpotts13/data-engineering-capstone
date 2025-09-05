from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

with DAG(
    dag_id="full_pipeline_dag",
    schedule="@daily",
    start_date=pendulum.now("UTC").subtract(days=1),
    catchup=False,
) as dag:

    trigger_api_ingestion = TriggerDagRunOperator(
        task_id="trigger_api_ingestion",
        trigger_dag_id="api_ingestion_dag",
        wait_for_completion=True,
    )

    trigger_webscraping = TriggerDagRunOperator(
        task_id="trigger_webscraping",
        trigger_dag_id="webscraping_dag",
        wait_for_completion=True,
    )

    trigger_duckdb_ingest = TriggerDagRunOperator(
        task_id="trigger_duckdb_ingest",
        trigger_dag_id="duckdb_ingest_dag",
        wait_for_completion=True,
    )

    trigger_duckdb_transformation = TriggerDagRunOperator(
        task_id="trigger_duckdb_transformation",
        trigger_dag_id="duckdb_transformation_dag",
        wait_for_completion=True,
    )

    trigger_api_ingestion >> trigger_webscraping >> trigger_duckdb_ingest >> trigger_duckdb_transformation