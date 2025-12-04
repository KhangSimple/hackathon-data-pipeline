import logging

logger = logging.getLogger(__name__)

from airflow import DAG

# from airflow.sdk import dag, task

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from datetime import datetime

from const.business_query_const.query_const import REVENUE_BY_DATE_QUERY

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "start_date": datetime(2025, 1, 1),
}


current_date = datetime.now().strftime("%Y%m%d")
ingestion_date = datetime.now().strftime("%Y-%m-%d")

with DAG(
    dag_id="dag_pipeline_for_revenue_by_date_tb",
    start_date=default_args["start_date"],
    schedule_interval="0 2 * * *",
    catchup=False,
) as dag:

    create_revenue_by_date_tb = BigQueryInsertJobOperator(
        task_id="create_revenue_by_date_tb",
        configuration={
            "query": {
                "query": REVENUE_BY_DATE_QUERY,
                "useLegacySql": False,
            }
        },
    )

    create_revenue_by_date_tb
