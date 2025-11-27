from airflow import DAG
from airflow.decorators import dag, task

# from airflow.sdk import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from const.const import (
    POSTGRES_CONN_ID,
    GCS_BUCKET,
    BQ_PROJECT_ID,
    BQ_RAW_DATASET_NAME,
    BQ_STG_DATASET_NAME,
    BQ_MART_DATASET_NAME,
)

from datetime import datetime
import importlib

configs = {
    "orders": {},
    "users": {},
    # "customers": {},
    "products": {},
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "start_date": datetime(2025, 1, 1),
}


current_date = datetime.now().strftime("%Y%m%d")
ingestion_date = datetime.now().strftime("%Y-%m-%d")


def create_dag(dag_id: str, table_name: str):
    @dag(
        dag_id=dag_id,
        start_date=default_args["start_date"],
        schedule_interval="@daily",
        catchup=False,
    )
    def pipeline_dim_dag():
        constlib = importlib.import_module(f"const.{table_name}_const")

        postgres_query = getattr(constlib, "POSTGRES_QUERY_RAW_DATA")
        transform_query_standardize = getattr(constlib, "TRANSFORM_QUERY_STANDARDIZE")
        mart_query_fact = getattr(constlib, "MART_QUERY_FACT")
        query_delete_old_n_days_data = getattr(constlib, "QUERY_DELETE_OLD_N_DAYS_DATA")
        n_days = getattr(constlib, "N_DAYS_EXPRIRED")
        schema_fields = getattr(constlib, "SCHEMA_FIELDS")

        export_postgres_to_gcs = PostgresToGCSOperator(
            task_id="export_postgres_data_to_gcs",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=postgres_query.format(
                ingestion_date=ingestion_date, RAW_TABLE_NAME=table_name
            ),
            bucket=GCS_BUCKET,
            filename=f"raw/{table_name}/{current_date}/{table_name}_{current_date}.csv",
            export_format="csv",
            gzip=False,
        )

        export_gcs_to_bq = GCSToBigQueryOperator(
            task_id="export_gcs_data_to_bq",
            bucket=GCS_BUCKET,
            source_objects=[
                f"raw/{table_name}/{current_date}/{table_name}_{current_date}.csv"
            ],
            destination_project_dataset_table=f"{BQ_PROJECT_ID}.{BQ_RAW_DATASET_NAME}.{table_name}",
            autodetect=False,
            write_disposition="WRITE_APPEND",
            skip_leading_rows=1,
            schema_fields=schema_fields,
        )

        transform_bq_data_standardize = BigQueryInsertJobOperator(
            task_id="transform_bq_data_standardize",
            configuration={
                "query": {
                    "query": transform_query_standardize.format(
                        BQ_PROJECT_ID=BQ_PROJECT_ID,
                        BQ_STG_DATASET_NAME=BQ_STG_DATASET_NAME,
                        STG_TABLE_NAME=f"{table_name}_standardized",
                        BQ_RAW_DATASET_NAME=BQ_RAW_DATASET_NAME,
                        RAW_TABLE_NAME=table_name,
                        ingestion_date=ingestion_date,
                    ),
                    "useLegacySql": False,
                }
            },
        )

        bq_data_mart = BigQueryInsertJobOperator(
            task_id="bq_data_mart",
            configuration={
                "query": {
                    "query": mart_query_fact.format(
                        BQ_PROJECT_ID=BQ_PROJECT_ID,
                        BQ_MART_DATASET_NAME=BQ_MART_DATASET_NAME,
                        MART_TABLE_NAME=f"dm_dim_{table_name}",
                        BQ_STG_DATASET_NAME=BQ_STG_DATASET_NAME,
                        STG_TABLE_NAME=f"{table_name}_standardized",
                        ingestion_date=ingestion_date,
                    ),
                    "useLegacySql": False,
                }
            },
        )

        delete_data_exprired_n_days = BigQueryInsertJobOperator(
            task_id="delete_data_exprired_n_days",
            configuration={
                "query": {
                    "query": query_delete_old_n_days_data.format(
                        BQ_PROJECT_ID=BQ_PROJECT_ID,
                        BQ_STG_DATASET_NAME=BQ_STG_DATASET_NAME,
                        STG_TABLE_NAME=f"{table_name}_standardized",
                        BQ_RAW_DATASET_NAME=BQ_RAW_DATASET_NAME,
                        RAW_TABLE_NAME=table_name,
                        current_date=ingestion_date,
                        n_days=n_days,
                    ),
                    "useLegacySql": False,
                }
            },
        )

        (
            export_postgres_to_gcs
            >> export_gcs_to_bq
            >> transform_bq_data_standardize
            >> bq_data_mart
            >> delete_data_exprired_n_days
        )

    generated_dag = pipeline_dim_dag()

    return generated_dag


for table_name in configs.keys():
    dag_id = f"dag_pipeline_for_{table_name}"
    globals()[dag_id] = create_dag(dag_id, table_name)
