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

from const.scd2_const import (
    POSTGRES_CONN_ID,
    GCS_BUCKET,
    BQ_PROJECT_ID,
    BQ_RAW_DATASET_NAME,
    BQ_STG_DATASET_NAME,
    BQ_MART_DATASET_NAME,
    generate_get_data_postgres_query,
    generate_transform_raw_data_query,
    generate_data_mart_query,
)

from datetime import datetime
import importlib

configs = {
    # "orders": {},
    "user": {},
    "coupon": {},
    "product": {},
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

        # transform_query_standardize = getattr(constlib, "TRANSFORM_QUERY_STANDARDIZE")
        # mart_query_fact = getattr(constlib, "MART_QUERY_FACT")
        # query_delete_old_n_days_data = getattr(constlib, "QUERY_DELETE_OLD_N_DAYS_DATA")
        # n_days = getattr(constlib, "N_DAYS_EXPRIRED")
        schema_fields = getattr(constlib, "SCHEMA_FIELDS")
        schema_postgres_fields = getattr(constlib, "SCHEMA_POSTGRES_FIELDS")
        pk_columns = getattr(constlib, "PK_COLUMNS")
        order_columns = getattr(constlib, "ORDER_COLUMNS")
        postgres_query = generate_get_data_postgres_query(
            table_name, schema_postgres_fields
        )
        transform_bq_data_standardize_query = generate_transform_raw_data_query(
            table_name, schema_postgres_fields
        )
        data_mark_query = generate_data_mart_query(
            table_name, schema_postgres_fields, pk_columns
        )

        export_postgres_to_gcs = PostgresToGCSOperator(
            task_id="export_postgres_data_to_gcs",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=postgres_query.format(ingestion_date=ingestion_date),
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
                    "query": transform_bq_data_standardize_query.format(
                        ingestion_date=ingestion_date,
                        partition_columns=",".join(pk_columns),
                        order_columns=order_columns,
                    ),
                    "useLegacySql": False,
                }
            },
        )

        bq_data_mart = BigQueryInsertJobOperator(
            task_id="bq_data_mart",
            configuration={
                "query": {
                    "query": data_mark_query.format(ingestion_date=ingestion_date),
                    "useLegacySql": False,
                }
            },
        )

        # delete_data_exprired_n_days = BigQueryInsertJobOperator(
        #     task_id="delete_data_exprired_n_days",
        #     configuration={
        #         "query": {
        #             "query": query_delete_old_n_days_data.format(
        #                 BQ_PROJECT_ID=BQ_PROJECT_ID,
        #                 BQ_STG_DATASET_NAME=BQ_STG_DATASET_NAME,
        #                 STG_TABLE_NAME=f"{table_name}_standardized",
        #                 BQ_RAW_DATASET_NAME=BQ_RAW_DATASET_NAME,
        #                 RAW_TABLE_NAME=table_name,
        #                 current_date=ingestion_date,
        #                 n_days=n_days,
        #             ),
        #             "useLegacySql": False,
        #         }
        #     },
        # )

        (
            export_postgres_to_gcs
            >> export_gcs_to_bq
            >> transform_bq_data_standardize
            >> bq_data_mart
            # >> delete_data_exprired_n_days
        )

    generated_dag = pipeline_dim_dag()

    return generated_dag


for table_name in configs.keys():
    dag_id = f"dag_pipeline_for_{table_name}"
    globals()[dag_id] = create_dag(dag_id, table_name)
