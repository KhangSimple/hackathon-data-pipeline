POSTGRES_CONN_ID = "hackathon_postgres"
GCS_BUCKET = "my-hackathon"

BQ_PROJECT_ID = "hackathon-478514"
BQ_RAW_DATASET_NAME = "raw_ds"
BQ_STG_DATASET_NAME = "stg_ds"
BQ_MART_DATASET_NAME = "mart_ds"


def generate_get_data_postgres_query(table_name, schema_postgres_fields):
    query = "SELECT "
    query += ",\n".join(
        [
            (
                f"CAST({field_detail['name']} AS TEXT) AS {field_detail['name']}"
                if field_detail["name"] != "ingestion_date"
                else "'{ingestion_date}' AS ingestion_date"
            )
            for field_detail in schema_postgres_fields
        ]
    )
    query += f" FROM {table_name}"
    return query


def generate_transform_raw_data_query(table_name, schema_postgres_fields):
    query = f"""
CREATE OR REPLACE TABLE `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{table_name}_standardized` AS
SELECT 
    """
    query += ",\n".join(
        [
            (
                f"CAST({field_detail['name']} AS {field_detail['type']}) AS {field_detail['name']}"
            )
            for field_detail in schema_postgres_fields
        ]
    )
    query += f" FROM {BQ_PROJECT_ID}.{BQ_RAW_DATASET_NAME}.{table_name}"
    query += """ QUALIFY ROW_NUMBER() OVER(PARTITION BY {partition_columns} ORDER BY {order_columns} DESC) = 1"""

    return query


def generate_data_mart_query(table_name, schema_postgres_fields, pk_columns):
    query = f"""
        MERGE INTO `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.dm_fact_{table_name}` AS fact
    USING (
    SELECT
        stg.order_id,
        dim_c.user_sk,
        stg.status,
        stg.order_total,
        stg.currency,
        stg.created_at,
        stg.updated_at
    FROM `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{table_name}_standardized` stg
    LEFT JOIN `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.dm_dim_users` dim_c
        ON stg.user_id = dim_c.user_id
    ) AS src
    ON fact.order_id = src.order_id
    WHEN MATCHED THEN
    UPDATE SET
        fact.user_sk = src.user_sk,
        fact.status = src.status,
        fact.order_total = src.order_total,
        fact.currency = src.currency,
        fact.created_at = src.created_at,
        fact.updated_at = src.updated_at
    WHEN NOT MATCHED THEN
    INSERT (
        order_sk,
        order_id,
        user_sk,
        status,
        order_total,
        currency,
        created_at,
        updated_at
    )
    VALUES (
        GENERATE_UUID(),
        src.order_id,
        src.user_sk,
        src.status,
        src.order_total,
        src.currency,
        src.created_at,
        src.updated_at
    );
    """

    return query
