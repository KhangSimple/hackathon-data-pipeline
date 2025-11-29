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


def generate_data_mart_query(
    table_name, schema_postgres_fields, pk_columns, sk_columns, mart_schema_fields
):
    schema_except_ingestion_date = [
        x for x in schema_postgres_fields if x["name"] != "ingestion_date"
    ]

    schema_except_pk_cols = [
        x
        for x in schema_except_ingestion_date
        if x["name"] not in pk_columns
        and x["name"] not in [f"{col[:-4]}_id" for col in sk_columns]
    ]

    get_stg_cols = ", ".join(
        [f"stg.{field_detail['name']}" for field_detail in schema_except_ingestion_date]
    )

    get_sk_cols = ", ".join([f"{col[:-3]}.{col}" for col in sk_columns])

    left_join_tb = "\n".join(
        [
            f"LEFT JOIN (SELECT * FROM `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.dm_dim_{col[:-3]}` WHERE is_current) AS {col[:-3]}"
            for col in sk_columns
        ]
    )

    join_conditions = " AND ".join(
        [f"stg.{col[:-4]}_id = {col[:-3]}.{col[:-4]}_id" for col in sk_columns]
    )

    update_value = ", ".join(
        [f"fact.{col['name']} = src.{col['name']}" for col in schema_except_pk_cols]
    )

    query = f"""
        MERGE INTO `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.dm_fact_{table_name}` AS fact
    USING (
    SELECT
        {get_stg_cols},
        {get_sk_cols}
    FROM `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{table_name}_standardized` stg
    {left_join_tb}
        ON {join_conditions}
    ) AS src
    ON fact.{pk_columns[0]} = src.{pk_columns[0]}
    WHEN MATCHED THEN
    UPDATE SET {update_value}
    WHEN NOT MATCHED THEN
    INSERT (
        {', '.join([col['name'] for col in mart_schema_fields if col['name'] != 'dw_inserted_at'])},
        dw_inserted_at
    )
    VALUES (
        GENERATE_UUID(),
        src.{', src.'.join([col['name'] for col in mart_schema_fields if col['name'] not in (f'{table_name[:-1]}_sk', 'dw_inserted_at')])},
        CURRENT_TIMESTAMP()
    );
    """

    return query
