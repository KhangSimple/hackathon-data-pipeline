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
CREATE OR REPLACE TABLE {BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{table_name}_standardized AS
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
    mapping_query = ", ".join(
        [
            f"dim.{field_detail['name']} = stg.{field_detail['name']}"
            for field_detail in schema_postgres_fields
        ]
    )

    insert_query = ", ".join(
        [f"stg.{field_detail['name']}" for field_detail in schema_postgres_fields]
    )

    query = f"""
        MERGE INTO `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.dm_dim_{table_name}` AS dim
        USING {BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{table_name}_standardized AS stg
        ON {" AND ".join([f'dim.{col} = stg.{col}' for col in pk_columns])}
        WHEN MATCHED THEN
        UPDATE SET
            {mapping_query}, dim.dw_inserted_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
        INSERT ({', '.join([field_detail['name'] for field_detail in schema_postgres_fields])}, dw_inserted_at)
        VALUES ({insert_query}, CURRENT_TIMESTAMP())
    """

    return query
