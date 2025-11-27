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

    # query = f"""CREATE TABLE IF NOT EXISTS {BQ_PROJECT_ID}.{BQ_RAW_DATASET_NAME}.{table_name}
    #     (
    #         {','.join([f"{field_detail['name']} STRING" for field_detail in schema_postgres_fields])}
    #     )
    # """


def generate_transform_raw_data_query(table_name, schema_postgres_fields):
    query = f"""CREATE OR REPLACE TABLE IF NOT EXISTS {BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{table_name}_standardized
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
    query += """QUALIFY ROW_NUMBER() OVER(PARTITION BY {partition_columns} ORDER BY {order_columns} DESC) = 1"""

    return query


def generate_data_mart_query(table_name, schema_postgres_fields, pk_columns):
    query_diff_condition = " OR ".join(
        [
            f"COALESCE(CAST(dim.{field_detail['name']} AS STRING),'') <> COALESCE(CAST(stg.{field_detail['name']} AS STRING),'')"
            for field_detail in schema_postgres_fields
        ]
    )

    create_table_query = f"""CREATE TABLE IF NOT EXISTS {BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.dm_dim_{table_name}
        (
            {table_name}_sk STRING NOT NULL,
            {','.join([f"{field_detail['name']} {field_detail['type']} {field_detail['mode'] if field_detail['mode'] == 'NOT NULL' else ''}"
                       for field_detail in schema_postgres_fields])},
            effective_from    TIMESTAMP NOT NULL,
            effective_to      TIMESTAMP,
            is_current        BOOL DEFAULT TRUE,
            dw_inserted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
    """
    merge_query = f"""
        MERGE INTO `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.dm_dim_{table_name}` AS dim
        USING (
        SELECT * FROM `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{table_name}_standardized` WHERE ingestion_date = '{{ingestion_date}}'
        ) AS stg
        ON {" AND ".join([f'dim.{col} = stg.{col}' for col in pk_columns])}
        WHEN MATCHED AND (
            {query_diff_condition}
        ) THEN
        UPDATE SET is_current = FALSE, effective_to = CURRENT_TIMESTAMP();
    """

    insert_query = f"""
        INSERT INTO `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.dm_dim_{table_name}`
        ({table_name}_sk, {', '.join([field_detail['name'] for field_detail in schema_postgres_fields])}, effective_from, effective_to, is_current, dw_inserted_at)
        SELECT GENERATE_UUID(), {', '.join([f"stg.{field_detail['name']}" for field_detail in schema_postgres_fields])},
            CURRENT_TIMESTAMP(), NULL, TRUE, CURRENT_TIMESTAMP()
        FROM `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{table_name}_standardized` stg
        WHERE NOT EXISTS
        (
            SELECT * from `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.dm_dim_{table_name}` dim
            WHERE {" AND ".join([f'dim.{col} = stg.{col}' for col in pk_columns])}
            AND NOT (
                {query_diff_condition}
            )
        )
        """
    query = f"""
        {create_table_query}
        {merge_query}
        {insert_query}
    """
    return query
