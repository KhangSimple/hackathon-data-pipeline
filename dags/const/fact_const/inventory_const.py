POSTGRES_QUERY_RAW_DATA = """
    SELECT
        CAST(inventory_id AS TEXT),
        CAST(product_id AS TEXT),
        CAST(warehouse_id AS TEXT),
        CAST(quantity AS TEXT) as role_id,
        TO_CHAR(last_updated, 'YYYY-MM-DD HH24:MI:SS') AS last_updated,
        '{ingestion_date}' AS ingestion_date
    FROM {RAW_TABLE_NAME}
"""

TRANSFORM_QUERY_STANDARDIZE = """
    CREATE OR REPLACE TABLE `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{STG_TABLE_NAME}` AS
    WITH cleaned AS (
    SELECT
        product_id,
        UPPER(TRIM(warehouse_id)) AS warehouse_id,
        SAFE_CAST(REGEXP_EXTRACT(quantity, r'\d+') AS INT64) AS quantity,
        TIMESTAMP(last_updated) AS last_updated,
        CURRENT_TIMESTAMP() AS ingestion_ts,
        ROW_NUMBER() OVER (
        PARTITION BY product_id, warehouse_id
        ORDER BY last_updated DESC
        ) AS rn
    FROM `{BQ_PROJECT_ID}.{BQ_RAW_DATASET_NAME}.{RAW_TABLE_NAME}`
    )
    SELECT * EXCEPT(rn)
    FROM cleaned
    WHERE rn = 1;
"""

MART_QUERY_FACT = """
    DELETE FROM `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.{MART_TABLE_NAME}`
    WHERE snapshot_date = '{current_date}';

    INSERT INTO `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.{MART_TABLE_NAME}`
    SELECT 
        DATE('{current_date}') AS snapshot_date,
        product_id,
        warehouse_id,
        quantity,
        last_updated,
        CURRENT_TIMESTAMP() AS dw_inserted_at
    FROM `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{STG_TABLE_NAME}`
"""

SCHEMA_FIELDS = [
    {"name": "inventory_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "warehouse_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "quantity", "type": "STRING", "mode": "NULLABLE"},
    {"name": "last_updated", "type": "STRING", "mode": "REQUIRED"},
    {"name": "ingestion_date", "type": "STRING", "mode": "NULLABLE"},
]
