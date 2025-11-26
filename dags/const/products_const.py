RAW_TABLE_NAME = "orders"
STG_TABLE_NAME = "orders_standardized"
MART_TABLE_NAME = "dm_fact_orders"

POSTGRES_QUERY_RAW_DATA = f"""
    select CAST(order_id as TEXT),
    user_id,
    status,
    CAST(order_total AS TEXT) as order_total,
    currency, TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') as created_at,
    TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS') as updated_at
    from {RAW_TABLE_NAME} order by order_id
"""

TRANSFORM_QUERY_STANDARDIZE = """
    INSERT INTO `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{STG_TABLE_NAME}`
    SELECT
        order_id,
        user_id,
        UPPER(status) AS status,
        CAST(order_total AS FLOAT64) AS order_total,
        UPPER(currency) AS currency,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', created_at) AS created_at,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', updated_at) AS updated_at
    FROM `{BQ_PROJECT_ID}.{BQ_RAW_DATASET_NAME}.{RAW_TABLE_NAME}`;
"""

MART_QUERY_FACT_ORDERS = """
    INSERT INTO `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.{MART_TABLE_NAME}`
    SELECT
        *
    FROM `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{STG_TABLE_NAME}`;
"""

SCHEMA_FIELDS = [
    {"name": "order_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "user_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "order_total", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
]
