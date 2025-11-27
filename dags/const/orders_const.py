# RAW_TABLE_NAME = "orders"
# STG_TABLE_NAME = "orders_standardized"
# MART_TABLE_NAME = "dm_fact_orders"
# N_DAYS_EXPRIRED = 3
PK_COLUMNS = ["order_id", "ingestion_date"]
ORDER_COLUMNS = "updated_at"

SCHEMA_POSTGRES_FIELDS = [
    {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "order_total", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "ingestion_date", "type": "DATE", "mode": "NULLABLE"},
]

SCHEMA_FIELDS = [
    {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "order_total", "type": "STRING", "mode": "NULLABLE"},
    {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
    {"name": "created_at", "type": "STRING", "mode": "NULLABLE"},
    {"name": "updated_at", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ingestion_date", "type": "STRING", "mode": "NULLABLE"},
]

# QUERY_DELETE_OLD_N_DAYS_DATA = """
#     DELETE FROM `{BQ_PROJECT_ID}.{BQ_RAW_DATASET_NAME}.{RAW_TABLE_NAME}`
#     WHERE PARSE_DATE('%Y-%m-%d', ingestion_date) < DATE_SUB(PARSE_DATE('%Y-%m-%d', {current_date}), INTERVAL {n_days} DAY);

#     DELETE FROM `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{STG_TABLE_NAME}`
#     WHERE PARSE_DATE('%Y-%m-%d', ingestion_date) < DATE_SUB(PARSE_DATE('%Y-%m-%d', {current_date}), INTERVAL {n_days} DAY);
# """
