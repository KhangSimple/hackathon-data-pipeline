# RAW_TABLE_NAME = "orders"
# STG_TABLE_NAME = "orders_standardized"
# MART_TABLE_NAME = "dm_fact_orders"
# N_DAYS_EXPRIRED = 3
PK_COLUMNS = ["payment_id"]
SK_TABLES = [{"table": "dm_fact_order", "joined_col": "order_id"}]
SK_COLUMNS = ["user_sk"]
ORDER_COLUMNS = "updated_at"

SCHEMA_POSTGRES_FIELDS = [
    {"name": "payment_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "order_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "amount", "type": "FLOAT64", "mode": "NOT NULL"},
    {"name": "method", "type": "STRING", "mode": "NOT NULL"},
    {"name": "status", "type": "STRING", "mode": "NOT NULL"},
    {"name": "paid_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "updated_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "ingestion_date", "type": "DATE", "mode": "NOT NULL"},
]

SCHEMA_FIELDS = [
    {"name": "payment_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "amount", "type": "STRING", "mode": "REQUIRED"},
    {"name": "method", "type": "STRING", "mode": "REQUIRED"},
    {"name": "status", "type": "STRING", "mode": "REQUIRED"},
    {"name": "paid_at", "type": "STRING", "mode": "NULLABLE"},
    {"name": "created_at", "type": "STRING", "mode": "REQUIRED"},
    {"name": "updated_at", "type": "STRING", "mode": "REQUIRED"},
    {"name": "ingestion_date", "type": "STRING", "mode": "REQUIRED"},
]

MART_SCHEMA_FIELDS = [
    {"name": "payment_sk", "type": "STRING", "mode": "NOT NULL"},
    {"name": "payment_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "order_sk", "type": "STRING", "mode": "NULLABLE"},
    {"name": "amount", "type": "FLOAT64", "mode": "NOT NULL"},
    {"name": "method", "type": "STRING", "mode": "NOT NULL"},
    {"name": "status", "type": "STRING", "mode": "NOT NULL"},
    {"name": "paid_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "updated_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "dw_inserted_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
]

# QUERY_DELETE_OLD_N_DAYS_DATA = """
#     DELETE FROM `{BQ_PROJECT_ID}.{BQ_RAW_DATASET_NAME}.{RAW_TABLE_NAME}`
#     WHERE PARSE_DATE('%Y-%m-%d', ingestion_date) < DATE_SUB(PARSE_DATE('%Y-%m-%d', {current_date}), INTERVAL {n_days} DAY);

#     DELETE FROM `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{STG_TABLE_NAME}`
#     WHERE PARSE_DATE('%Y-%m-%d', ingestion_date) < DATE_SUB(PARSE_DATE('%Y-%m-%d', {current_date}), INTERVAL {n_days} DAY);
# """
