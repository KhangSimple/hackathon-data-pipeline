# RAW_TABLE_NAME = "orders"
# STG_TABLE_NAME = "orders_standardized"
# MART_TABLE_NAME = "dm_fact_orders"
# N_DAYS_EXPRIRED = 3
PK_COLUMNS = ["product_review_id"]
SK_TABLES = [
    {"table": "dm_dim_product", "joined_col": "product_id"},
    {"table": "dm_dim_user", "joined_col": "user_id"},
]
SK_COLUMNS = ["product_sk", "user_sk"]
ORDER_COLUMNS = "updated_at"

SCHEMA_POSTGRES_FIELDS = [
    {"name": "product_review_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "product_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "user_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "rating", "type": "INT64", "mode": "NULLABLE"},
    {"name": "comment", "type": "STRING", "mode": "NULLABLE"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "updated_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "ingestion_date", "type": "DATE", "mode": "NOT NULL"},
]

SCHEMA_FIELDS = [
    {"name": "product_review_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "rating", "type": "STRING", "mode": "NULLABLE"},
    {"name": "comment", "type": "STRING", "mode": "NULLABLE"},
    {"name": "created_at", "type": "STRING", "mode": "REQUIRED"},
    {"name": "updated_at", "type": "STRING", "mode": "REQUIRED"},
    {"name": "ingestion_date", "type": "STRING", "mode": "REQUIRED"},
]

MART_SCHEMA_FIELDS = [
    {"name": "product_review_sk", "type": "STRING", "mode": "NOT NULL"},
    {"name": "product_review_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "rating", "type": "INT64", "mode": "NULLABLE"},
    {"name": "comment", "type": "STRING", "mode": "NULLABLE"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "updated_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "dw_inserted_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "user_sk", "type": "STRING", "mode": "NULLABLE"},
    {"name": "product_sk", "type": "STRING", "mode": "NULLABLE"},
]

# QUERY_DELETE_OLD_N_DAYS_DATA = """
#     DELETE FROM `{BQ_PROJECT_ID}.{BQ_RAW_DATASET_NAME}.{RAW_TABLE_NAME}`
#     WHERE PARSE_DATE('%Y-%m-%d', ingestion_date) < DATE_SUB(PARSE_DATE('%Y-%m-%d', {current_date}), INTERVAL {n_days} DAY);

#     DELETE FROM `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{STG_TABLE_NAME}`
#     WHERE PARSE_DATE('%Y-%m-%d', ingestion_date) < DATE_SUB(PARSE_DATE('%Y-%m-%d', {current_date}), INTERVAL {n_days} DAY);
# """
