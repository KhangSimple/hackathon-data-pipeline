# RAW_TABLE_NAME = "users"
# STG_TABLE_NAME = "users_standardized"
# MART_TABLE_NAME = "dm_dim_users"
N_DAYS_EXPRIRED = 3

PK_COLUMNS = ["category_id", "ingestion_date"]
ORDER_COLUMNS = "updated_at"

SCHEMA_POSTGRES_FIELDS = [
    {"name": "category_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "name", "type": "STRING", "mode": "NOT NULL"},
    {"name": "parent_category_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "type", "type": "STRING", "mode": "NOT NULL"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "updated_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "ingestion_date", "type": "DATE", "mode": "NOT NULL"},
]

SCHEMA_FIELDS = [
    {"name": "category_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "parent_category_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "created_at", "type": "STRING", "mode": "REQUIRED"},
    {"name": "updated_at", "type": "STRING", "mode": "REQUIRED"},
    {"name": "ingestion_date", "type": "STRING", "mode": "REQUIRED"},
]
