# RAW_TABLE_NAME = "users"
# STG_TABLE_NAME = "users_standardized"
# MART_TABLE_NAME = "dm_dim_users"
N_DAYS_EXPRIRED = 3

PK_COLUMNS = ["coupon_id", "ingestion_date"]
ORDER_COLUMNS = "updated_at"

SCHEMA_POSTGRES_FIELDS = [
    {"name": "coupon_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "code", "type": "STRING", "mode": "NOT NULL"},
    {"name": "discount_type", "type": "STRING", "mode": "NOT NULL"},
    {"name": "value", "type": "STRING", "mode": "NOT NULL"},
    {"name": "valid_from", "type": "STRING", "mode": "NOT NULL"},
    {"name": "valid_to", "type": "STRING", "mode": "NULLABLE"},
    {"name": "usage_count", "type": "STRING", "mode": "NOT NULL"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "updated_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "ingestion_date", "type": "DATE", "mode": "NULLABLE"},
]

SCHEMA_FIELDS = [
    {"name": "coupon_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "discount_type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "value", "type": "STRING", "mode": "REQUIRED"},
    {"name": "valid_from", "type": "STRING", "mode": "REQUIRED"},
    {"name": "valid_to", "type": "STRING", "mode": "NULLABLE"},
    {"name": "usage_count", "type": "STRING", "mode": "REQUIRED"},
    {"name": "created_at", "type": "STRING", "mode": "REQUIRED"},
    {"name": "updated_at", "type": "STRING", "mode": "REQUIRED"},
    {"name": "ingestion_date", "type": "STRING", "mode": "NULLABLE"},
]
