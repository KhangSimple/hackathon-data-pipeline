# RAW_TABLE_NAME = "users"
# STG_TABLE_NAME = "users_standardized"
# MART_TABLE_NAME = "dm_dim_users"
N_DAYS_EXPRIRED = 3

PK_COLUMNS = ["user_id"]
ORDER_COLUMNS = "updated_at"

SCHEMA_POSTGRES_FIELDS = [
    {"name": "user_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "email", "type": "STRING", "mode": "NOT NULL"},
    {"name": "full_name", "type": "STRING", "mode": "NOT NULL"},
    {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
    {"name": "address", "type": "STRING", "mode": "NULLABLE"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "role_id", "type": "INT64", "mode": "NOT NULL"},
    {"name": "is_active", "type": "BOOL", "mode": "NOT NULL"},
    {"name": "date_of_birth", "type": "DATE", "mode": "NULLABLE"},
    {"name": "job", "type": "STRING", "mode": "NULLABLE"},
    {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ingestion_date", "type": "DATE", "mode": "NULLABLE"},
]

SCHEMA_FIELDS = [
    {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "email", "type": "STRING", "mode": "REQUIRED"},
    {"name": "full_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
    {"name": "address", "type": "STRING", "mode": "NULLABLE"},
    {"name": "created_at", "type": "STRING", "mode": "NULLABLE"},
    {"name": "updated_at", "type": "STRING", "mode": "NULLABLE"},
    {"name": "role_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "is_active", "type": "STRING", "mode": "REQUIRED"},
    {"name": "date_of_birth", "type": "STRING", "mode": "NULLABLE"},
    {"name": "job", "type": "STRING", "mode": "NULLABLE"},
    {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ingestion_date", "type": "STRING", "mode": "NULLABLE"},
]
