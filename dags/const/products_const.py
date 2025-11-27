RAW_TABLE_NAME = "products"
STG_TABLE_NAME = "products_standardized"
MART_TABLE_NAME = "dm_fact_products"
PK_COLUMNS = "product_id"
ORDER_COLUMNS = "updated_at"

SCHEMA_POSTGRES_FIELDS = [
    {"name": "product_id", "type": "STRING", "mode": "NOT NULL"},
    {"name": "sku", "type": "STRING", "mode": "NOT NULL"},
    {"name": "name", "type": "STRING", "mode": "NOT NULL"},
    {"name": "description", "type": "STRING", "mode": "NULLABLE"},
    {"name": "price", "type": "FLOAT64", "mode": "NOT NULL"},
    {"name": "currency", "type": "STRING", "mode": "NOT NULL"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "updated_at", "type": "TIMESTAMP", "mode": "NOT NULL"},
    {"name": "is_active", "type": "BOOL", "mode": "NOT NULL"},
    {"name": "sale_price", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "category_id", "type": "INT64", "mode": "NULLABLE"},
    {"name": "product_url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ingestion_date", "type": "DATE", "mode": "NULLABLE"},
]

SCHEMA_FIELDS = [
    {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "sku", "type": "STRING", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "description", "type": "STRING", "mode": "NULLABLE"},
    {"name": "price", "type": "STRING", "mode": "REQUIRED"},
    {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
    {"name": "created_at", "type": "STRING", "mode": "REQUIRED"},
    {"name": "updated_at", "type": "STRING", "mode": "REQUIRED"},
    {"name": "is_active", "type": "STRING", "mode": "REQUIRED"},
    {"name": "sale_price", "type": "STRING", "mode": "NULLABLE"},
    {"name": "category_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "product_url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ingestion_date", "type": "STRING", "mode": "NULLABLE"},
]
