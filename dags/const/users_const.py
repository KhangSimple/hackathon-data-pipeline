# RAW_TABLE_NAME = "users"
# STG_TABLE_NAME = "users_standardized"
# MART_TABLE_NAME = "dm_dim_users"
N_DAYS_EXPRIRED = 3

POSTGRES_QUERY_RAW_DATA = """
    SELECT
        CAST(user_id AS TEXT),
        email,
        full_name,
        phone,
        address,
        TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at,
        TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
        CAST(role_id AS TEXT) as role_id,
        CAST(is_active AS TEXT) as is_active,
        '{ingestion_date}' AS ingestion_date
    FROM {RAW_TABLE_NAME} order by user_id
"""

TRANSFORM_QUERY_STANDARDIZE = """
    INSERT INTO `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{STG_TABLE_NAME}`
    SELECT
        user_id,
        email,
        full_name,
        phone,
        address,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', created_at) AS created_at,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', updated_at) AS updated_at,
        CAST(role_id AS INT64) AS role_id,
        CAST(is_active AS BOOL) as is_active,
        PARSE_DATE('%Y-%m-%d', ingestion_date) AS ingestion_date
    FROM `{BQ_PROJECT_ID}.{BQ_RAW_DATASET_NAME}.{RAW_TABLE_NAME}`
    WHERE ingestion_date = '{ingestion_date}'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY updated_at DESC) = 1;
"""

MART_QUERY_FACT = """
    -- assume staging contains only changed/new rows since last run
    MERGE INTO `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.{MART_TABLE_NAME}` AS dim_users
    USING (
    SELECT * FROM `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{STG_TABLE_NAME}` stg_users WHERE ingestion_date = '{ingestion_date}'
    ) AS stg_users
    ON dim_users.user_id = stg_users.user_id AND dim_users.is_current = TRUE
    WHEN MATCHED AND (
        COALESCE(dim_users.email,'') != COALESCE(stg_users.email,'') OR
        COALESCE(dim_users.full_name,'') != COALESCE(stg_users.full_name,'') OR
        COALESCE(dim_users.phone,'') != COALESCE(stg_users.phone,'') OR
        COALESCE(dim_users.address,'') != COALESCE(stg_users.address,'') OR
        COALESCE(dim_users.is_active, FALSE) != COALESCE(stg_users.is_active, FALSE)
    ) THEN
    -- close old
    UPDATE SET is_current = FALSE, effective_to = CURRENT_TIMESTAMP()
    ;
    -- -- insert new versions for changed rows and insert for not matched
    INSERT INTO `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.{MART_TABLE_NAME}`
    (user_sk, user_id, email, full_name, phone, address, role_id, is_active, effective_from, effective_to, is_current, dw_inserted_at)
    SELECT GENERATE_UUID(), stg_users.user_id, stg_users.email, stg_users.full_name, stg_users.phone, stg_users.address, stg_users.role_id, stg_users.is_active,
        CURRENT_TIMESTAMP(), NULL, TRUE, CURRENT_TIMESTAMP()
    FROM `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{STG_TABLE_NAME}` stg_users
    WHERE NOT EXISTS
    (
        SELECT user_id from `{BQ_PROJECT_ID}.{BQ_MART_DATASET_NAME}.{MART_TABLE_NAME}` dim_users
        WHERE dim_users.user_id = stg_users.user_id
        AND (
            COALESCE(dim_users.email,'') = COALESCE(stg_users.email,'') AND
            COALESCE(dim_users.full_name,'') = COALESCE(stg_users.full_name,'') AND
            COALESCE(dim_users.phone,'') = COALESCE(stg_users.phone,'') AND
            COALESCE(dim_users.address,'') = COALESCE(stg_users.address,'') AND
            COALESCE(dim_users.is_active, FALSE) = COALESCE(stg_users.is_active, FALSE)
        )
    )
"""

QUERY_DELETE_OLD_N_DAYS_DATA = """
    DELETE FROM `{BQ_PROJECT_ID}.{BQ_RAW_DATASET_NAME}.{RAW_TABLE_NAME}`
    WHERE PARSE_DATE('%Y-%m-%d', ingestion_date) < DATE_SUB(PARSE_DATE('%Y-%m-%d', '{current_date}'), INTERVAL {n_days} DAY);

    DELETE FROM `{BQ_PROJECT_ID}.{BQ_STG_DATASET_NAME}.{STG_TABLE_NAME}`
    WHERE ingestion_date < DATE_SUB(PARSE_DATE('%Y-%m-%d', '{current_date}'), INTERVAL {n_days} DAY);
"""

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
    {"name": "ingestion_date", "type": "STRING", "mode": "NULLABLE"},
]
