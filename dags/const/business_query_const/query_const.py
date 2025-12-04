REVENUE_BY_DATE_QUERY = """
    CREATE OR REPLACE TABLE `hackathon-478514.sale_ds.revenue_product_by_date` AS
    SELECT DATE(C.updated_at) as date, SUM(total_price) as total_price FROM `hackathon-478514.mart_ds.dm_fact_order_item` A
    join `hackathon-478514.mart_ds.dm_dim_product` B
    using (product_sk)
    join `hackathon-478514.mart_ds.dm_fact_order` C
    using (order_sk)
    WHERE C.status = 'Delivered'
    GROUP BY date
"""

REVENUE_QUERY = """
    CREATE OR REPLACE TABLE `hackathon-478514.sale_ds.revenue` AS
    SELECT C.updated_at as date,
    EXTRACT(DATE FROM C.updated_at) as day,
    EXTRACT(MONTH FROM C.updated_at) as month,
    EXTRACT(YEAR FROM C.updated_at) as year,
    total_price as total_price FROM `hackathon-478514.mart_ds.dm_fact_order_item` A
    join `hackathon-478514.mart_ds.dm_dim_product` B
    using (product_sk)
    join `hackathon-478514.mart_ds.dm_fact_order` C
    using (order_sk)
    WHERE C.status = 'Delivered';
"""

REVENUE_BY_CATEGORY_QUERY = """
    CREATE OR REPLACE TABLE `hackathon-478514.sale_ds.revenue_by_category` AS
    SELECT
    C.updated_at as date,
    IFNULL(D.name, 'Not categoried') as category_name,
    total_price as total_price
    FROM `hackathon-478514.mart_ds.dm_fact_order_item` A
    join `hackathon-478514.mart_ds.dm_dim_product` B
    using (product_sk)
    join `hackathon-478514.mart_ds.dm_fact_order` C
    using (order_sk)
    left join `hackathon-478514.mart_ds.dm_dim_category` D on B.category_id = D.category_id
    WHERE C.status = 'Delivered';
"""
