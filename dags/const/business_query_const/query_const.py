REVENUE_BY_DATE_QUERY = """
    CREATE OR REPLACE TABLE `hackathon-478514.sale_ds.revenue_product_by_date` AS
    SELECT DATE(C.updated_at) as date, SUM(total_price) as total_price FROM `hackathon-478514.mart_ds.dm_fact_order_item` A
    join `hackathon-478514.mart_ds.dm_dim_product` B
    using (product_sk)
    join `hackathon-478514.mart_ds.dm_fact_order` C
    using (order_sk)
    WHERE C.status = 'delivered'
    GROUP BY date
"""

REVENUE_QUERY = """
    CREATE OR REPLACE TABLE `hackathon-478514.sale_ds.revenue` AS
    SELECT A.updated_at as date,
    EXTRACT(DATE FROM A.updated_at) as day,
    EXTRACT(MONTH FROM A.updated_at) as month,
    EXTRACT(YEAR FROM A.updated_at) as year,
    total_price as total_price FROM `hackathon-478514.mart_ds.dm_fact_order_item` A
    join `hackathon-478514.mart_ds.dm_dim_product` B
    using (product_sk)
    join `hackathon-478514.mart_ds.dm_fact_order` C
    using (order_sk)
    WHERE C.status = 'delivered';
"""
