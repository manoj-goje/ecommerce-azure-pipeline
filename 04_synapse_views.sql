-- =============================================================================
-- 04_synapse_views.sql
-- E-Commerce Sales Analytics Pipeline — Synapse Analytics Gold Views
-- Run these in Azure Synapse Analytics (Serverless SQL Pool)
-- =============================================================================

-- Replace 'yourstorageaccount' with your actual ADLS Gen2 account name

-- -------------------------------------------------------
-- VIEW 1: Monthly Revenue Dashboard
-- -------------------------------------------------------
CREATE OR ALTER VIEW gold.vw_monthly_revenue AS
SELECT
    order_year,
    order_month,
    total_revenue,
    total_orders,
    unique_customers,
    avg_order_value,
    max_order_value,
    -- Month-over-Month revenue growth %
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (ORDER BY order_year, order_month))
        / NULLIF(LAG(total_revenue) OVER (ORDER BY order_year, order_month), 0) * 100,
        2
    ) AS mom_revenue_growth_pct
FROM
    OPENROWSET(
        BULK 'https://yourstorageaccount.dfs.core.windows.net/ecommerce-gold/monthly_revenue/**',
        FORMAT = 'DELTA'
    ) AS [result];
GO

-- -------------------------------------------------------
-- VIEW 2: Top 10 Products by Revenue
-- -------------------------------------------------------
CREATE OR ALTER VIEW gold.vw_top_products AS
SELECT TOP 10
    product_id,
    product_name,
    category,
    price_tier,
    units_sold,
    product_revenue,
    order_count,
    revenue_rank
FROM
    OPENROWSET(
        BULK 'https://yourstorageaccount.dfs.core.windows.net/ecommerce-gold/top_products/**',
        FORMAT = 'DELTA'
    ) AS [result]
ORDER BY revenue_rank ASC;
GO

-- -------------------------------------------------------
-- VIEW 3: Customer CLV Segments Summary
-- -------------------------------------------------------
CREATE OR ALTER VIEW gold.vw_clv_summary AS
SELECT
    clv_segment,
    COUNT(*)                     AS customer_count,
    ROUND(SUM(lifetime_value),2) AS segment_total_revenue,
    ROUND(AVG(lifetime_value),2) AS avg_clv,
    ROUND(AVG(total_orders),2)   AS avg_orders_per_customer
FROM
    OPENROWSET(
        BULK 'https://yourstorageaccount.dfs.core.windows.net/ecommerce-gold/customer_clv/**',
        FORMAT = 'DELTA'
    ) AS [result]
GROUP BY clv_segment
ORDER BY avg_clv DESC;
GO

-- -------------------------------------------------------
-- VIEW 4: Executive KPI Summary (single-row dashboard feed)
-- -------------------------------------------------------
CREATE OR ALTER VIEW gold.vw_kpi_summary AS
SELECT
    SUM(total_revenue)        AS total_revenue_all_time,
    SUM(total_orders)         AS total_orders_all_time,
    SUM(unique_customers)     AS total_unique_customers,
    ROUND(AVG(avg_order_value), 2) AS overall_avg_order_value,
    MAX(max_order_value)      AS highest_single_order
FROM
    OPENROWSET(
        BULK 'https://yourstorageaccount.dfs.core.windows.net/ecommerce-gold/monthly_revenue/**',
        FORMAT = 'DELTA'
    ) AS [result];
GO
