-- Query 3: Sales Channel Profitability Analysis
-- Purpose: Compare net sales, gross margin and profitability
-- between Physical Pharmacy and Ecommerce channels.

SELECT
    sales_channel,
    COUNT(*) AS total_transactions,
    SUM(quantity_sold_packs) AS total_packs_sold,
    ROUND(SUM(net_sales_amount_usd), 2) AS total_net_sales_usd,
    ROUND(SUM(gross_margin_usd), 2) AS total_gross_margin_usd,
    ROUND(
        100.0 * SUM(gross_margin_usd)
        / NULLIF(SUM(net_sales_amount_usd), 0), 2
    ) AS gross_margin_pct
FROM mart.fact_sales
GROUP BY sales_channel
ORDER BY total_net_sales_usd DESC;
