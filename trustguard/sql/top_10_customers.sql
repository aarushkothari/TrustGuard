-- =============================================================================
-- TrustGuard SQL: Top 10 Customers by Spend
-- Target table: customer_summary (Delta Lake)
-- =============================================================================

SELECT
    Customer_ID,
    ROUND(total_spend, 2)       AS total_spend,
    order_count,
    ROUND(avg_order_value, 2)   AS avg_order_value,
    last_purchase_date,
    loyalty_tier
FROM
    customer_summary
ORDER BY
    total_spend DESC
LIMIT 10;
