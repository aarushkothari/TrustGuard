-- =============================================================================
-- TrustGuard SQL: Monthly Revenue by City
-- Target table: final_transactions (Delta Lake)
-- =============================================================================

SELECT
    YEAR(Date)           AS year,
    MONTH(Date)          AS month,
    Location             AS city,
    ROUND(SUM(Total_Spent), 2)   AS total_revenue,
    COUNT(Transaction_ID)         AS order_count,
    ROUND(AVG(Total_Spent), 2)   AS avg_order_value
FROM
    final_transactions
WHERE
    Date IS NOT NULL
GROUP BY
    YEAR(Date), MONTH(Date), Location
ORDER BY
    year DESC, month DESC, total_revenue DESC;
