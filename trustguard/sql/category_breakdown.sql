-- =============================================================================
-- TrustGuard SQL: Category Breakdown (Revenue + Order Count)
-- Target table: final_transactions (Delta Lake)
-- =============================================================================

SELECT
    Category,
    COUNT(Transaction_ID)               AS order_count,
    ROUND(SUM(Total_Spent), 2)          AS total_revenue,
    ROUND(AVG(Total_Spent), 2)          AS avg_per_order,
    ROUND(MIN(Total_Spent), 2)          AS min_order_value,
    ROUND(MAX(Total_Spent), 2)          AS max_order_value,
    ROUND(100.0 * SUM(Total_Spent) /
          SUM(SUM(Total_Spent)) OVER (), 2) AS revenue_share_pct
FROM
    final_transactions
WHERE
    Category IS NOT NULL
GROUP BY
    Category
ORDER BY
    total_revenue DESC;
