-- =============================================================================
-- TrustGuard SQL: Payment Method Share
-- Target table: final_transactions (Delta Lake)
-- =============================================================================

SELECT
    Payment_Method,
    COUNT(*)                                                        AS transaction_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)             AS pct_share,
    ROUND(SUM(Total_Spent), 2)                                      AS total_revenue,
    ROUND(AVG(Total_Spent), 2)                                      AS avg_transaction_value
FROM
    final_transactions
WHERE
    Payment_Method IS NOT NULL
GROUP BY
    Payment_Method
ORDER BY
    transaction_count DESC;
