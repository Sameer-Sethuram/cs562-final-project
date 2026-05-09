-- TEST: Customers with substantial NY purchase totals and counts
-- WHAT IT TESTS: A single-grouping-variable scan combined with HAVING on
-- a grouping-variable aggregate. Confirms: Scan 0 discovers all customer
-- keys without computing any bare aggregate; Scan 1 re-executes, filters
-- by sigma_1 (state='NY'), and updates BOTH 1_sum_quant and 1_count_quant
-- in the same bucket on each row that passes the predicate; the finalize
-- stage applies HAVING via translate_having_to_python wrapping 1_sum_quant
-- as entry['1_sum_quant'].

WITH ny_purchases AS (
    SELECT cust, SUM(quant)   AS "1_sum_quant", COUNT(quant) AS "1_count_quant"
    FROM sales
    WHERE state = 'NY'
    GROUP BY cust
    HAVING SUM(quant) > 1000
)
SELECT cust, "1_sum_quant", "1_count_quant"
FROM ny_purchases
ORDER BY cust;
