-- TEST: NY and NJ sums per customer
-- WHAT IT TESTS: Two grouping variables side by side. Two cur.execute calls
-- in body, both updating different columns of the same mf_struct entry.

WITH ny_sums AS (
    SELECT cust, SUM(quant) AS "1_sum_quant"
    FROM sales
    WHERE state = 'NY'
    GROUP BY cust
),
nj_sums AS (
    SELECT cust, SUM(quant) AS "2_sum_quant"
    FROM sales
    WHERE state = 'NJ'
    GROUP BY cust
)
SELECT ny_sums.cust, "1_sum_quant", "2_sum_quant"
FROM ny_sums
JOIN nj_sums ON ny_sums.cust = nj_sums.cust
ORDER BY ny_sums.cust;
