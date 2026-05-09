-- TEST: Customers whose NY total exceeds their NJ total
-- WHAT IT TESTS: A HAVING predicate that compares two grouping-variable
-- aggregates against each other -- not a bare comparison against a constant.
-- Confirms translate_having_to_python wraps both 1_sum_quant and 2_sum_quant
-- as entry['...'] lookups so the comparison happens between bucket fields.

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
WHERE "1_sum_quant" > "2_sum_quant"
ORDER BY ny_sums.cust;
