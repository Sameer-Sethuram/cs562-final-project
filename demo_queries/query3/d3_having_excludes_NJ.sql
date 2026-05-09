-- TEST: Customers who bought in NY but never in NJ
-- WHAT IT TESTS: A negative-existence check via "= 0" in HAVING. A real
-- EMF pattern: "show me X who is also NOT Y". Confirms the single '='
-- becomes Python's '==' in translate_having_to_python; the predicate is
-- well-defined because count starts at 0 and stays 0 when no NJ rows are seen.

WITH ny_sums AS (
    SELECT cust, SUM(quant) AS "1_sum_quant"
    FROM sales
    WHERE state = 'NY'
    GROUP BY cust
),
nj_customers AS (
    SELECT cust
    FROM sales
    WHERE state = 'NJ'
    GROUP BY cust
)
SELECT cust, "1_sum_quant", 0 AS "2_count_quant"
FROM ny_sums
WHERE cust NOT IN (SELECT cust FROM nj_customers)
ORDER BY cust;
