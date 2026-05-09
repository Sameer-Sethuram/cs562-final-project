-- TEST: EMF dependent aggregate --> NJ count only for customers who also bought in NY
-- WHAT IT TESTS: The "extended" in EMF --> sigma_2's predicate references
-- 1_count_quant, an aggregate computed by sigma_1's scan. This is the defining
-- feature that distinguishes EMF queries from plain MF: a grouping variable's
-- predicate can read aggregates produced by earlier grouping variables.
-- translate_sigma_to_python wraps 1_count_quant in entry['...'] (mirroring
-- what translate_having_to_python does for HAVING) and the scan binds entry
-- before evaluating the predicate.

WITH ny_counts AS (
    SELECT cust, COUNT(quant) AS "1_count_quant"
    FROM sales
    WHERE state = 'NY'
    GROUP BY cust
),
nj_counts AS (
    SELECT cust, COUNT(quant) AS "2_count_quant"
    FROM sales
    WHERE state = 'NJ'
    GROUP BY cust
)
SELECT ny_counts.cust, "1_count_quant", "2_count_quant"
FROM ny_counts
JOIN nj_counts ON ny_counts.cust = nj_counts.cust
ORDER BY ny_counts.cust;
