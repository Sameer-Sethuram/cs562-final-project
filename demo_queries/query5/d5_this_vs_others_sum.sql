-- TEST: This customer's total vs the sum of all other customers' totals
-- WHAT IT TESTS: The self-referential predicate pattern that distinguishes
-- "this group" from "all other groups" --> the canonical EMF use case.
-- sigma_1 uses "1.cust = cust" (this row belongs to this group), sigma_2
-- uses "2.cust != cust" (this row belongs to a different group). The "cust"
-- on the right side of each comparison is the unprefixed V reference, which
-- translate_sigma_to_python wraps as entry['cust']. build_grouping_variable_scan
-- detects the V reference and switches to a nested for-row, for-entry loop so
-- each row can contribute to multiple groups.

WITH per_customer AS (
    SELECT cust, SUM(quant) AS total_quant
    FROM sales
    GROUP BY cust
),
grand_total AS (
    SELECT SUM(quant) AS total
    FROM sales
)
SELECT
    per_customer.cust,
    total_quant AS "1_sum_quant",
    total - total_quant AS "2_sum_quant"
FROM per_customer, grand_total
ORDER BY per_customer.cust;
