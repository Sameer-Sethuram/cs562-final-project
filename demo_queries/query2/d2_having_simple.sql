-- TEST: HAVING with a single bare aggregate
-- WHAT IT TESTS: The simplest HAVING path --> gv=0 sum, no grouping variables,
-- only a single comparison. Confirms translate_having_to_python translates
-- 'sum_quant > 1000000' to "entry['sum_quant'] > 1000000" and the finalize
-- loop drops failing rows.

SELECT cust, SUM(quant) AS sum_quant
FROM sales
GROUP BY cust
HAVING SUM(quant) > 1000000
ORDER BY cust;
