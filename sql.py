import os
import sys
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv


# Default query corresponding to inputs/three_states.txt's ESQL spec.
# test_generator.py calls query() with no args and compares this against
# the output of _generated.py for that same spec.
_DEFAULT_QUERY = """SELECT
    cust,
    SUM(CASE WHEN state = 'NY' THEN quant ELSE 0 END) AS "1_sum_quant",
    SUM(CASE WHEN state = 'NJ' THEN quant ELSE 0 END) AS "2_sum_quant",
    SUM(CASE WHEN state = 'CT' THEN quant ELSE 0 END) AS "3_sum_quant"
FROM sales
GROUP BY cust
HAVING SUM(CASE WHEN state = 'NY' THEN quant ELSE 0 END) > SUM(CASE WHEN state = 'NJ' THEN quant ELSE 0 END)
ORDER BY cust;
"""


def query(path=None):
    """
    Used for testing standard queries in SQL.

    If `path` is provided, read the SQL text from that file and execute it.
    Otherwise run the default query (kept for test_generator.py compatibility).
    """
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    if path is not None:
        with open(path, "r", encoding="utf-8") as f:
            sql_text = f.read()
    else:
        sql_text = _DEFAULT_QUERY

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute(sql_text)

    return tabulate.tabulate(cur.fetchall(),
                             headers="keys", tablefmt="psql")


def main():
    if len(sys.argv) > 1:
        print(query(sys.argv[1]))
    else:
        print(query())


if "__main__" == __name__:
    main()
