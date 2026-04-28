import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv


def query():
    """
    Used for testing standard queries in SQL.
    """
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("""SELECT
    cust,
    SUM(CASE WHEN state = 'NY' THEN quant ELSE 0 END) AS "1_sum_quant",
    SUM(CASE WHEN state = 'NJ' THEN quant ELSE 0 END) AS "2_sum_quant",
    SUM(CASE WHEN state = 'CT' THEN quant ELSE 0 END) AS "3_sum_quant"
FROM sales
GROUP BY cust
HAVING SUM(CASE WHEN state = 'NY' THEN quant ELSE 0 END) > SUM(CASE WHEN state = 'NJ' THEN quant ELSE 0 END)
ORDER BY cust;
""")

    return tabulate.tabulate(cur.fetchall(),
                             headers="keys", tablefmt="psql")


def main():
    print(query())


if "__main__" == __name__:
    main()
