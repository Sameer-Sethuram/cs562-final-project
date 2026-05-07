#CS562 Final Project — Query Processing Engine (QPE) generator.
#parses a phi-operator spec file and emits a Python program that evaluates
#MF/EMF OLAP queries over the PostgreSQL `sales` table without relying on
#the DBMS for any aggregation.

import re
import subprocess
import sys
import textwrap
from dataclasses import dataclass, field


#columns of the sales table; identifies which bare names are column refs in predicates.
_SALES_COLUMNS = ("cust", "prod", "day", "month", "year", "state", "quant", "date")


@dataclass
class Aggregate:
    """One aggregate token from F-VECT, e.g. '1_sum_quant' or 'sum_quant'."""
    gv: int            #grouping-variable index (0 for bare aggregates)
    func: str          #'sum' | 'count' | 'avg' | 'min' | 'max'
    attr: str          #attribute being aggregated (e.g. 'quant')
    key: str           #original token; used as the dict key in generated code


@dataclass
class PhiSpec:
    S: list = field(default_factory=list)        #SELECT attributes
    n: int = 0                                   #number of grouping variables
    V: list = field(default_factory=list)        #GROUP BY attributes
    F: list = field(default_factory=list)        #F-VECT aggregates
    sigma: list = field(default_factory=list)    #per-GV predicates
    G: str = None                                #HAVING expression


#converts the six raw string operands into a PhiSpec — shared by the file and interactive readers.
def _phi_spec_from_values(s_val, n_val, v_val, f_val, sigma_val, g_val):

    # --- parse S (SELECT attributes) ---
    S = []
    for token in s_val.split(","):
        token = token.strip()
        if token != "":
            S.append(token)

    # --- parse n (number of grouping variables) ---
    if n_val != "":
        n = int(n_val)
    else:
        n = 0

    # --- parse V (GROUP BY attributes) ---
    V = []
    for token in v_val.split(","):
        token = token.strip()
        if token != "":
            V.append(token)

    # --- parse F (F-VECT aggregates) — each token like '1_sum_quant' or 'sum_quant' ---
    F = []
    for token in f_val.split(","):
        token = token.strip()
        if token == "":
            continue

        parts = token.split("_")
        if parts[0].isdigit():
            aggregate = Aggregate(
                gv=int(parts[0]),
                func=parts[1],
                attr="_".join(parts[2:]),
                key=token,
            )
        else:
            aggregate = Aggregate(
                gv=0,
                func=parts[0],
                attr="_".join(parts[1:]),
                key=token,
            )
        F.append(aggregate)

    # --- parse sigma (per-GV predicates, separated by semicolons) ---
    sigma = []
    for token in sigma_val.split(";"):
        token = token.strip()
        if token != "":
            sigma.append(token)

    # --- parse G (HAVING expression, may be blank) ---
    if g_val != "":
        G = g_val
    else:
        G = None

    return PhiSpec(S=S, n=n, V=V, F=F, sigma=sigma, G=G)


#reads a phi-spec file and parses its six operands into a PhiSpec dataclass.
def parse_phi_spec_file(path):
    lines = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped != "":
                lines.append(stripped)

    #traverse heading/value pairs: a heading contains ":", its value is the next non-heading line.
    values = []
    i = 0
    while i < len(lines):
        if ":" in lines[i]:
            if i + 1 < len(lines) and ":" not in lines[i + 1]:
                values.append(lines[i + 1])
                i += 2
            else:
                values.append("")
                i += 1
        else:
            i += 1

    while len(values) < 6:
        values.append("")

    return _phi_spec_from_values(values[0], values[1], values[2], values[3], values[4], values[5])


#prompts the user for each phi operand at the console and returns a PhiSpec.
def parse_phi_spec_interactive():
    print("Enter each Phi operand below.")
    print("Use commas in S, V, and F-VECT; use semicolons between per-GV predicates in sigma.")
    print("Leave HAVING blank for queries that don't filter groups.")
    print()
    s_val     = input("SELECT ATTRIBUTE(S):\n").strip()
    n_val     = input("NUMBER OF GROUPING VARIABLES(n):\n").strip()
    v_val     = input("GROUPING ATTRIBUTES(V):\n").strip()
    f_val     = input("F-VECT([F]):\n").strip()
    sigma_val = input("SELECT CONDITION-VECT([sigma]):\n").strip()
    g_val     = input("HAVING_CONDITION(G):\n").strip()

    return _phi_spec_from_values(s_val, n_val, v_val, f_val, sigma_val, g_val)


#builds Scan 0: discovers groups and applies any gv==0 aggregates.
#the group key expression and initial entry dict are built inline here.
def build_discovery_scan(spec):

    # --- build the key expression (single attr or tuple of attrs) ---
    if len(spec.V) == 1:
        key_expr = f"row['{spec.V[0]}']"
    else:
        key_parts = []
        for v in spec.V:
            key_parts.append(f"row['{v}']")
        key_expr = "(" + ", ".join(key_parts) + ")"

    # --- build the initial entry dict for a newly-discovered group ---
    init_parts = []
    for v in spec.V:
        init_parts.append(f"'{v}': row['{v}']")

    for agg in spec.F:
        if agg.func == "sum" or agg.func == "count":
            init_parts.append(f"'{agg.key}': 0")
        elif agg.func == "min" or agg.func == "max":
            init_parts.append(f"'{agg.key}': None")
        elif agg.func == "avg":
            #avg needs hidden _sum and _count slots so the finalize stage can divide.
            init_parts.append(f"'{agg.key}': None")
            init_parts.append(f"'_sum_{agg.key}': 0")
            init_parts.append(f"'_count_{agg.key}': 0")
        else:
            raise ValueError("unsupported aggregate func: " + agg.func)

    init_expr = "{" + ", ".join(init_parts) + "}"

    # --- emit the scan loop ---
    #no cur.execute here; scan 0 reuses the cursor that tmp already executed.
    lines = []
    lines.append("for row in cur:")
    lines.append(f"    key = {key_expr}")
    lines.append("    if key not in mf_struct:")
    lines.append(f"        mf_struct[key] = {init_expr}")
    lines.append("    entry = mf_struct[key]")

    for agg in spec.F:
        if agg.gv == 0:
            update_block = build_aggregate_update_line(agg)
            for update_line in update_block.split("\n"):
                lines.append(f"    {update_line}")

    return "\n".join(lines)


#builds one source line (or two for avg) that updates a single aggregate in `entry` from `row`.
def build_aggregate_update_line(agg):
    if agg.func == "sum":
        return f"entry['{agg.key}'] += row['{agg.attr}']"

    if agg.func == "count":
        return f"entry['{agg.key}'] += 1"

    if agg.func == "min":
        line  = f"entry['{agg.key}'] = row['{agg.attr}']"
        line += f" if entry['{agg.key}'] is None"
        line += f" else min(entry['{agg.key}'], row['{agg.attr}'])"
        return line

    if agg.func == "max":
        line  = f"entry['{agg.key}'] = row['{agg.attr}']"
        line += f" if entry['{agg.key}'] is None"
        line += f" else max(entry['{agg.key}'], row['{agg.attr}'])"
        return line

    if agg.func == "avg":
        sum_line   = f"entry['_sum_{agg.key}'] += row['{agg.attr}']"
        count_line = f"entry['_count_{agg.key}'] += 1"
        return sum_line + "\n" + count_line

    raise ValueError("unsupported aggregate func: " + agg.func)


#translates one sigma predicate into a python boolean over `row` (current scan row),
#`entry` (the group's V values), and aggregates from earlier scans.
def translate_sigma_to_python(sigma_i, spec):
    s = sigma_i.strip()

    #store string literals as __LIT<n>__ so the rewrites below don't touch them.
    literals = re.findall(r"'[^']*'", s)
    for index in range(len(literals)):
        s = s.replace(literals[index], f"__LIT{index}__", 1)

    #prefixed column refs (1.cust, 2.state) refer to the row currently being scanned.
    for col in _SALES_COLUMNS:
        s = re.sub(rf"\b\d+\.{col}\b", f"row['{col}']", s)

    #strip any leftover "<digits>." (e.g. before non-column tokens).
    s = re.sub(r"\b\d+\.", "", s)

    #wrap aggregate-shaped tokens (1_avg_quant, sum_quant, etc.) in entry['...'] so the
    #predicate can reference values that earlier grouping-variable scans have computed.
    s = re.sub(
        r"(?<!')\b((?:\d+_)?(?:sum|count|avg|min|max)_\w+)\b(?!')",
        r"entry['\1']",
        s,
    )

    #unprefixed V grouping-attribute refs point to the group's value (entry), not the row —
    #this is what makes "1.cust = cust" tautological and "2.cust != cust" mean "other groups".
    for v in spec.V:
        s = re.sub(rf"(?<!')\b{re.escape(v)}\b(?!')", f"entry['{v}']", s)

    #any remaining bare sales columns refer to the row.
    for col in _SALES_COLUMNS:
        s = re.sub(rf"(?<!')\b{col}\b(?!')", f"row['{col}']", s)

    #single "=" becomes "==", but leaves <=, >=, !=, == alone.
    s = re.sub(r"(?<![<>!=])=(?!=)", "==", s)

    s = re.sub(r"\bAND\b", "and", s, flags=re.IGNORECASE)
    s = re.sub(r"\bOR\b",  "or",  s, flags=re.IGNORECASE)
    s = re.sub(r"\bNOT\b", "not", s, flags=re.IGNORECASE)

    for index in range(len(literals)):
        s = s.replace(f"__LIT{index}__", literals[index])

    return s


#builds Scan i (i in 1..n): re-executes SELECT, filters by sigma_i, applies gv==i updates to populate the table we want.
#Ensures that EMF queries are  also accomodated for, as we use a nested row-x-entry loop
def build_grouping_variable_scan(i, spec):
    pred = translate_sigma_to_python(spec.sigma[i - 1], spec)

    references_grouping_attr = False #this tells us if it's an EMF query or an MF query.
    for v in spec.V: #this figures out what type our query actually is.
        if f"entry['{v}']" in pred:
            references_grouping_attr = True
            break

    #simple scan to accomodate such that clause from Phi variable.
    lines = []
    lines.append('cur.execute("SELECT * FROM sales")')
    lines.append("for row in cur:")

    if references_grouping_attr:
        lines.append("    for entry in mf_struct.values():") #this is the nested loop that allows us to handle EMF queries.
        lines.append(f"        if {pred}:")
        update_indent = "            "
    else:
        #build the key expression inline (same logic as in build_discovery_scan)
        if len(spec.V) == 1:
            key_expr = f"row['{spec.V[0]}']"
        else:
            key_parts = []
            for v in spec.V:
                key_parts.append(f"row['{v}']")
            key_expr = "(" + ", ".join(key_parts) + ")"

        lines.append(f"    key = {key_expr}")
        lines.append("    entry = mf_struct[key]")
        lines.append(f"    if {pred}:")
        update_indent = "        "
    
    #updates the aggregates for the grouping variable being operated on.
    #essentially going bucket by bucket and computing the aggregates for each bucket one at a time.
    for agg in spec.F:
        if agg.gv == i:
            update_block = build_aggregate_update_line(agg)
            for update_line in update_block.split("\n"):
                lines.append(f"{update_indent}{update_line}")

    return "\n".join(lines)


#computes the final averages amongst grouping variable aggregates.
def build_avg_division_block(spec, gv_filter):
    avg_aggregates = [] # a list holding whatever rows need to have their averages computed
    for agg in spec.F:
        if agg.gv == gv_filter and agg.func == "avg":
            avg_aggregates.append(agg)

    if not avg_aggregates: #early return if we know we don't need to compute any averages.
        return ""

    lines = []
    lines.append("for entry in mf_struct.values():")
    for agg in avg_aggregates:
        #divide the accumulated sum by the count while guard against zero count.
        lines.append(f"    if entry['_count_{agg.key}'] != 0:")
        lines.append(f"        entry['{agg.key}'] = entry['_sum_{agg.key}'] / entry['_count_{agg.key}']")
        lines.append(f"    else:")
        lines.append(f"        entry['{agg.key}'] = None")

    return "\n".join(lines)


#translates the HAVING expression G into a python boolean over `entry`.
def translate_having_to_python(G, spec):
    s = G.strip()

    #store string literals as __LIT<n>__ so the rewrites below don't accidentally corrupt our expression.
    literals = re.findall(r"'[^']*'", s)
    for index in range(len(literals)):
        s = s.replace(literals[index], f"__LIT{index}__", 1)

    #claude helped us with the following regexes we are using here.
    #wrap aggregate-shaped tokens (1_sum_quant, avg_quant, etc.) in entry['...'].
    s = re.sub(
        r"(?<!')\b((?:\d+_)?(?:sum|count|avg|min|max)_\w+)\b(?!')",
        r"entry['\1']",
        s,
    )

    #does the same thing to the grouping attributes that we have
    #this is just in case corruption occurs
    for v in spec.V:
        s = re.sub(rf"(?<!')\b{re.escape(v)}\b(?!')", f"entry['{v}']", s)

    #converts the logical operators we see in phi to python logical operators.
    s = re.sub(r"(?<![<>!=])=(?!=)", "==", s)
    s = re.sub(r"\bAND\b", "and", s, flags=re.IGNORECASE)
    s = re.sub(r"\bOR\b",  "or",  s, flags=re.IGNORECASE)
    s = re.sub(r"\bNOT\b", "not", s, flags=re.IGNORECASE)

    #puts the literals back to where they were.
    for index in range(len(literals)):
        s = s.replace(f"__LIT{index}__", literals[index])

    return s


#formats and grouping attributes and applies the last filters (HAVING clause) to the table, afterwards projecting to S defined in the spec.
def build_finalize_stage(spec):

    #sort/format the grouping variables using an inline expression.
    if len(spec.V) == 1:
        sort_key = f"e['{spec.V[0]}']"
    else:
        sort_parts = []
        for v in spec.V:
            sort_parts.append(f"e['{v}']")
        sort_key = "(" + ", ".join(sort_parts) + ")"

    #translate HAVING to python code (or use True if there is none) according to the spec given.
    if spec.G:
        having_expr = translate_having_to_python(spec.G, spec) #uses the function we created earlier.
    else:
        having_expr = "True"

    #build the projection dict so that our table only contains the columns we wanted from the spec.
    proj_parts = []
    for col in spec.S:
        proj_parts.append(f"'{col}': entry['{col}']")
    proj_dict = "{" + ", ".join(proj_parts) + "}"

    #apply those three builds to the table using this python code
    lines = []
    lines.append(f"for entry in sorted(mf_struct.values(), key=lambda e: {sort_key}):") #sorts the grouping variables
    lines.append(f"    if {having_expr}:") #applies the having expression (filters out the values that don't fit)
    lines.append(f"        _global.append({proj_dict})") #ensure that the values are stored in the table that only has the columns we want.

    return "\n".join(lines)


#constructs the entire query using all of the functions that we defined above.
def build_query_body(spec):
    parts = []
    parts.append("mf_struct = {}")
    parts.append(build_discovery_scan(spec)) #scan 0 code, appended to the final output

    #computes the average aggregates for the averages that are computed over the table as the 0th grouping variable.
    divide_zero = build_avg_division_block(spec, 0)
    if divide_zero:
        parts.append(divide_zero) #primarily for assistance with EMF queries and the HAVING clause.

    #computes the other grouping variable scans and average computations in the same fashion as above.
    for i in range(1, spec.n + 1):
        parts.append(build_grouping_variable_scan(i, spec))
        divide_i = build_avg_division_block(spec, i)
        if divide_i:
            parts.append(divide_i)

    parts.append(build_finalize_stage(spec)) #wraps up the sorting, the HAVING clause, and the projection that we need.

    raw = "\n\n".join(parts)
    indented = textwrap.indent(raw, "    ")

    #strip line 1's leading 4 spaces since tmp's "    {body}" already provides them.
    if indented.startswith("    "):
        return indented[4:]
    return indented

#runs main to generate the equivalent query.
def main():
    if len(sys.argv) > 1: #this is to process the case where want to input a path.
        spec = parse_phi_spec_file(sys.argv[1])
    else: #this is to process the case where the user wants to provide the spec interactively.
        spec = parse_phi_spec_interactive()

    #builds the python code to process the query.
    body = build_query_body(spec)

    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")

    _global = []
    {body}

    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql")

def main():
    print(query())

if "__main__" == __name__:
    main()
    """

    open("_generated.py", "w").write(tmp)
    subprocess.run(["python", "_generated.py"])

if __name__ == "__main__":
    main()