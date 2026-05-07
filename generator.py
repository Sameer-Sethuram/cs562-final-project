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


#builds Scan i (i in 1..n): re-executes SELECT, filters by sigma_i, applies gv==i updates.
#uses a nested row-x-entry loop when the predicate references a V grouping attribute
#(e.g. "2.cust != cust") so a single row can contribute to multiple groups.
def build_grouping_variable_scan(i, spec):
    pred = translate_sigma_to_python(spec.sigma[i - 1], spec)

    #if the predicate names entry['<v>'] for any V attr, every row may need to be tested
    #against every entry — that's how "all other customers" patterns work.
    references_grouping_attr = False
    for v in spec.V:
        if f"entry['{v}']" in pred:
            references_grouping_attr = True
            break

    lines = []
    lines.append('cur.execute("SELECT * FROM sales")')
    lines.append("for row in cur:")

    if references_grouping_attr:
        lines.append("    for entry in mf_struct.values():")
        lines.append(f"        if {pred}:")
        update_indent = "            "
    else:
        # --- build the key expression inline (same logic as in build_discovery_scan) ---
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

    for agg in spec.F:
        if agg.gv == i:
            update_block = build_aggregate_update_line(agg)
            for update_line in update_block.split("\n"):
                lines.append(f"{update_indent}{update_line}")

    return "\n".join(lines)


#emits a "for entry in mf_struct.values(): ..." block that divides any avg slots for gv_filter.
def build_avg_division_block(spec, gv_filter):
    avg_aggregates = []
    for agg in spec.F:
        if agg.gv == gv_filter and agg.func == "avg":
            avg_aggregates.append(agg)

    if not avg_aggregates:
        return ""

    lines = []
    lines.append("for entry in mf_struct.values():")
    for agg in avg_aggregates:
        #divide the accumulated sum by the count; guard against zero count.
        lines.append(f"    if entry['_count_{agg.key}'] != 0:")
        lines.append(f"        entry['{agg.key}'] = entry['_sum_{agg.key}'] / entry['_count_{agg.key}']")
        lines.append(f"    else:")
        lines.append(f"        entry['{agg.key}'] = None")

    return "\n".join(lines)


#translates the HAVING expression G into a python boolean over `entry`.
def translate_having_to_python(G, spec):
    s = G.strip()

    #store string literals as __LIT<n>__ so the rewrites below don't touch them.
    literals = re.findall(r"'[^']*'", s)
    for index in range(len(literals)):
        s = s.replace(literals[index], f"__LIT{index}__", 1)

    #wrap aggregate-shaped tokens (1_sum_quant, avg_quant, etc.) in entry['...'].
    s = re.sub(
        r"(?<!')\b((?:\d+_)?(?:sum|count|avg|min|max)_\w+)\b(?!')",
        r"entry['\1']",
        s,
    )

    for v in spec.V:
        s = re.sub(rf"(?<!')\b{re.escape(v)}\b(?!')", f"entry['{v}']", s)

    s = re.sub(r"(?<![<>!=])=(?!=)", "==", s)
    s = re.sub(r"\bAND\b", "and", s, flags=re.IGNORECASE)
    s = re.sub(r"\bOR\b",  "or",  s, flags=re.IGNORECASE)
    s = re.sub(r"\bNOT\b", "not", s, flags=re.IGNORECASE)

    for index in range(len(literals)):
        s = s.replace(f"__LIT{index}__", literals[index])

    return s


#builds the finalize stage: sort by V, apply HAVING, project to S (avgs already resolved by now).
def build_finalize_stage(spec):

    # --- build the sort key expression inline ---
    if len(spec.V) == 1:
        sort_key = f"e['{spec.V[0]}']"
    else:
        sort_parts = []
        for v in spec.V:
            sort_parts.append(f"e['{v}']")
        sort_key = "(" + ", ".join(sort_parts) + ")"

    # --- translate HAVING (or use True if there is none) ---
    if spec.G:
        having_expr = translate_having_to_python(spec.G, spec)
    else:
        having_expr = "True"

    # --- build the projection dict ---
    proj_parts = []
    for col in spec.S:
        proj_parts.append(f"'{col}': entry['{col}']")
    proj_dict = "{" + ", ".join(proj_parts) + "}"

    lines = []
    lines.append(f"for entry in sorted(mf_struct.values(), key=lambda e: {sort_key}):")
    lines.append(f"    if {having_expr}:")
    lines.append(f"        _global.append({proj_dict})")

    return "\n".join(lines)


#builds the full body string spliced into tmp's {body} placeholder.
def build_query_body(spec):
    parts = []
    parts.append("mf_struct = {}")
    parts.append(build_discovery_scan(spec))

    #resolve avg slots eagerly after each scan so later scans (EMF) and HAVING can read them.
    divide_zero = build_avg_division_block(spec, 0)
    if divide_zero:
        parts.append(divide_zero)

    for i in range(1, spec.n + 1):
        parts.append(build_grouping_variable_scan(i, spec))
        divide_i = build_avg_division_block(spec, i)
        if divide_i:
            parts.append(divide_i)

    parts.append(build_finalize_stage(spec))

    raw = "\n\n".join(parts)
    indented = textwrap.indent(raw, "    ")

    #strip line 1's leading 4 spaces since tmp's "    {body}" already provides them.
    if indented.startswith("    "):
        return indented[4:]
    return indented


#file-mode entry point: parses a phi-spec file at `path` and runs the generated query.
def main(path="inputs/three_states.txt"):
    spec = parse_phi_spec_file(path)
    _generate_and_run(spec)


#interactive entry point: prompts the user at the console for each operand, then runs.
def main_interactive():
    spec = parse_phi_spec_interactive()
    _generate_and_run(spec)


#shared back-end: builds the body, splices it into tmp, writes _generated.py, runs it.
def _generate_and_run(spec):
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


if "__main__" == __name__:
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main_interactive()