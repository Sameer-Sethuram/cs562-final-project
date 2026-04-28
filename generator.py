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


#parses an aggregate token like '1_sum_quant' or 'sum_quant' into an Aggregate.
def _parse_aggregate_token(token):
    t = token.strip()
    parts = t.split("_")
    if parts[0].isdigit():
        return Aggregate(gv=int(parts[0]), func=parts[1],
                         attr="_".join(parts[2:]), key=t)
    return Aggregate(gv=0, func=parts[0],
                     attr="_".join(parts[1:]), key=t)


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

    s_val = values[0]
    n_val = values[1]
    v_val = values[2]
    f_val = values[3]
    sigma_val = values[4]
    g_val = values[5]

    S = []
    for t in s_val.split(","):
        t = t.strip()
        if t != "":
            S.append(t)

    if n_val != "":
        n = int(n_val)
    else:
        n = 0

    V = []
    for t in v_val.split(","):
        t = t.strip()
        if t != "":
            V.append(t)

    F = []
    for t in f_val.split(","):
        t = t.strip()
        if t != "":
            F.append(_parse_aggregate_token(t))

    sigma = []
    for t in sigma_val.split(";"):
        t = t.strip()
        if t != "":
            sigma.append(t)

    if g_val != "":
        G = g_val
    else:
        G = None

    return PhiSpec(S=S, n=n, V=V, F=F, sigma=sigma, G=G)


#builds the python expression for the mf_struct dict key (single attr or tuple).
def build_group_key_expression(V):
    if len(V) == 1:
        return f"row['{V[0]}']"

    parts = []
    for v in V:
        parts.append(f"row['{v}']")
    return "(" + ", ".join(parts) + ")"


#builds the dict-literal source for a newly-discovered group's initial row.
def build_initial_entry_dict(V, F):
    parts = []
    for v in V:
        parts.append(f"'{v}': row['{v}']")

    for agg in F:
        if agg.func == "sum" or agg.func == "count":
            parts.append(f"'{agg.key}': 0")
        elif agg.func == "min" or agg.func == "max":
            parts.append(f"'{agg.key}': None")
        elif agg.func == "avg":
            #avg needs hidden _sum_X and _count_X slots so finalize can divide.
            parts.append(f"'{agg.key}': None")
            parts.append(f"'_sum_{agg.key}': 0")
            parts.append(f"'_count_{agg.key}': 0")
        else:
            raise ValueError("unsupported aggregate func: " + agg.func)

    return "{" + ", ".join(parts) + "}"


#builds one source line that updates a single aggregate in `entry` from `row`.
def build_aggregate_update_line(agg):
    if agg.func == "sum":
        return f"entry['{agg.key}'] += row['{agg.attr}']"
    if agg.func == "count":
        return f"entry['{agg.key}'] += 1"
    if agg.func == "min":
        line = f"entry['{agg.key}'] = row['{agg.attr}']"
        line += f" if entry['{agg.key}'] is None"
        line += f" else min(entry['{agg.key}'], row['{agg.attr}'])"
        return line
    if agg.func == "max":
        line = f"entry['{agg.key}'] = row['{agg.attr}']"
        line += f" if entry['{agg.key}'] is None"
        line += f" else max(entry['{agg.key}'], row['{agg.attr}'])"
        return line
    if agg.func == "avg":
        sum_line = f"entry['_sum_{agg.key}'] += row['{agg.attr}']"
        count_line = f"entry['_count_{agg.key}'] += 1"
        return sum_line + "\n" + count_line

    raise ValueError("unsupported aggregate func: " + agg.func)


#translates one sigma predicate into a python boolean expression over `row`.
def translate_sigma_to_python(sigma_i):
    s = sigma_i.strip()

    #store string literals as __LIT<n>__ so the rewrites below don't touch them.
    literals = re.findall(r"'[^']*'", s)
    for idx in range(len(literals)):
        s = s.replace(literals[idx], f"__LIT{idx}__", 1)

    s = re.sub(r"\b\d+\.", "", s)
    for col in _SALES_COLUMNS:
        s = re.sub(rf"\b{col}\b", f"row['{col}']", s)

    #single "=" becomes "==", but leaves <=, >=, !=, == alone.
    s = re.sub(r"(?<![<>!=])=(?!=)", "==", s)

    for idx in range(len(literals)):
        s = s.replace(f"__LIT{idx}__", literals[idx])

    return s


#builds Scan 0: discovers groups and applies any gv==0 aggregates.
def build_discovery_scan(spec):
    key_expr = build_group_key_expression(spec.V)
    init_expr = build_initial_entry_dict(spec.V, spec.F)

    #no cur.execute here since scan 0 reuses the cursor that tmp already executed.
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


#builds Scan i (i in 1..n): re-executes SELECT, filters by sigma_i, applies gv==i updates.
def build_grouping_variable_scan(i, spec):
    pred = translate_sigma_to_python(spec.sigma[i - 1])
    key_expr = build_group_key_expression(spec.V)

    lines = []
    lines.append('cur.execute("SELECT * FROM sales")')
    lines.append("for row in cur:")
    lines.append(f"    if {pred}:")
    lines.append(f"        key = {key_expr}")
    lines.append("        entry = mf_struct[key]")

    for agg in spec.F:
        if agg.gv == i:
            update_block = build_aggregate_update_line(agg)
            for update_line in update_block.split("\n"):
                lines.append(f"        {update_line}")

    return "\n".join(lines)


#translates the HAVING expression G into a python boolean over `entry`.
def translate_having_to_python(G, spec):
    s = G.strip()

    #store string literals as __LIT<n>__ so the rewrites below don't touch them.
    literals = re.findall(r"'[^']*'", s)
    for idx in range(len(literals)):
        s = s.replace(literals[idx], f"__LIT{idx}__", 1)

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
    s = re.sub(r"\bOR\b", "or", s, flags=re.IGNORECASE)
    s = re.sub(r"\bNOT\b", "not", s, flags=re.IGNORECASE)

    for idx in range(len(literals)):
        s = s.replace(f"__LIT{idx}__", literals[idx])

    return s


#builds the finalize stage: sort by V, divide avgs, apply HAVING, project to S.
def build_finalize_stage(spec):
    if len(spec.V) == 1:
        sort_key = f"e['{spec.V[0]}']"
    else:
        sort_parts = []
        for v in spec.V:
            sort_parts.append(f"e['{v}']")
        sort_key = "(" + ", ".join(sort_parts) + ")"

    #avg = _sum / _count, with None when count is 0 (avoids div-by-zero).
    avg_lines = []
    for agg in spec.F:
        if agg.func == "avg":
            line = (f"entry['{agg.key}'] = "
                    f"(entry['_sum_{agg.key}'] / entry['_count_{agg.key}']) "
                    f"if entry['_count_{agg.key}'] else None")
            avg_lines.append(line)

    if spec.G:
        having_expr = translate_having_to_python(spec.G, spec)
    else:
        having_expr = "True"

    proj_parts = []
    for col in spec.S:
        proj_parts.append(f"'{col}': entry['{col}']")
    proj_dict = "{" + ", ".join(proj_parts) + "}"

    lines = []
    lines.append(f"for entry in sorted(mf_struct.values(), key=lambda e: {sort_key}):")
    for line in avg_lines:
        lines.append(f"    {line}")
    lines.append(f"    if {having_expr}:")
    lines.append(f"        _global.append({proj_dict})")

    return "\n".join(lines)


#builds the full body string spliced into tmp's {body} placeholder.
def build_query_body(spec):
    parts = []
    parts.append("mf_struct = {}")
    parts.append(build_discovery_scan(spec))

    for i in range(1, spec.n + 1):
        parts.append(build_grouping_variable_scan(i, spec))

    parts.append(build_finalize_stage(spec))

    raw = "\n\n".join(parts)
    indented = textwrap.indent(raw, "    ")

    #strip line 1's leading 4 spaces since tmp's "    {body}" already provides them.
    if indented.startswith("    "):
        return indented[4:]
    return indented


def main(path="inputs/three_states.txt"):
    spec = parse_phi_spec_file(path)
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
        main()
