#CS562 Final Project — Query Processing Engine (QPE) generator.
#parses a phi-operator spec file and emits a Python program that evaluates
#MF/EMF OLAP queries over the PostgreSQL `sales` table without relying on
#the DBMS for any aggregation.

import re
import subprocess
import sys
import textwrap
from dataclasses import dataclass, field


#columns of the sales table — used by emit_predicate to decide which bare
#identifiers in a sigma predicate are column references (-> row['col']) vs.
#Python keywords/literals (passed through unchanged).
_SALES_COLUMNS = ("cust", "prod", "day", "month", "year", "state", "quant", "date")


@dataclass
#data class representing an aggregate
class Aggregate:
    """One aggregate token from F-VECT, e.g. '1_sum_quant' or 'sum_quant'."""
    gv: int  #whether it's the first, second, third, etc. grouping attribute
    func: str          #the function, like sum, count, avg, min, or max
    attr: str          #the attribute being called in the aggregate
    key: str           # original token; used as the dict key in generated code


@dataclass
#dataclass with all phi variables assigned
class PhiSpec:
    S: list = field(default_factory=list)   # elements of the select statement, in a list
    n: int = 0                              # num of grouping vars
    V: list = field(default_factory=list)   # grouping attributes
    F: list = field(default_factory=list)   # aggregate functions
    sigma: list = field(default_factory=list)   # conditionals (such that clause)
    G: str = None                           # elements in the having clause


#turns aggregates like '1_sum_quant' or 'sum_quant' into an Aggregate dataclass instance.
def _parse_agg(token):
    t = token.strip()
    parts = t.split("_")
    if parts[0].isdigit():
        return Aggregate(gv=int(parts[0]), func=parts[1],
                         attr="_".join(parts[2:]), key=t)
    return Aggregate(gv=0, func=parts[0],
                     attr="_".join(parts[1:]), key=t)


#reads input from a file (formatted like in the document), and parses the values into a PhiSpec dataclass.
'''
    SELECT ATTRIBUTE(S):
    cust, 1_sum_quant, 2_sum_quant, 3_sum_quant
    NUMBER OF GROUPING VARIABLES(n):
    3
    GROUPING ATTRIBUTES(V):
    cust
    F-VECT([F]):
    1_sum_quant, 2_sum_quant, 3_sum_quant
    SELECT CONDITION-VECT([σ]):
    1.state='NY'; 2.state='NJ'; 3.state='CT'
    HAVING_CONDITION(G):
    (blank — G has no value)
'''
def read_input(path):
    lines = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped != "":
                lines.append(stripped)

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

    #this is to make sure that values array has at least 6 values
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
            parsed = _parse_agg(t)
            F.append(parsed)

    sigma = []
    for t in sigma_val.split(";"):
        t = t.strip()
        if t != "":
            sigma.append(t)

    if g_val != "":
        G = g_val
    else:
        G = None

    return PhiSpec(
        S=S,
        n=n,
        V=V,
        F=F,
        sigma=sigma,
        G=G,
    )


#builds the Python expression for the mf_struct key from `row`.
#single-attr V like ['cust']        -> "row['cust']"
#multi-attr V like ['cust','prod']  -> "(row['cust'], row['prod'])"
def emit_key_expr(V):
    if len(V) == 1:
        return f"row['{V[0]}']"

    parts = []
    for v in V:
        parts.append(f"row['{v}']")
    return "(" + ", ".join(parts) + ")"


#builds the dict-literal source for a newly-discovered group's initial row.
#sums and counts initialize to 0; min and max init to None;
#avg gets a None for the visible slot plus _sum_X and _count_X helpers.
def emit_init_entry(V, F):
    parts = []
    for v in V:
        parts.append(f"'{v}': row['{v}']")

    for agg in F:
        if agg.func == "sum" or agg.func == "count":
            parts.append(f"'{agg.key}': 0")
        elif agg.func == "min" or agg.func == "max":
            parts.append(f"'{agg.key}': None")
        elif agg.func == "avg":
            parts.append(f"'{agg.key}': None")
            parts.append(f"'_sum_{agg.key}': 0")
            parts.append(f"'_count_{agg.key}': 0")
        else:
            raise ValueError("unsupported aggregate func: " + agg.func)

    return "{" + ", ".join(parts) + "}"


#builds one source line that updates a single aggregate in `entry` from `row`.
#avg returns two lines (one for _sum_X, one for _count_X).
def emit_update(agg):
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


#translates one sigma predicate into a Python boolean expression that can be
#evaluated against `row`. for example:
#   "1.state='NY'"  ->  "row['state'] == 'NY'"
def emit_predicate(sigma_i):
    s = sigma_i.strip()

    #find every quoted string literal first and replace each one with __LIT0__,
    #__LIT1__, etc. so that the rewrites below don't touch their contents.
    literals = re.findall(r"'[^']*'", s)
    for idx in range(len(literals)):
        s = s.replace(literals[idx], f"__LIT{idx}__", 1)

    #strip the "1.", "2." grouping-variable prefixes from column references
    s = re.sub(r"\b\d+\.", "", s)

    #wrap each known sales column name in row['col']
    for col in _SALES_COLUMNS:
        s = re.sub(rf"\b{col}\b", f"row['{col}']", s)

    #single "=" becomes Python's "==" — but leave <=, >=, !=, and == alone
    s = re.sub(r"(?<![<>!=])=(?!=)", "==", s)

    #put the string literals back where they were
    for idx in range(len(literals)):
        s = s.replace(f"__LIT{idx}__", literals[idx])

    return s


#builds Scan 0 — discovers all distinct groups and applies any aggregates
#with gv==0. piggybacks on the cursor that the tmp template already executed,
#so this scan has no cur.execute of its own.
def emit_scan_zero(spec):
    key_expr = emit_key_expr(spec.V)
    init_expr = emit_init_entry(spec.V, spec.F)

    lines = []
    lines.append("for row in cur:")
    lines.append(f"    key = {key_expr}")
    lines.append("    if key not in mf_struct:")
    lines.append(f"        mf_struct[key] = {init_expr}")
    lines.append("    entry = mf_struct[key]")

    #append update lines for each gv==0 aggregate
    for agg in spec.F:
        if agg.gv == 0:
            update_block = emit_update(agg)
            for update_line in update_block.split("\n"):
                lines.append(f"    {update_line}")

    return "\n".join(lines)


#builds Scan i for i in 1..n — re-executes SELECT * FROM sales, filters rows
#by sigma_i, then applies updates for every aggregate whose gv == i. each per-GV
#scan owns its own cur.execute (Option B from the paper) — matches the original
#paper structure and also generalizes to datasets too large to materialize.
def emit_scan_i(i, spec):
    pred = emit_predicate(spec.sigma[i - 1])
    key_expr = emit_key_expr(spec.V)

    lines = []
    lines.append('cur.execute("SELECT * FROM sales")')
    lines.append("for row in cur:")
    lines.append(f"    if {pred}:")
    lines.append(f"        key = {key_expr}")
    lines.append("        entry = mf_struct[key]")

    #append update lines for each aggregate that belongs to this grouping variable
    for agg in spec.F:
        if agg.gv == i:
            update_block = emit_update(agg)
            for update_line in update_block.split("\n"):
                lines.append(f"        {update_line}")

    return "\n".join(lines)


#translates the HAVING expression G into a Python boolean over `entry`.
#wraps aggregate-shaped tokens (1_sum_quant, avg_quant, etc.) and V grouping
#attributes in entry['...']. translates SQL "=" to Python "==" and the words
#AND/OR/NOT (any case) to Python and/or/not. string literals are protected
#during the rewrites so their contents (e.g. 'state') aren't mangled.
def emit_having(G, spec):
    s = G.strip()

    #stash string literals first so their contents aren't touched by the rewrites
    literals = re.findall(r"'[^']*'", s)
    for idx in range(len(literals)):
        s = s.replace(literals[idx], f"__LIT{idx}__", 1)

    #wrap aggregate-shaped tokens like 1_sum_quant or avg_quant in entry[...]
    s = re.sub(
        r"(?<!')\b((?:\d+_)?(?:sum|count|avg|min|max)_\w+)\b(?!')",
        r"entry['\1']",
        s,
    )

    #wrap V grouping attribute references that aren't already wrapped
    for v in spec.V:
        s = re.sub(rf"(?<!')\b{re.escape(v)}\b(?!')", f"entry['{v}']", s)

    #SQL single "=" becomes Python "=="
    s = re.sub(r"(?<![<>!=])=(?!=)", "==", s)

    #SQL AND/OR/NOT (any case) becomes Python and/or/not
    s = re.sub(r"\bAND\b", "and", s, flags=re.IGNORECASE)
    s = re.sub(r"\bOR\b", "or", s, flags=re.IGNORECASE)
    s = re.sub(r"\bNOT\b", "not", s, flags=re.IGNORECASE)

    #put the string literals back
    for idx in range(len(literals)):
        s = s.replace(f"__LIT{idx}__", literals[idx])

    return s


#builds the finalize stage of the body — sort by V, divide avg slots,
#apply HAVING, project to S columns, append surviving rows to _global.
def emit_finalize_full(spec):
    #sort key is e['cust'] for single-attr V, or (e['a'], e['b']) for multi-attr
    if len(spec.V) == 1:
        sort_key = f"e['{spec.V[0]}']"
    else:
        sort_parts = []
        for v in spec.V:
            sort_parts.append(f"e['{v}']")
        sort_key = "(" + ", ".join(sort_parts) + ")"

    #for each avg aggregate, divide _sum_X by _count_X (None when count is 0)
    avg_lines = []
    for agg in spec.F:
        if agg.func == "avg":
            line = (f"entry['{agg.key}'] = "
                    f"(entry['_sum_{agg.key}'] / entry['_count_{agg.key}']) "
                    f"if entry['_count_{agg.key}'] else None")
            avg_lines.append(line)

    #HAVING expression — when no G is given, "True" lets every entry through
    if spec.G:
        having_expr = emit_having(spec.G, spec)
    else:
        having_expr = "True"

    #project dict — pull only the S columns from each entry into _global
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


#builds the full body string that gets spliced into tmp's {body} placeholder.
#starts with mf_struct init, then Scan 0, then one block per grouping variable,
#then the finalize stage. indents every line to 4 spaces (function-scope), then
#strips the 4 leading spaces from line 1 because tmp's "    {body}" already
#provides that indent on the first line.
def emit_body(spec):
    parts = []
    parts.append("mf_struct = {}")
    parts.append(emit_scan_zero(spec))

    #one scan block per grouping variable (skipped entirely when n == 0)
    for i in range(1, spec.n + 1):
        parts.append(emit_scan_i(i, spec))

    parts.append(emit_finalize_full(spec))

    raw = "\n\n".join(parts)
    indented = textwrap.indent(raw, "    ")

    #strip the 4 leading spaces from line 1 because tmp already provides them
    if indented.startswith("    "):
        return indented[4:]
    return indented


#takes a phi-spec file, builds the body, splices it into tmp, writes
#_generated.py, and runs it. path defaults to inputs/three_states.txt when
#invoked as `python generator.py`. callers that import this module (e.g.
#test_generator.py) should pass `path` explicitly so we don't accidentally
#read whatever happens to be in argv.
def main(path="inputs/three_states.txt"):
    spec = read_input(path)
    body = emit_body(spec)

    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
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

    # Write the generated code to a file
    open("_generated.py", "w").write(tmp)
    # Execute the generated code
    subprocess.run(["python", "_generated.py"])


if "__main__" == __name__:
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()
