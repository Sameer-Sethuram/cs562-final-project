"""
CS562 Final Project — Query Processing Engine (QPE) generator.

Parses a Φ-operator spec file and (eventually) emits a Python program that
evaluates MF/EMF OLAP queries over the PostgreSQL `sales` table without
relying on the DBMS for any aggregation.
"""
from __future__ import annotations

import re
import subprocess
import sys
import textwrap
from dataclasses import dataclass, field
from pathlib import Path


# Columns of the `sales` table. Used by emit_predicate to decide which bare
# identifiers in a σ predicate are column references (→ row['col']) vs. Python
# keywords/literals (passed through unchanged).
_SALES_COLUMNS = ("cust", "prod", "day", "month", "year", "state", "quant", "date")


# ---------------------------------------------------------------------------
# Parsed Φ-operator spec
# ---------------------------------------------------------------------------

@dataclass
class Aggregate:
    """One aggregate token from F-VECT, e.g. '1_sum_quant' or 'sum_quant'."""
    gv: int            # 0 for bare aggregates, 1..n for grouping-variable-subscripted
    func: str          # 'sum' | 'count' | 'avg' | 'min' | 'max'
    attr: str          # attribute being aggregated, e.g. 'quant'
    key: str           # original token; used as the dict key in generated code


@dataclass
class PhiSpec:
    """A parsed Φ-operator spec: projection, grouping, aggregates, predicates."""
    S:     list[str]        = field(default_factory=list)   # projected attributes in order
    n:     int              = 0                             # number of grouping variables
    V:     list[str]        = field(default_factory=list)   # grouping attributes
    F:     list[Aggregate]  = field(default_factory=list)   # aggregate functions
    sigma: list[str]        = field(default_factory=list)   # raw predicate per GV, len == n
    G:     str | None       = None                          # having-clause predicate, if any


# ---------------------------------------------------------------------------
# Input parsing
# ---------------------------------------------------------------------------

def _parse_agg(token: str) -> Aggregate:
    """Parse '1_sum_quant' or 'sum_quant' into an Aggregate (splits on '_')."""
    t = token.strip()
    parts = t.split("_")
    if parts[0].isdigit():
        return Aggregate(gv=int(parts[0]), func=parts[1],
                         attr="_".join(parts[2:]), key=t)
    return Aggregate(gv=0, func=parts[0],
                     attr="_".join(parts[1:]), key=t)


def read_input(path: str | Path) -> PhiSpec:
    """
    Parse a Φ-spec file. The file has a fixed sequence of six heading/value
    pairs — one heading per line (any line ending with a colon) followed by
    its value on the very next line:

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
    """
    lines = [l.strip() for l in Path(path).read_text(encoding="utf-8").splitlines()
             if l.strip()]

    # Walk pairs: a heading contains a colon; the value is the next line,
    # unless that next line is itself a heading (then the value is empty).
    values: list[str] = []
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

    # Pad so unpacking survives a missing trailing HAVING value.
    values += [""] * (6 - len(values))
    s_val, n_val, v_val, f_val, sigma_val, g_val = values[:6]

    return PhiSpec(
        S=[t.strip() for t in s_val.split(",") if t.strip()],
        n=int(n_val) if n_val else 0,
        V=[t.strip() for t in v_val.split(",") if t.strip()],
        F=[_parse_agg(t) for t in f_val.split(",") if t.strip()],
        sigma=[t.strip() for t in sigma_val.split(";") if t.strip()],
        G=g_val or None,
    )


# ---------------------------------------------------------------------------
# Code generation — each helper returns a fragment of Python source
# ---------------------------------------------------------------------------

def emit_key_expr(V: list[str]) -> str:
    """Python expression that computes the mf_struct key from `row`."""
    if len(V) == 1:
        return f"row['{V[0]}']"
    attrs = ", ".join(f"row['{v}']" for v in V)
    return f"({attrs})"


def emit_init_entry(V: list[str], F: list[Aggregate]) -> str:
    """Dict-literal text for a newly-discovered group's initial row."""
    parts: list[str] = [f"'{v}': row['{v}']" for v in V]
    for agg in F:
        if agg.func in ("sum", "count"):
            parts.append(f"'{agg.key}': 0")
        elif agg.func in ("min", "max"):
            parts.append(f"'{agg.key}': None")
        elif agg.func == "avg":
            parts.append(f"'{agg.key}': None")
            parts.append(f"'_sum_{agg.key}': 0")
            parts.append(f"'_count_{agg.key}': 0")
        else:
            raise ValueError(f"unsupported aggregate func: {agg.func}")
    return "{" + ", ".join(parts) + "}"


def emit_update(agg: Aggregate) -> str:
    """One source line (two for avg) that updates a single aggregate in `entry`."""
    if agg.func == "sum":
        return f"entry['{agg.key}'] += row['{agg.attr}']"
    if agg.func == "count":
        return f"entry['{agg.key}'] += 1"
    if agg.func == "min":
        return (f"entry['{agg.key}'] = row['{agg.attr}'] "
                f"if entry['{agg.key}'] is None "
                f"else min(entry['{agg.key}'], row['{agg.attr}'])")
    if agg.func == "max":
        return (f"entry['{agg.key}'] = row['{agg.attr}'] "
                f"if entry['{agg.key}'] is None "
                f"else max(entry['{agg.key}'], row['{agg.attr}'])")
    if agg.func == "avg":
        return (f"entry['_sum_{agg.key}'] += row['{agg.attr}']\n"
                f"entry['_count_{agg.key}'] += 1")
    raise ValueError(f"unsupported aggregate func: {agg.func}")


def emit_predicate(sigma_i: str) -> str:
    """Translate one σ predicate into a Python boolean expression.

    Example: "1.state='NY'" → "row['state'] == 'NY'". Steps: protect quoted
    string literals, strip the grouping-variable prefix, wrap known sales
    columns in row['...'], swap the single-`=` SQL operator for Python `==`,
    then restore the literals.
    """
    s = sigma_i.strip()

    literals: list[str] = []

    def _stash(m: "re.Match[str]") -> str:
        literals.append(m.group(0))
        return f"__LIT{len(literals) - 1}__"

    s = re.sub(r"'[^']*'", _stash, s)
    s = re.sub(r"\b\d+\.", "", s)
    for col in _SALES_COLUMNS:
        s = re.sub(rf"\b{col}\b", f"row['{col}']", s)
    s = re.sub(r"(?<![<>!=])=(?!=)", "==", s)
    for idx, lit in enumerate(literals):
        s = s.replace(f"__LIT{idx}__", lit)
    return s


def emit_scan_zero(spec: PhiSpec) -> str:
    """Scan 0: discover distinct groups + apply any gv==0 aggregate updates.

    Uses the cursor already executed by the tmp template — no cur.execute here.
    """
    key_expr = emit_key_expr(spec.V)
    init_expr = emit_init_entry(spec.V, spec.F)
    lines = [
        "for row in cur:",
        f"    key = {key_expr}",
        "    if key not in mf_struct:",
        f"        mf_struct[key] = {init_expr}",
        "    entry = mf_struct[key]",
    ]
    for agg in (a for a in spec.F if a.gv == 0):
        for update_line in emit_update(agg).split("\n"):
            lines.append(f"    {update_line}")
    return "\n".join(lines)


def emit_scan_i(i: int, spec: PhiSpec) -> str:
    """Scan i (i in 1..n): re-execute SELECT * FROM sales, filter by σᵢ,
    apply updates for every aggregate with gv == i.

    Each per-GV scan gets its own cur.execute — Option B, matches the paper's
    structure and generalizes to datasets too large to materialize.
    """
    pred = emit_predicate(spec.sigma[i - 1])
    key_expr = emit_key_expr(spec.V)
    lines = [
        'cur.execute("SELECT * FROM sales")',
        "for row in cur:",
        f"    if {pred}:",
        f"        key = {key_expr}",
        "        entry = mf_struct[key]",
    ]
    for agg in (a for a in spec.F if a.gv == i):
        for update_line in emit_update(agg).split("\n"):
            lines.append(f"        {update_line}")
    return "\n".join(lines)


def emit_finalize_simple(spec: PhiSpec) -> str:
    """Copy every mf_struct entry into _global, sorted by V.

    Sorting by the grouping attributes mirrors `ORDER BY <V>` in SQL, which
    makes the output deterministic across runs and comparable via string
    equality against sql.py's reference output (see test_generator.py).
    Next slice replaces this with an emit_finalize_full(spec) that also
    applies the HAVING filter and projects only the S columns.
    """
    if len(spec.V) == 1:
        key_expr = f"e['{spec.V[0]}']"
    else:
        key_expr = "(" + ", ".join(f"e['{v}']" for v in spec.V) + ")"
    return (f"for entry in sorted(mf_struct.values(), key=lambda e: {key_expr}):\n"
            f"    _global.append(entry)")


def emit_body(spec: PhiSpec) -> str:
    """Full body text ready to splice into tmp's {body} placeholder.

    Indents every line 4 spaces (function-scope), then drops the 4 leading
    spaces from line 1 since tmp's own "    {body}" already provides them.
    """
    parts = [
        "mf_struct = {}",
        emit_scan_zero(spec),
        *[emit_scan_i(i, spec) for i in range(1, spec.n + 1)],
        emit_finalize_simple(spec),
    ]
    raw = "\n\n".join(parts)
    indented = textwrap.indent(raw, "    ")
    return indented[4:] if indented.startswith("    ") else indented


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a
    file (e.g. _generated.py) and then run.

    Usage: python generator.py [inputs/<spec>.txt]
    Defaults to inputs/three_states.txt if no path is given.
    """
    path = sys.argv[1] if len(sys.argv) > 1 else "inputs/three_states.txt"
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
    main()
