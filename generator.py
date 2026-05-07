import re
import subprocess
import sys
import textwrap
from dataclasses import dataclass, field


#columns of the sales table; identifies which bare names are column refs in predicates.
SALES_COLUMNS = ("cust", "prod", "day", "month", "year", "state", "quant", "date")


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


#takes raw strings for each input (from a file scan or interactive input), and put them into a PhiSpec.
def phi_spec_from_values(s_val, n_val, v_val, f_val, sigma_val, g_val):
    '''
    We have raw string inputs for each value, each separated by a comma. So:
    We split on ',' to get each value, and append it to our array. 
    This process is roughly the same for all 6 phi operands. 
    '''
    S = []
    for token in s_val.split(","):
        token = token.strip()
        if token != "":
            S.append(token)

    if n_val != "":
        n = int(n_val)
    else:
        n = 0

    V = []
    for token in v_val.split(","):
        token = token.strip()
        if token != "":
            V.append(token)

    #This case is slightly unique, as F is a list of aggregates. Because of this, we parse it further down into an "Aggregate" data class. 
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

    sigma = []
    for token in sigma_val.split(";"):
        token = token.strip()
        if token != "":
            sigma.append(token)

    if g_val != "":
        G = g_val
    else:
        G = None

    return PhiSpec(S=S, n=n, V=V, F=F, sigma=sigma, G=G) 


#this function is for parsing a file input with ESQL.
def parse_phi_spec_file(path):
    lines = [] #array for storing the lines
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped != "":
                lines.append(stripped) #adding each line of the file to the array blindly.

    #this section is for parsing through the lines (now stored in an array)
    values = []
    i = 0
    while i < len(lines):
        if ":" in lines[i]: #since the format is given, any line with a ':' is a "header", so the values will likely be below it.
            if i + 1 < len(lines) and ":" not in lines[i + 1]: #if the line below does not have a colon, we know its the values. Append to our array.
                values.append(lines[i + 1])
                i += 2
            else: #if the line below has a colon, then the previous header did not have any values. so we append the empty string to the array
                values.append("")
                i += 1
        else: #if the line has a :, we know its a header, so we just skip
            i += 1

    while len(values) < 6: #we must ensure that all phi operands have a value, even if they are not provided. since there are 6, we must do this until the length is 6.
        values.append("")

    return phi_spec_from_values(values[0], values[1], values[2], values[3], values[4], values[5]) #pass our parsed string operands to this function in order to turn them into a real PhiSpec class. 


'''
Generates the code to run the first loop in generated.py.
1. Finds all unique combinations of attributes listed in the GROUP BY clause and puts them in the MF struct. 
2. Applies any aggregates that have nothing to do with the grouping variable. 
'''
def initial_scan(spec):
    #goal of this block of code is to build key_expr = "(row['cust'], row['prod']), .... for each V"
    if len(spec.V) == 1: #if the length of the V array is 1, then we can just set the expression to the first value.
        key_expr = f"row['{spec.V[0]}']"
    else: #if the length of the V array is more than 1, then we construct a tuple of attributes.
        key_parts = []
        for v in spec.V:
            key_parts.append(f"row['{v}']")
        key_expr = "(" + ", ".join(key_parts) + ")"

    #goal of this block of code is to build init_expr, which is the initial/blank state for a group
    init_parts = []
    for v in spec.V:
        init_parts.append(f"'{v}': row['{v}']")

    #this section just looks through the Aggregates in F, and determines what the aggregate function is. 
    #It initializes the entry in the dictionary appropriately depending on what the function is.
    for agg in spec.F:
        if agg.func == "sum" or agg.func == "count":
            init_parts.append(f"'{agg.key}': 0")
        elif agg.func == "min" or agg.func == "max":
            init_parts.append(f"'{agg.key}': None")
        elif agg.func == "avg":
            #avg needs hidden sum and count slots so the finalize stage can compute the avg.
            init_parts.append(f"'{agg.key}': None")
            init_parts.append(f"'_sum_{agg.key}': 0")
            init_parts.append(f"'_count_{agg.key}': 0")
        else:
            raise ValueError("unsupported aggregate func: " + agg.func) #edge case for if another aggregate function is implemented. 

    init_expr = "{" + ", ".join(init_parts) + "}"


    lines = []
    lines.append("for row in cur:")
    lines.append(f"    key = {key_expr}")
    lines.append("    if key not in mf_struct:")
    lines.append(f"        mf_struct[key] = {init_expr}") #the key for the initial expression would be a unique combination of the specified attributes (e.g, (Alice, Apple) for (cust, prod))
    lines.append("    entry = mf_struct[key]")

    #if the aggregate is not tied to a grouping attribute, we just write the code to compute it here.
    for agg in spec.F:
        if agg.gv == 0:
            update_block = build_aggregate_update_line(agg) #this is the function that updates the aggregate value inside of mf_struct. 
            for update_line in update_block.split("\n"):
                lines.append(f"    {update_line}")

    return "\n".join(lines)

#This function builds the code to update the aggregate value inside of mf_struct.
def build_aggregate_update_line(agg):
    if agg.func == "sum":
        return f"entry['{agg.key}'] += row['{agg.attr}']" #for sum, we must add the value at the attribute.

    if agg.func == "count":
        return f"entry['{agg.key}'] += 1" #for count, we just increment by 1.

    if agg.func == "min": #for min, if the value is undefined, we just set it to the current value. else, we set it to the minimum of what exists and the new value.
        line  = f"entry['{agg.key}'] = row['{agg.attr}'] if entry['{agg.key}'] is None else min(entry['{agg.key}'], row['{agg.attr}'])"
        return line

    if agg.func == "max": #for max, if the value is undefined, we just set it to the current value. else, we set it to the maximum of what exists and the new value.
        line = f"entry['{agg.key}'] = row['{agg.attr}'] if entry['{agg.key}'] is None else max(entry['{agg.key}'], row['{agg.attr}'])"
        return line

    if agg.func == "avg": #we need to compute a line for the sum, and the count, and then divide them!
        sum_line   = f"entry['_sum_{agg.key}'] += row['{agg.attr}']"
        count_line = f"entry['_count_{agg.key}'] += 1"
        return sum_line + "\n" + count_line

    raise ValueError("unsupported aggregate func: " + agg.func) #fallback for if there an unsupported aggregate provided. 


#translates one sigma predicate into a python boolean over `row` (current scan row), entry (the group's V values), and aggregates from earlier scans.
#CITE: Claude AI was used to help with the regex and overarching logic of this particular function. 
def translate_sigma_to_python(sigma_i, spec):
    s = sigma_i.strip()

    #store strings with temp placeholders so that they do not get overwritten.
    literals = re.findall(r"'[^']*'", s)
    for index in range(len(literals)):
        s = s.replace(literals[index], f"__LIT{index}__", 1)

    #1.cust, 2.state, 3.quant -> row['cust'], row['state'], row['quant'].
    for col in SALES_COLUMNS:
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
    for col in SALES_COLUMNS:
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
#Ensures that EMF queries are  also accomodated for, as we use a nested row-x-entry loop to compute "over all others" cases and etc.
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
        #divide the accumulated sum by the count while guarding against zero count.
        lines.append(f"    if entry['_count_{agg.key}'] != 0:")
        lines.append(f"        entry['{agg.key}'] = entry['_sum_{agg.key}'] / entry['_count_{agg.key}']")
        lines.append(f"    else:")
        lines.append(f"        entry['{agg.key}'] = None")

    return "\n".join(lines)


#translates the HAVING expression G into a python boolean over `entry`.
#CITE: Claude AI helped us with the following regexes we are using here as well as with the literal storing logic.
def translate_having_to_python(G, spec):
    s = G.strip()

    #store string literals as __LIT<n>__ so the rewrites below don't accidentally corrupt our expression.
    literals = re.findall(r"'[^']*'", s)
    for index in range(len(literals)):
        s = s.replace(literals[index], f"__LIT{index}__", 1)


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
    parts.append(initial_scan(spec)) #scan 0 code, appended to the final output

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

#this function serves to provide the user with command-line inputs for the spec interactively.
def parse_phi_spec_interactive():
    print("Enter each Phi operand below.")
    print("Use commas in S, V, and F-VECT; use semicolons between per-GV predicates in sigma.")
    print("Leave HAVING blank for queries that don't filter groups.")
    print()
    #the following grab the spec values so that we can parse it using our existing function.
    s_val     = input("SELECT ATTRIBUTE(S):\n").strip()
    n_val     = input("NUMBER OF GROUPING VARIABLES(n):\n").strip()
    v_val     = input("GROUPING ATTRIBUTES(V):\n").strip()
    f_val     = input("F-VECT([F]):\n").strip()
    sigma_val = input("SELECT CONDITION-VECT([sigma]):\n").strip()
    g_val     = input("HAVING_CONDITION(G):\n").strip()
 
    spec = phi_spec_from_values(s_val, n_val, v_val, f_val, sigma_val, g_val)
    return spec


#runs main to generate the equivalent query.
def main(path=None):
    if path is not None: #this is to process the case where the user passes a path directly.
        spec = parse_phi_spec_file(path)
    elif len(sys.argv) > 1: #this is to process the case where the user wants to input a path.
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