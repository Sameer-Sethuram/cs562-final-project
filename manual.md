# CS562 Project Manual — Aditya Kumaran and Sameer Sethuram

## Overview

Welcome to the implementation of our project for CS562's final project! This codebase works to extend the current implementation of SQL by adding the Phi operator to the traditional SQL relational algebra.

## Purpose

The group-by clause in SQL is notoriously inefficient to implement, and often requires creating queries that are NOT succinct and are difficult to manage. Not only that, but the group-by clause is generally computationally inefficient, as it requires multiple passes and complex logic over the same grouping variables in order to produce the query that we want.

The Phi operator is a proposed solution to the issues that the group-by clause has, and in this codebase we implement a generator that ingests a file using relative paths or ingests the Phi attribute inputs using an interactive command line input mode. From there, it generates Python code to operate on the relation that we have in order to produce a table that follows the specifications of our Phi operator.

## Setup

Before running anything, the project needs a `.env` file in the same folder as `generator.py`. It should contain three lines:

```
USER=<your postgres username>
PASSWORD=<your postgres password>
DBNAME=<your database name>
```

We did not include our own `.env` in the submission since it holds personal credentials, so you will need to drop one in for the connection to work.

The database itself must already have a `sales` table with the following columns: `cust`, `prod`, `day`, `month`, `year`, `state`, `quant`, `date`. All of our ESQL and SQL queries operate on this table, so without it both the generated code and the SQL comparison files will error out before producing any output.

## Instructions

1. Once inside the project folder, install the required libraries for this project using the following command:

   ```
   pip install -r requirements.txt
   ```

   It is recommended that you activate a virtual environment for this project for reproducibility purposes, as that is the way that our project is currently working.

2. After the required libraries are installed, either you can create your own ESQL query by creating a new text document and following the format in existing folders (such as `inputs` or `demo_queries`), or you can simply use the ones that we have.

   Currently, we have a few ESQL queries within the `inputs` folder that are essentially just our own test files just to push the boundaries of our generator. If you would like an ESQL query that has a corresponding SQL query that goes along with it, take a look at the queries in the `demo_queries` folder. There, you will find 5 demo queries that we will be using for the presentation of our project, and within each query subfolder in `demo_queries`, you will find an SQL file (containing the SQL query) and a `.txt` file (containing the ESQL query).

3. Run the generator using:

   ```
   python generator.py <optional relative filepath>
   ```

   (this is for our virtual environment implementation). You may provide a relative file path to an ESQL query in order to get its results, or you can simply run the command without arguments to access the interactive command line (which follows the same structure as those in the files).

    If you would like to run the corresponding SQL queries in order to compare the results to the ESQL queries, we can simply do the following command:

    ```
    python sql.py <relative filepath>
    ```

    If the command is not prompted with a filepath, it will default to the SQL query that is equivalent to three_states.txt.

## Demo Queries

Inside the `demo_queries` folder we have 5 queries that we will be using for the presentation, each in its own subfolder. The list below briefly describes what each query is meant to demonstrate:

1. **Query 1** — Two grouping variables side by side: NY sums and NJ sums per customer. Demonstrates the basic multi-grouping-variable case where each grouping variable's scan updates a different column of the same `mf_struct` entry.

2. **Query 2** — Single grouping variable with multiple aggregates and HAVING: customers whose NY total exceeds 1,000, with both a NY sum and a NY count tracked per customer. Demonstrates Scan 0 doing bucket discovery, Scan 1 updating two aggregates of the same bucket in one σ-filtered pass, and HAVING filtering on a grouping-variable aggregate in the finalize stage.

3. **Query 3** — HAVING comparing two grouping-variable aggregates: customers whose NY total exceeds their NJ total. Demonstrates a HAVING predicate that compares two `entry['...']` fields against each other rather than against a constant.

4. **Query 4** — EMF dependent aggregate: NJ count only for customers who also bought in NY. Demonstrates the "extended" in EMF, where one grouping variable's σ predicate references an aggregate computed by an earlier grouping variable's scan.

5. **Query 5** — This vs others (self-referential predicate): each customer's total compared against the sum of all other customers' totals. Demonstrates the canonical EMF pattern where σ references the grouping attribute itself, switching the scan into a nested for-row, for-entry loop.