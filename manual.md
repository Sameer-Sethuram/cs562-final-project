# CS562 Project Manual — Aditya Kumaran and Sameer Sethuram

## Overview

Welcome to the implementation of our project for CS562's final project! This codebase works to extend the current implementation of SQL by adding the Phi operator to the traditional SQL relational algebra.

## Purpose

The group-by clause in SQL is notoriously inefficient to implement, and often requires creating queries that are NOT succinct and are difficult to manage. Not only that, but the group-by clause is generally computationally inefficient, as it requires multiple passes and complex logic over the same grouping variables in order to produce the query that we want.

The Phi operator is a proposed solution to the issues that the group-by clause has, and in this codebase we implement a generator that ingests a file using relative paths or ingests the Phi attribute inputs using an interactive command line input mode. From there, it generates Python code to operate on the relation that we have in order to produce a table that follows the specifications of our Phi operator.

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