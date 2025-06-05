import sqlite3
import shutil
import os
import re

"""
Files:
- removals.sql: Contains SQL statements to remove selected rows from the database.
- insertions.xlsx: Contains data to be inserted into the database. Each sheet corresponds to a table in the database. If a row exists in the sqlite and insertions.xlsx, overwrite value with that from insertions.xlsx 
"""

def edit_sqlite_db(input_db_path, output_db_path, table, replace_rows=None, add_rows=None):
    """
    Loads an SQLite database, replaces and adds rows in a given table, and saves as a new file.

    Args:
        input_db_path (str): Path to the input SQLite database.
        output_db_path (str): Path to save the modified SQLite database.
        table (str): Table name to modify.
        replace_rows (list of dict): Rows to replace (must include primary key).
        add_rows (list of dict): Rows to add.
    """
    # Copy the original database to the new file
    shutil.copy(input_db_path, output_db_path)

    conn = None
    try:
        # Connect to the new database
        conn = sqlite3.connect(output_db_path)
        cursor = conn.cursor()

        # Replace rows
        if replace_rows:
            for row in replace_rows:
                columns = ', '.join(row.keys())
                placeholders = ', '.join(['?'] * len(row))
                update_stmt = f"REPLACE INTO {table} ({columns}) VALUES ({placeholders})"
                cursor.execute(update_stmt, tuple(row.values()))

        # Add new rows
        if add_rows:
            for row in add_rows:
                columns = ', '.join(row.keys())
                placeholders = ', '.join(['?'] * len(row))
                insert_stmt = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_stmt, tuple(row.values()))

        conn.commit()
    finally:
        if conn:
            conn.close()

def copy_and_rename_tech_rows(db_path, output_db_path, tech_column='tech'):
    """
    For every table in the database, copies all rows where the 'tech' column starts with 'T_HDV_T',
    renames the 'tech' value to start with 'T_HDV_T_LH', and inserts the new rows into the same table in a new database file.
    """
    shutil.copy(db_path, output_db_path)
    conn = None
    try:
        conn = sqlite3.connect(output_db_path)
        cursor = conn.cursor()
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        for table_name in tables:
            # Check if the table has the tech column
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()
            columns = [info[1] for info in columns_info]
            if tech_column not in columns:
                continue
            columns_str = ', '.join(columns)
            # Select rows to copy
            cursor.execute(f"SELECT * FROM {table_name} WHERE {tech_column} LIKE 'T_HDV_T%'")
            rows = cursor.fetchall()
            tech_idx = columns.index(tech_column)
            for row in rows:
                row = list(row)
                # Only rename if not already T_HDV_T_LH
                if not str(row[tech_idx]).startswith('T_HDV_T_LH'):
                    row[tech_idx] = re.sub(r'^T_HDV_T', 'T_HDV_T_LH', str(row[tech_idx]))
                    placeholders = ', '.join(['?'] * len(row))
                    cursor.execute(f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})", row)
        conn.commit()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Example usage
    input_db = "input.db"
    output_db = "output.db"
    table_name = "my_table"

    # Example rows (adjust keys/values to match your table schema)
    rows_to_replace = [
        {"id": 1, "name": "Alice", "age": 30},
    ]
    rows_to_add = [
        {"id": 3, "name": "Charlie", "age": 25},
    ]

    edit_sqlite_db(input_db, output_db, table_name, replace_rows=rows_to_replace, add_rows=rows_to_add)

    # Example usage for copying and renaming tech rows across all tables
    copy_and_rename_tech_rows(
        db_path="canoe_on_12d_baseline.sqlite",
        output_db_path="canoe_on_12d_baseline_with_LH.sqlite",
        tech_column="tech"  # Replace with actual tech column name if different
    )
