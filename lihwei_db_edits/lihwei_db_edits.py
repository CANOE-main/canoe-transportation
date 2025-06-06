import sqlite3
import shutil
import os
import re
import pandas as pd

from utils import remove_empty_tables

"""
Files:
- removals.sql: Contains SQL statements to remove selected rows from the database.
- insertions.xlsx: Contains data to be inserted into the database. Each sheet corresponds to a table in the database. If a row exists in the sqlite and insertions.xlsx, overwrite value with that from insertions.xlsx 
"""

def read_melt_excel(xls, sheet_name, header=0, melt_int_col=None, value_name=None):
    """
    xls: pd.ExcelFile object
    header: list of rows to read as the headers. Default is first row (header=0)
    melt_int_col: column in 1st row to melt, values in 2nd row should be integers
    value_name: what to name the column of values after melting to long format?
    """
    df = pd.read_excel(xls, sheet_name=sheet_name, header=header)

    if type(header) is list and len(header) > 1:
        cols = []
        for col in df.columns:
            if (col[0] == melt_int_col and type(col[1]) is int):
                cols += [col[0]+'_'+str(col[1])]
            else:
                cols += [str(col[1])]    # get value in 2nd row, convert to str
        df.columns = cols

        id_vars = [col for col in df.columns if not col.startswith(melt_int_col+'_')]
        value_vars = [col for col in df.columns if col.startswith(melt_int_col+'_')]
        df = df.melt(id_vars=id_vars, value_vars=value_vars,
                            var_name=melt_int_col, value_name=value_name)

        # Optionally, clean up the 'vintage' column to just have the year
        df[melt_int_col] = df[melt_int_col].str.replace(melt_int_col+'_', '').astype(int)
        # drop rows where value is nan
        df = df.dropna(subset=value_name)
    return df


def xlsx_to_sqlite(xlsx_path, sqlite_path, schema_ref_db: str,
                   read_melt_excel_config: dict = None
                   ):
    """
    Converts an Excel file with multiple sheets into an SQLite database.
    Each sheet in the xlsx file corresponds to a table in the sqlite database.
    If a table already exists, it will be replaced with the new data from the sheet.
    The schema_ref_db argument is the path to an SQLite file to extract the schema from.

    Args:
        xlsx_path (str): Path to the input Excel file.
        sqlite_path (str): Path to save the output SQLite database.
        schema_sql_file: the sql file defining the schema of the database, which includes table names and their respective columns.
    """
    if read_melt_excel_config is None:
        # configuration to use in read_melt_excel for specific tables
        read_melt_excel_config = {
            'Efficiency': {'header': [0,1], 'melt_int_col': 'vintage', 'value_name': 'efficiency'},
            'Demand': {'header': [0,1], 'melt_int_col': 'period', 'value_name': 'demand'}}

    # Copy the schema reference file
    shutil.copy(schema_ref_db, sqlite_path)

    # Connect to the copied database
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

        # Get all table names (excluding SQLite internal tables)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]

        # Delete all data from each table
        for table in tables:
            cursor.execute(f'DELETE FROM "{table}"')

        conn.commit()

        # Extract schema
        schema = {}
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        for table in tables:
            cursor.execute(f"PRAGMA table_info('{table}')")
            columns = [col[1] for col in cursor.fetchall()]
            schema[table] = columns
        
        # Read each sheet in the Excel file and write it to the SQLite database
        # Only read sheets that match the schema
        xls = pd.ExcelFile(xlsx_path)
        for sheet_name in xls.sheet_names:
            if sheet_name in schema:
                if sheet_name in read_melt_excel_config:
                    df = read_melt_excel(xls, sheet_name=sheet_name, **read_melt_excel_config[sheet_name])
                else:
                    df = read_melt_excel(xls, sheet_name=sheet_name)

                # select columns based on the columns defined for the table in the schema, ignore any extra columns
                df = df[[col for col in schema[sheet_name] if col in df.columns]]

                # insert the df into the sqlite database, adding to existing table if it exists
                # if the table does not exist, it will be created
                df.to_sql(sheet_name, conn, if_exists='append', index=False)
                # print message indicating columns dropped
                dropped_columns = set(df.columns) - set(schema[sheet_name])
                if dropped_columns:
                    print(f"Dropped columns {dropped_columns} from sheet '{sheet_name}' as they are not defined in the schema.")

            else:
                print(f"Sheet '{sheet_name}' does not match any defined schema and will be skipped.")

    finally:
        remove_empty_tables(cursor=cursor)
        conn.close()


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
