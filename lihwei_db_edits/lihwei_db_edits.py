import sqlite3
import shutil
import os
import re
import pandas as pd

import sys

sys.path.append('..')
from utils import remove_empty_tables

"""
Files:
- removals.sql: Contains SQL statements to remove selected rows from the database.
- insertions.xlsx: Contains data to be inserted into the database. Each sheet corresponds to a table in the database. If a row exists in the sqlite and insertions.xlsx, overwrite value with that from insertions.xlsx 
"""

def read_melt_excel(xls, sheet_name, header=0, melt_int_col=None, value_name=None, end_col_str='model input'):
    """
    xls: pd.ExcelFile object
    header: list of rows to read as the headers. Default is first row (header=0)
    melt_int_col: column in 1st row to melt, values in 2nd row should be integers
    value_name: what to name the column of values after melting to long format?
    end_col_str: string or regex to search for to signify the last column to be read
    """
    df = pd.read_excel(xls, sheet_name=sheet_name, header=header)

    if type(header) is list and len(header) > 1:
        # Concat multiindex column names with '_'
        cols_concat = pd.Series(['_'.join([str(i) for i in col if str(i) != '']) for col in df.columns])
        # Find the lowest column index that matches end_col_str
        end_col_detected = cols_concat.str.lower().str.contains(end_col_str)
        if end_col_detected.all() is False:
            print('df.columns:', df.columns)
            raise ValueError(f"No columns matching '{end_col_str}' found in sheet '{sheet_name}'.")
        else:
            df = df.iloc[:,:end_col_detected.argmax()].dropna(how='all')

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
            'Demand': {'header': [0,1], 'melt_int_col': 'period', 'value_name': 'demand'},
            'ExistingCapacity': {'header': [0,1], 'melt_int_col': 'vintage', 'value_name': 'capacity'},
            'MinAnnualCapacityFactor': {'header': [0,1], 'melt_int_col': 'period', 'value_name': 'factor'},
            'MaxAnnualCapacityFactor': {'header': [0,1], 'melt_int_col': 'period', 'value_name': 'factor'},
            'CostInvest': {'header': [0,1], 'melt_int_col': 'vintage', 'value_name': 'cost'},
            'CostFixed':  {'header': [0,1], 'melt_int_col': 'vintage', 'value_name': 'cost'}
            }

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
        with pd.ExcelFile(xlsx_path) as xls:
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

def insert_profiles_CapacityFactorTech(db_path, profiles_df, new_db_path=None):
    """
    db_path: Path to the database where the profiles will be inserted.
    profiles_df: DataFrame containing the profiles to be inserted. Already formatted with columns in CapacityFactorTech format.
    Insert the capacity factor profiles into the database. Removes existing entries for the specified technologies before inserting new ones.
    """
    if new_db_path is not None:
        print(f"Copying database from {db_path} to {new_db_path} for insertion of profiles.")
        # Copy the schema reference file
        shutil.copy(db_path, new_db_path)
        db_path = new_db_path

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Remove rows where tech is already in profiles_df
        tech_list = profiles_df.tech.unique()
        placeholders = ','.join(['?' for _ in tech_list])
        cursor.execute(f"DELETE FROM CapacityFactorTech WHERE tech IN ({placeholders})", tech_list)
        conn.commit()
        
        # Add the new profiles. Only add columns that are in CapacityFactorTech
        (profiles_df[['region', 'period', 'season', 'tod', 'tech', 'factor', 'notes']]
         .to_sql('CapacityFactorTech', con=conn, if_exists='append', index=False))
    
    finally:
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

import sqlite3
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any

def compare_sqlite_databases(db1_path: str, db2_path: str, 
                           ignore_order: bool = True, 
                           show_sample_diffs: bool = True,
                           max_sample_rows: int = 10) -> Dict[str, Any]:
    """
    Compare two SQLite databases and return detailed differences.
    
    Parameters:
    - db1_path: Path to first database
    - db2_path: Path to second database  
    - ignore_order: Whether to ignore row order when comparing
    - show_sample_diffs: Whether to show sample different rows
    - max_sample_rows: Maximum number of sample rows to show
    
    Returns:
    - Dictionary with comparison results
    """
    
    def get_table_names(conn):
        """Get all table names from database"""
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return set(row[0] for row in cursor.fetchall())
    
    def get_table_schema(conn, table_name):
        """Get table schema"""
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        return cursor.fetchall()
    
    def compare_dataframes(df1, df2, table_name, ignore_order=True):
        """Compare two dataframes and return differences"""
        comparison_result = {
            'identical': False,
            'shape_match': df1.shape == df2.shape,
            'df1_shape': df1.shape,
            'df2_shape': df2.shape,
            'column_differences': [],
            'data_differences': [],
            'sample_differences': []
        }
        
        # Check columns
        cols1, cols2 = set(df1.columns), set(df2.columns)
        if cols1 != cols2:
            comparison_result['column_differences'] = {
                'only_in_db1': list(cols1 - cols2),
                'only_in_db2': list(cols2 - cols1)
            }
            return comparison_result
        
        # If same columns, compare data
        common_cols = list(cols1.intersection(cols2))
        df1_common = df1[common_cols].copy()
        df2_common = df2[common_cols].copy()
        
        if ignore_order:
            # Sort by all columns for comparison
            try:
                df1_sorted = df1_common.sort_values(by=common_cols).reset_index(drop=True)
                df2_sorted = df2_common.sort_values(by=common_cols).reset_index(drop=True)
            except:
                # If sorting fails, convert to string and sort
                df1_sorted = df1_common.astype(str).sort_values(by=common_cols).reset_index(drop=True)
                df2_sorted = df2_common.astype(str).sort_values(by=common_cols).reset_index(drop=True)
        else:
            df1_sorted = df1_common.reset_index(drop=True)
            df2_sorted = df2_common.reset_index(drop=True)
        
        # Check if identical
        try:
            if df1_sorted.equals(df2_sorted):
                comparison_result['identical'] = True
                return comparison_result
        except:
            pass
        
        # Find differences
        if df1_sorted.shape == df2_sorted.shape:
            # Same shape - find cell-level differences
            diff_mask = df1_sorted != df2_sorted
            if diff_mask.any().any():
                diff_positions = []
                for col in common_cols:
                    col_diffs = diff_mask[col]
                    if col_diffs.any():
                        diff_rows = col_diffs[col_diffs].index.tolist()
                        for row in diff_rows[:max_sample_rows]:
                            diff_positions.append({
                                'row': row,
                                'column': col,
                                'db1_value': df1_sorted.iloc[row][col],
                                'db2_value': df2_sorted.iloc[row][col]
                            })
                comparison_result['sample_differences'] = diff_positions
        
        # Row-level differences
        if show_sample_diffs and df1_sorted.shape != df2_sorted.shape:
            # Find rows only in df1
            try:
                merged = df1_sorted.merge(df2_sorted, how='outer', indicator=True)
                only_df1 = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
                only_df2 = merged[merged['_merge'] == 'right_only'].drop('_merge', axis=1)
                
                comparison_result['sample_differences'] = {
                    'only_in_db1_sample': only_df1.head(max_sample_rows).to_dict('records'),
                    'only_in_db2_sample': only_df2.head(max_sample_rows).to_dict('records')
                }
            except:
                pass
        
        return comparison_result
    
    # Main comparison logic
    results = {
        'database_summary': {},
        'table_differences': {},
        'schema_differences': {},
        'data_differences': {}
    }
    
    try:
        # Connect to databases
        conn1 = sqlite3.connect(db1_path)
        conn2 = sqlite3.connect(db2_path)
        
        # Get table names
        tables1 = get_table_names(conn1)
        tables2 = get_table_names(conn2)
        
        results['database_summary'] = {
            'db1_tables': len(tables1),
            'db2_tables': len(tables2),
            'tables_only_in_db1': list(tables1 - tables2),
            'tables_only_in_db2': list(tables2 - tables1),
            'common_tables': list(tables1.intersection(tables2))
        }
        
        # Compare common tables
        common_tables = tables1.intersection(tables2)
        
        for table in common_tables:
            print(f"Comparing table: {table}")
            
            # Compare schemas
            schema1 = get_table_schema(conn1, table)
            schema2 = get_table_schema(conn2, table)
            
            if schema1 != schema2:
                results['schema_differences'][table] = {
                    'db1_schema': schema1,
                    'db2_schema': schema2
                }
            
            # Compare data
            try:
                df1 = pd.read_sql_query(f"SELECT * FROM {table}", conn1)
                df2 = pd.read_sql_query(f"SELECT * FROM {table}", conn2)
                
                comparison = compare_dataframes(df1, df2, table, ignore_order)
                if not comparison['identical']:
                    results['data_differences'][table] = comparison
                    
            except Exception as e:
                results['data_differences'][table] = {'error': str(e)}
        
        conn1.close()
        conn2.close()
        
    except Exception as e:
        results['error'] = str(e)
    
    return results

def print_comparison_summary(results: Dict[str, Any]):
    """Print a formatted summary of the comparison results"""
    
    print("=" * 80)
    print("DATABASE COMPARISON SUMMARY")
    print("=" * 80)
    
    # Database overview
    summary = results.get('database_summary', {})
    print(f"\nDatabase 1 tables: {summary.get('db1_tables', 'N/A')}")
    print(f"Database 2 tables: {summary.get('db2_tables', 'N/A')}")
    
    if summary.get('tables_only_in_db1'):
        print(f"\nTables only in DB1: {summary['tables_only_in_db1']}")
    
    if summary.get('tables_only_in_db2'):
        print(f"\nTables only in DB2: {summary['tables_only_in_db2']}")
    
    # Schema differences
    schema_diffs = results.get('schema_differences', {})
    if schema_diffs:
        print(f"\n📋 SCHEMA DIFFERENCES ({len(schema_diffs)} tables):")
        for table, diff in schema_diffs.items():
            print(f"  - {table}: Schema structures differ")
    
    # Data differences
    data_diffs = results.get('data_differences', {})
    if data_diffs:
        print(f"\n📊 DATA DIFFERENCES ({len(data_diffs)} tables):")
        for table, diff in data_diffs.items():
            if 'error' in diff:
                print(f"  ❌ {table}: Error - {diff['error']}")
            else:
                shape1 = diff.get('df1_shape', 'N/A')
                shape2 = diff.get('df2_shape', 'N/A')
                print(f"  📈 {table}: DB1{shape1} vs DB2{shape2}")
                
                if diff.get('column_differences'):
                    col_diff = diff['column_differences']
                    if col_diff.get('only_in_db1'):
                        print(f"    - Columns only in DB1: {col_diff['only_in_db1']}")
                    if col_diff.get('only_in_db2'):
                        print(f"    - Columns only in DB2: {col_diff['only_in_db2']}")
                
                if diff.get('sample_differences'):
                    print(f"    - Sample differences found (see detailed results)")
    
    if not schema_diffs and not data_diffs:
        print("\n✅ DATABASES ARE IDENTICAL!")