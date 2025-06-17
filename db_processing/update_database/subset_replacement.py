"""
Updates the content of a target .sqlite database with a source .sqlite
@author: Rashid Zetter
"""

import sqlite3
import os
import shutil

def replace_tech_rows(table_name, subset_cursor, source_cursor, target_cursor, log_file, **kwargs):
    """
    Replaces rows based on 'tech' matching any 'tech' from Technology table in the subset db
    
    """
    if subset_cursor is not None:
        # Get the list of techs from the subset Technology table
        subset_cursor.execute('SELECT tech FROM "Technology"')
        techs = [row[0] for row in subset_cursor.fetchall()]
        
        # Delete rows where 'tech' matches any tech from the subset Technology table
        for tech in techs:
            target_cursor.execute(f'DELETE FROM "{table_name}" WHERE tech = ?', (tech,))
    
    # Insert rows from the source database
    source_cursor.execute(f'SELECT * FROM "{table_name}"')
    source_data = source_cursor.fetchall()
    source_cursor.execute(f'PRAGMA table_info("{table_name}")')
    columns = [col[1] for col in source_cursor.fetchall()]

    for row in source_data:
        try:
            target_cursor.execute(f'INSERT INTO "{table_name}" ({", ".join(columns)}) VALUES ({", ".join(["?"] * len(columns))})', row)
            log_file.write(f"Inserted new row in {table_name} for tech: {row[columns.index('tech')]}\n")
        except sqlite3.IntegrityError as e:
            error_message = f"Insert failed for {table_name} with row {row}; with error: {e}"
            print(error_message)
            log_file.write(error_message + "\n")

# Function to replace rows for Commodity, Demand, and DemandSpecificDistribution tables based on 'name' from subset db
def replace_commodity_rows(table_name, column_name, subset_cursor, source_cursor, target_cursor, log_file, **kwargs):
    if subset_cursor is not None:
        # Get the list of names from the subset Commodity table
        subset_cursor.execute('SELECT name FROM "Commodity"')
        names = [row[0] for row in subset_cursor.fetchall()]
        
        # Delete rows where 'name' matches any name from the subset Commodity table
        for name in names:
            target_cursor.execute(f'DELETE FROM "{table_name}" WHERE {column_name} = ?', (name,))
    
    # Insert rows from the source database
    source_cursor.execute(f'SELECT * FROM "{table_name}"')
    source_data = source_cursor.fetchall()
    source_cursor.execute(f'PRAGMA table_info("{table_name}")')
    columns = [col[1] for col in source_cursor.fetchall()]

    for row in source_data:
        try:
            target_cursor.execute(f'INSERT INTO "{table_name}" ({", ".join(columns)}) VALUES ({", ".join(["?"] * len(columns))})', row)
            log_file.write(f"Inserted new row in {table_name} for {column_name}: {row[columns.index(column_name)]}\n")
        except sqlite3.IntegrityError as e:
            error_message = f"Insert failed for {table_name} with row {row}; with error: {e}"
            print(error_message)
            log_file.write(error_message + "\n")

# Special handling for references table
def replace_references(source_cursor, target_cursor, log_file, **kwargs):
    """
    Replaces the references in the target database with those from the source database.
    """
    source_cursor.execute('SELECT * FROM "references"')
    source_references_data = source_cursor.fetchall()
    target_cursor.execute('SELECT reference FROM "references"')
    target_references_data = set([row[0] for row in target_cursor.fetchall()])

    for row in source_references_data:
        if row[0] not in target_references_data:
            try:
                target_cursor.execute('INSERT INTO "references" (reference) VALUES (?)', row)
                log_file.write(f"Inserted new reference: {row[0]}\n")
            except sqlite3.IntegrityError as e:
                error_message = f"Insert failed for references: {row}; with error: {e}"
                print(error_message)
                log_file.write(error_message + "\n")

# Function to remove duplicates from all tables
def remove_duplicates(target_cursor, log_file, **kwargs):
    target_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = target_cursor.fetchall()
    for table in tables:
        table_name = table[0]
        target_cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns = [col[1] for col in target_cursor.fetchall()]
        if columns:
            columns_str = ", ".join(columns)
            # Create a temporary table with unique rows
            target_cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS temp_{table_name} AS 
                SELECT DISTINCT {columns_str} FROM "{table_name}"
            ''')
            # Drop the original table
            target_cursor.execute(f'DROP TABLE "{table_name}"')
            # Rename the temporary table to the original table name
            target_cursor.execute(f'ALTER TABLE temp_{table_name} RENAME TO "{table_name}"')
            log_file.write(f"Removed duplicates from {table_name}\n")

def replace_subset(target_name: str = None,
                   source_name: str = None, 
                   subset_name: str = None,
                   output_name: str = None,
                   target_path: str = None,
                   source_path: str = None, 
                   subset_path: str = None,
                   output_path: str = None,
                   sql_instructions_path: str = None,
                   dir_path: str = None,
                   references: bool = True,
                   tech_tables = None,
                   commodity_tables_dict: dict = None):
    """
    Removes rows in target database that appear in subset database, then adds rows from source database.
    
    This script removes all subset entries from the user-specified list of tables that have any matching tech or commodity index, depending on the table, in the target db. 
    Then, it inserts all the entries from the source db into the same list of tables. An easy way of interpreting this is with the following set operation:

    `output.sqlite = [set(target.sqlite) - set(subset.sqlite)] + set(source.sqlite)`
    
    Modifies Temoa v3 databases.

    Inputs:
    - target_name: str, name of the target database to be updated. E.g. 'canoe_on_12d_vanilla4'
    - source_name: str, name of the source database from which new datapoints are taken. 
        This should have the new or updated entries that you want to insert into target db. E.g. 'canoe_trn_on_vanilla4_v3'
    - subset_name: str, name of the subset database to identify old datapoints to be replaced. 
        This should have the deprecated or outdated entries that you want to remove or update in the target db. E.g. 'canoe_trn_on_vanilla4_v3'
        If None, no rows will be removed from the target database.
    - output_name: str, name of the output database. If provided, creates a copy of target database. If None, the target database will be updated in place.
    - {target, source, subset, output}_path: the direct path to the file, if desired. If '.sqlite' extension isn't present in path, automatically append it.
    - sql_instructions_path: path to an sql file to be run
    - references: bool, if True, replace references in the target database with those from the source database
    - tech_tables: list of str, names of the tables that contain technology data to be updated.
    - commodity_tables_dict: dict {table_name: column_name}, mapping of table names to the column name that contains the commodity or demand name.
    """ 

    # Define the paths for the source, target, and log files
    if dir_path is None:
        dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
    if target_name is not None:
        target_path = dir_path + 'target_database/' + target_name + '.sqlite' #    Database to be updated
    if source_name is not None:
        source_path = dir_path + '../to_temoa_v3/v3_database/' + source_name + '.sqlite' #   Where new datapoints come from
    
    if '.sqlite' not in target_path: target_path += '.sqlite'
    if '.sqlite' not in source_path: source_path += '.sqlite'

    # source = dir_path + 'target_database/' + source_name + '.sqlite' #   Where new datapoints come from
    
    # If either output_name or output_path is provided, create a copy of the target database
    if (output_name is not None) or (output_path is not None):
        if output_path is None:
            output_path = dir_path + 'target_database/' + output_name + '.sqlite'
        if '.sqlite' not in output_path: output_path += '.sqlite'
        shutil.copyfile(target_path, output_path)
        del target_path    # Remove the original target variable to avoid confusion
        target_path = output_path    # All operations will be performed on the output database
    else:
        print('output_name nor output_path not provided, updating target database in place:')
        print(target_path)  

    log = dir_path + 'update_log.txt'

    # shutil.copyfile(original, target)
    
    if type(tech_tables) is str:
        tech_tables = [tech_tables]
    if tech_tables is None:
        # List of tables to target
        tech_tables = [
            'Technology', 
            'LifetimeTech', 
            'ExistingCapacity', 
            'CapacityToActivity',
            'CapacityFactorProcess',
            'CapacityFactorTech', 
            'MaxAnnualCapacityFactor', 
            'MinAnnualCapacityFactor', 
            'Efficiency', 
            'CostInvest', 
            'CostFixed', 
            'CostVariable', 
            'EmissionActivity',
            'EmissionEmbodied', 
            'TechInputSplit',
            # 'StorageDuration'
            ]

    if commodity_tables_dict is None:
        commodity_tables_dict = {
            'Commodity': 'name', 
            'Demand': 'commodity', 
            # 'DemandSpecificDistribution': 'demand_name'
            }
    
    try:
        # Connect to the source, target, and subset databases
        source_conn = sqlite3.connect(source_path)
        target_conn = sqlite3.connect(target_path)

        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()

        if subset_name is None and subset_path is None:
            # subset not provided
            subset_conn = None
            subset_cursor = None
        else:
            if subset_path is None:
                subset_path = dir_path + '../to_temoa_v3/v3_database/' + subset_name + '.sqlite' #   To identify the old datapoints to be replaced
            # subset = dir_path + 'target_database/' + subset_name + '.sqlite' #   To identify the old datapoints to be replaced
            if '.sqlite' not in subset_path: subset_path += '.sqlite'
            subset_conn = sqlite3.connect(subset_path)
            subset_cursor = subset_conn.cursor()

        # Open the log file
        log_file = open(log, 'w')

        if sql_instructions_path is not None:
            # execute sql instructions
            if '.sql' not in sql_instructions_path: sql_instructions_path += '.sql'
            with open(sql_instructions_path, 'r') as f:
                sql_script = f.read()
            target_cursor.executescript(sql_script)

        # dictionary to pass arguments to the functions
        config = {'subset_cursor': subset_cursor, 
                'source_cursor': source_cursor, 
                'target_cursor': target_cursor, 
                'log_file': log_file}
        
        # Get list of tables in source_conn (excluding SQLite internal tables)
        source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        source_tables = [row[0] for row in source_cursor.fetchall()]
        print("Tables in source_conn:", source_tables)

        # Replace rows for tables with 'tech' key
        for table_name in tech_tables:
            if table_name in source_tables:
                replace_tech_rows(table_name, **config)

        # Replace rows for Commodity, Demand, and DemandSpecificDistribution tables
        for table_name, col in commodity_tables_dict.items():
            replace_commodity_rows(table_name, col, **config)

        if not references:
            print('References not replaced.')
        elif 'references' not in source_tables:
            print("'references' not in source_tables. Will not replace references.")
        else:
            replace_references(**config)    

        remove_duplicates(**config)

        # Commit the changes and close the connections
        target_conn.commit()
        target_cursor.execute('VACUUM;') #  Reclaims unused space
        print("Done. Saved to:", target_path)

    finally:
        source_conn.close()
        target_conn.close()
        if subset_conn is not None:
            subset_conn.close()

        # Close the log file
        log_file.close()

if __name__ == "__main__":
    # Example usage
    replace_subset(
        target_name='canoe_on_12d_vanilla4',
        source_name='canoe_trn_on_vanilla4_v3',
        subset_name='canoe_trn_on_vanilla4_v3',
        replace_references=True,
    )