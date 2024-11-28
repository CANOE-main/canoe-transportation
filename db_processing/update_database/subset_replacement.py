"""
Updates the content of a target .sqlite database with a source .sqlite
@author: Rashid Zetter
"""

import sqlite3
import os
import shutil

target_name = 'canoe_on_12d_vanilla3'
source_name = 'canoe_trn_on_vanilla3_v3'
subset_name = 'canoe_trn_vanilla2_v3'

# Define the paths for the source, target, and log files
dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
target = dir_path + 'target_database/' + target_name + '.sqlite' #    Database to be updated
source = dir_path + '../to_temoa_v3/v3_database/' + source_name + '.sqlite' #   Where new datapoints come from
subset = dir_path + '../to_temoa_v3/v3_database/' + subset_name + '.sqlite' #   To identify the old datapoints to be replaced
log = dir_path + 'update_log.txt'

# shutil.copyfile(original, target)

# Connect to the source, target, and subset databases
source_conn = sqlite3.connect(source)
target_conn = sqlite3.connect(target)
subset_conn = sqlite3.connect(subset)

source_cursor = source_conn.cursor()
target_cursor = target_conn.cursor()
subset_cursor = subset_conn.cursor()

# Open the log file
log_file = open(log, 'w')

# Replace references
references = True

# List of tables to target
tech_tables = [
    'Technology', 
    'LifetimeTech', 
    'ExistingCapacity', 
    'CapacityToActivity', 
    'MaxAnnualCapacityFactor', 
    'MinAnnualCapacityFactor', 
    'Efficiency', 
    'CostInvest', 
    'CostFixed', 
    'CostVariable', 
    'EmissionActivity', 
    'TechInputSplit'
    ]
commodity_tables = {
    'Commodity': 'name', 
    'Demand': 'commodity', 
    # 'DemandSpecificDistribution': 'demand_name'
    }

# Function to replace rows based on 'tech' matching any 'tech' from Technology table in the subset db
def replace_tech_rows(table_name):
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
def replace_commodity_rows(table_name, column_name):
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
def replace_references():
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
def remove_duplicates():
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

# Replace rows for tables with 'tech' key
for table_name in tech_tables:
    replace_tech_rows(table_name)

# Replace rows for Commodity, Demand, and DemandSpecificDistribution tables
for table_name, col in commodity_tables.items():
    replace_commodity_rows(table_name, col)

if references:
    replace_references()

remove_duplicates()

# Commit the changes and close the connections
target_conn.commit()
target_cursor.execute('VACUUM;') #  Reclaims unused space

source_conn.close()
target_conn.close()
subset_conn.close()

# Close the log file
log_file.close()