"""
Updates the content of a target .sqlite database with a source .sqlite
@author: Rashid Zetter
"""

import pandas as pd
import sqlite3
import numpy as np
import os

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
target = dir_path + 'canoe_on.sqlite'
source = dir_path + '../to_temoa_v3/canoe_trn_2024-06-22_v3.sqlite'
log = dir_path + 'update_log.txt'

# Connect to the source and target databases
source_conn = sqlite3.connect(source)
target_conn = sqlite3.connect(target)

source_cursor = source_conn.cursor()
target_cursor = target_conn.cursor()

# Open the log file
log_file = open(log, 'w')

# Define keys and parameters for each table
tables_info = {
    'Commodity': {'keys': ['name'], 'parameter': 'description'},
    'Demand': {'keys': ['period', 'commodity'], 'parameter': 'demand'},
    'LifetimeTech': {'keys': ['tech'], 'parameter': 'lifetime'},
    'ExistingCapacity': {'keys': ['tech', 'vintage'], 'parameter': 'capacity'},
    'CapacityToActivity': {'keys': ['tech'], 'parameter': 'c2a'},
    'MaxAnnualCapacityFactor': {'keys': ['tech', 'period'], 'parameter': 'factor'},
    'MinAnnualCapacityFactor': {'keys': ['tech', 'period'], 'parameter': 'factor'},
    'Efficiency': {'keys': ['tech', 'vintage', 'input_comm'], 'parameter': 'efficiency'},
    'CostInvest': {'keys': ['tech', 'vintage'], 'parameter': 'cost'},
    'CostFixed': {'keys': ['tech', 'vintage', 'period'], 'parameter': 'cost'},
    'CostVariable': {'keys': ['tech', 'vintage', 'period'], 'parameter': 'cost'},
    'EmissionActivity': {'keys': ['tech', 'input_comm', 'emis_comm'], 'parameter': 'activity'},
    'TechInputSplit': {'keys': ['tech', 'input_comm', 'period'], 'parameter': 'min_proportion'},
}

# Function to update a table based on keys and parameter
def update_table(table_name, keys, parameter):
    source_cursor.execute(f'SELECT * FROM "{table_name}"')
    source_data = source_cursor.fetchall()
    
    source_cursor.execute(f'PRAGMA table_info("{table_name}")')
    columns = [col[1] for col in source_cursor.fetchall()]
    
    for row in source_data:
        key_values = [row[columns.index(key)] for key in keys]
        param_value = row[columns.index(parameter)]
        ref_value = row[columns.index('reference')]
        
        # Construct conditions for SQL WHERE clause
        conditions = " AND ".join([f"{key} = ?" for key in keys])
        # Execute SELECT query on target table
        target_cursor.execute(f'SELECT * FROM "{table_name}" WHERE {conditions}', key_values)
        target_row = target_cursor.fetchone()
        
        if target_row:
            # If a matching row is found, check if the parameter or reference value needs to be updated
            target_param_value = target_row[columns.index(parameter)]
            target_ref_value = target_row[columns.index('reference')]
            
            if target_param_value != param_value or target_ref_value != ref_value:
                update_values = [row[columns.index(col)] for col in columns]
                update_set = ", ".join([f"{col} = ?" for col in columns])
                try:
                    target_cursor.execute(f'UPDATE "{table_name}" SET {update_set} WHERE {conditions}', update_values + key_values)
                except sqlite3.IntegrityError as e:
                    error_message = f"Update failed for {table_name} with keys {key_values}: {e}"
                    print(error_message)
                    log_file.write(error_message + "\n")
        else:
            # If no matching row is found, insert the new row into the target table
            try:
                insert_values = [row[columns.index(col)] for col in columns]
                placeholders = ", ".join(["?"] * len(columns))
                target_cursor.execute(f'INSERT INTO "{table_name}" ({", ".join(columns)}) VALUES ({placeholders})', insert_values)
                log_file.write(f"Inserted new row in {table_name} with keys {key_values}\n")
            except sqlite3.IntegrityError as e:
                error_message = f"Insert failed for {table_name} with keys {key_values}: {e}"
                print(error_message)
                log_file.write(error_message + "\n")

# Update each table based on the keys and parameters
for table_name, info in tables_info.items():
    update_table(table_name, info['keys'], info['parameter'])

# Special handling for Technology table
source_cursor.execute('SELECT * FROM "Technology" WHERE LOWER(sector) IN ("transport", "transportation")')
source_technology_data = source_cursor.fetchall()
target_cursor.execute('DELETE FROM "Technology" WHERE LOWER(sector) IN ("transport", "transportation")')
for row in source_technology_data:
    try:
        target_cursor.execute(f'INSERT INTO "Technology" VALUES ({", ".join(["?"] * len(row))})', row)
        # log_file.write(f"Inserted new row in Technology: {row}\n")
    except sqlite3.IntegrityError as e:
        error_message = f"Insert failed for Technology: {row}; with error: {e}"
        print(error_message)
        log_file.write(error_message + "\n")

# Special handling for DemandSpecificDistribution table
demand_names = ['T_D_pkm_ldv_c', 'T_D_pkm_ldv_t', 'T_D_tkm_ldv_t']
for demand_name in demand_names:
    source_cursor.execute(f'SELECT * FROM "DemandSpecificDistribution" WHERE demand_name = ?', (demand_name,))
    source_demand_data = source_cursor.fetchall()
    target_cursor.execute(f'DELETE FROM "DemandSpecificDistribution" WHERE demand_name = ?', (demand_name,))
    for row in source_demand_data:
        try:
            target_cursor.execute(f'INSERT INTO "DemandSpecificDistribution" VALUES ({", ".join(["?"] * len(row))})', row)
            # log_file.write(f"Inserted new row in DemandSpecificDistribution: {row}\n")
        except sqlite3.IntegrityError as e:
            error_message = f"Insert failed for DemandSpecificDistribution: {row}; with error: {e}"
            print(error_message)
            log_file.write(error_message + "\n")

# Special handling for references table
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

# Commit the changes and close the connections
target_conn.commit()
source_conn.close()
target_conn.close()

# Close the log file
log_file.close()