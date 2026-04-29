import sqlite3
import os

db_source = 'CANOE_ON_12D'
db_target = 'canoe_on_12d_vanilla4_2'

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
source = dir_path + 'target_database/' + db_source + '.sqlite'
target = dir_path + 'target_database/' + db_target + '.sqlite'

target_tables = [
    'CapacityCredit',
    'LifetimeTech', 
    'ExistingCapacity', 
    'CapacityToActivity', 
    'MaxAnnualCapacityFactor', 
    'MinAnnualCapacityFactor', 
    # 'Efficiency', 
    'CostInvest', 
    'CostFixed', 
    'CostVariable', 
    'EmissionActivity', 
    # 'TechInputSplit'
]

def update_matching_techs(source_db, target_db, tables):
    source_conn = sqlite3.connect(source_db)
    target_conn = sqlite3.connect(target_db)
    source_cur = source_conn.cursor()
    target_cur = target_conn.cursor()
    
    # Get the list of 'tech' values from the Technology table in target that do NOT start with 'T_'
    target_cur.execute("SELECT DISTINCT tech FROM Technology WHERE tech NOT LIKE 'T_%'")
    existing_techs = {row[0] for row in target_cur.fetchall()}
    
    print(f"Found {len(existing_techs)} matching techs in target DB.")
    
    for table in tables:
        print(f"Updating table: {table}")
        
        # Retrieve matching techs that exist in the source DB for this table
        source_cur.execute(f"SELECT DISTINCT tech FROM {table} WHERE tech IN ({','.join(['?'] * len(existing_techs))})", tuple(existing_techs))
        valid_techs = {row[0] for row in source_cur.fetchall()}
        
        if not valid_techs:
            print(f"No matching techs found in source DB for {table}. Skipping update.")
            continue
        
        # Retrieve matching rows from the source DB
        source_cur.execute(f"SELECT * FROM {table} WHERE tech IN ({','.join(['?'] * len(valid_techs))})", tuple(valid_techs))
        source_data = source_cur.fetchall()
        
        if not source_data:
            print(f"No matching rows found in source DB for {table}.")
            continue
        
        # Get column names dynamically
        source_cur.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in source_cur.fetchall()]
        
        # Delete existing rows in target with matching 'tech' that exist in the source DB
        target_cur.execute(f"DELETE FROM {table} WHERE tech IN ({','.join(['?'] * len(valid_techs))})", tuple(valid_techs))
        
        # Insert new data from source
        insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
        target_cur.executemany(insert_query, source_data)
        
        print(f"Updated {len(source_data)} rows in {table}.")
    
    target_conn.commit()
    source_conn.close()
    target_conn.close()
    print("Update process completed.")

update_matching_techs(source, target, target_tables)
