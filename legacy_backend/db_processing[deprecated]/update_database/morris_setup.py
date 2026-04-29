import sqlite3
import os

target_name = 'canoe_on_12d_vanilla_morris'
dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
target = dir_path + 'target_database/' + target_name + '.sqlite' #    Database to be updated

conn = sqlite3.connect(target)
cursor = conn.cursor()

# MMorris currently only recognizes co2 emission commodities
try:
    cursor.execute("DELETE FROM EmissionActivity WHERE emis_comm != 'co2e';")
    print("Rows removed where emis_comm is not 'co2e'.")
except Exception as e:
    print(f"Error removing rows: {e}")

# Replace "co2e" with "co2" in emis_comm
try:
    cursor.execute("UPDATE EmissionActivity SET emis_comm = 'co2' WHERE emis_comm = 'co2e';")
    print('Replaced "co2e" with "co2" in emis_comm.')
except Exception as e:
    print(f"Error updating emis_comm: {e}")

# Function to add the MMAnalysis column to a table if it exists
def add_mm_analysis_column(table_name):
    try:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN MMAnalysis TEXT NULL;")
        print(f"Column MMAnalysis added to {table_name}.")
    except Exception as e:
        print(f"Error adding column to {table_name}: {e}")

# List of tables to augment
tables = ["CostInvest", "CostVariable", "Efficiency"]

# Add the MMAnalysis column to each table
for table in tables:
    add_mm_analysis_column(table)

# Add group entries into MMAnalysis
queries = [
    # CostInvest: Insert "H2_supply_cap_cost" for rows with tech like "%H2%"
    ("UPDATE CostInvest SET MMAnalysis = 'H2_supply_cap_cost' WHERE tech LIKE '%H2%';", "CostInvest H2"),
    
    # CostVariable: Insert "RT_fuel_cost" for specific tech values
    ("UPDATE CostVariable SET MMAnalysis = 'RT_fuel_cost' WHERE tech IN ('T_IMP_GSL', 'T_IMP_DSL', 'T_IMP_CNG', 'T_IMP_NG');", "CostVariable RT_fuel_cost"),
    
    # CostInvest: Insert "LDV_BEV_cap_cost" for tech like "T_LDV%" and "%BEV%", except "T_LDV_M%"
    ("UPDATE CostInvest SET MMAnalysis = 'LDV_BEV_cap_cost' WHERE tech LIKE 'T_LDV%' AND tech LIKE '%BEV%' AND tech NOT LIKE 'T_LDV_M%';", "CostInvest LDV_BEV"),
    
    # CostInvest: Insert "MHDV_H2_cap_cost" for tech like "T_MDV%" or "T_HDV_T%" and "%FC%"
    ("UPDATE CostInvest SET MMAnalysis = 'MHDV_H2_cap_cost' WHERE (tech LIKE 'T_MDV%' OR tech LIKE 'T_HDV_T%') AND tech LIKE '%FC%';", "CostInvest MHDV_H2"),
    
    # Efficiency: Insert "LDV_BEV_eff" for tech like "T_LDV%" and "%BEV%", except "T_LDV_M%"
    ("UPDATE Efficiency SET MMAnalysis = 'LDV_BEV_eff' WHERE tech LIKE 'T_LDV%' AND tech LIKE '%BEV%' AND tech NOT LIKE 'T_LDV_M%';", "Efficiency LDV_BEV_eff"),
    
    # Efficiency: Insert "MHDV_H2_eff" for tech like "T_MDV%" or "T_HDV_T%" and "%FC%"
    ("UPDATE Efficiency SET MMAnalysis = 'MHDV_H2_eff' WHERE (tech LIKE 'T_MDV%' OR tech LIKE 'T_HDV_T%') AND tech LIKE '%FC%';", "Efficiency MHDV_H2_eff")

    # ("UPDATE Efficiency SET MMAnalysis = 'RT_eff' WHERE tech LIKE 'T_LDV%' AND tech LIKE '%BEV%' AND tech NOT LIKE 'T_LDV_M%';", "Efficiency LDV_BEV_eff")
]

# Execute each query and print status
for query, description in queries:
    try:
        cursor.execute(query)
        print(f"Update applied: {description}")
    except Exception as e:
        print(f"Error updating {description}: {e}") 

# Commit changes and close the connection
conn.commit()
conn.close()
