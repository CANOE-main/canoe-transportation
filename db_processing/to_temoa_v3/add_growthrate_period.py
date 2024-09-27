import sqlite3
import shutil
import os

db_name = 'canoe_on_365d_vanilla'

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
target = dir_path + '../update_database/target_database/' + db_name + '_wperiod.sqlite'
source = dir_path + '../update_database/target_database/' + db_name + '.sqlite'

shutil.copyfile(source, target)

conn = sqlite3.connect(target)

# Function to add column and reorder table
def add_column_and_reorder_table(table_name):
    # Adding a new column 'period' to the specified table
    alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN period;"
    conn.execute(alter_table_query)

    # Fetching the existing columns
    cursor = conn.execute(f"PRAGMA table_info({table_name});")
    columns = [col[1] for col in cursor.fetchall()]

    # Reordering columns: region, tech, period, rate, ...
    reordered_columns = ['region', 'tech', 'period'] + [col for col in columns if col not in ['region', 'tech', 'period']]

    # Create a temporary table with reordered columns
    create_temp_table_query = f"""
    CREATE TABLE {table_name}_temp AS
    SELECT {', '.join(reordered_columns)}
    FROM {table_name};
    """

    # Drop the original table
    drop_table_query = f"DROP TABLE {table_name};"

    # Rename the temporary table to the original table name
    rename_table_query = f"ALTER TABLE {table_name}_temp RENAME TO {table_name};"

    # Execute the queries
    conn.execute(create_temp_table_query)
    conn.execute(drop_table_query)
    conn.execute(rename_table_query)

# Process GrowthRateMin table
add_column_and_reorder_table('GrowthRateMin')

# Process GrowthRateMax table
add_column_and_reorder_table('GrowthRateMax')

# Commit changes and close the connection
conn.commit()
conn.close()