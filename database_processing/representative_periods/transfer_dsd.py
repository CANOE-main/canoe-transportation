import sqlite3

# Connect to the source and destination SQLite databases
src_conn = sqlite3.connect('../canoe_trn_repday.sqlite')
dst_conn = sqlite3.connect('../canoe_trn.sqlite')

src_cursor = src_conn.cursor()
dst_cursor = dst_conn.cursor()

# List of tables to replace
tables_to_replace = ['DemandSpecificDistribution', 'SegFrac', 'time_season']

for table in tables_to_replace:
    # Drop the table in the destination database if it exists
    dst_cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    # Create the table in the destination database using the schema from the source database
    src_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
    create_table_sql = src_cursor.fetchone()[0]
    dst_cursor.execute(create_table_sql)
    
    # Copy the data from the source database to the destination database
    src_cursor.execute(f"SELECT * FROM {table}")
    rows = src_cursor.fetchall()
    if rows:
        placeholders = ', '.join('?' for _ in rows[0])
        dst_cursor.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

# Commit the changes and close the connections
dst_conn.commit()
src_conn.close()
dst_conn.close()