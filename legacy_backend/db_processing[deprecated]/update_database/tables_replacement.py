import sqlite3
import os

db_source = 'canoe_on_365d_vanilla'
db_target = 'canoe_on_365d_vanilla4_cf'

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
source = dir_path + 'target_database/' + db_source + '.sqlite'
target = dir_path + 'target_database/' + db_target + '.sqlite'

tables_to_replace = ["TimeSeason", "TimeSegmentFraction"]

def replace_tables(source_db, target_db, tables):
    """
    Replace the specified tables in target_db with data from source_db.
    """
    try:
        source_conn = sqlite3.connect(source_db)
        source_cursor = source_conn.cursor()
        
        target_conn = sqlite3.connect(target_db)
        target_cursor = target_conn.cursor()
        
        for table in tables:
            print(f"Processing table: {table}")
            
            # Fetch data from source
            source_cursor.execute(f"SELECT * FROM {table}")
            data = source_cursor.fetchall()
            
            # Get column names
            source_cursor.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in source_cursor.fetchall()]
            
            # Clear target table
            target_cursor.execute(f"DELETE FROM {table}")
            
            # Insert data into target table
            if data:
                placeholders = ', '.join(['?'] * len(columns))
                insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
                target_cursor.executemany(insert_query, data)
                print(f"Replaced {len(data)} rows in {table}")
            else:
                print(f"No data found in {table} in source database")
        
        target_conn.commit()
        
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    
    finally:
        source_conn.close()
        target_conn.close()
        print("Databases closed.")

replace_tables(source, target, tables_to_replace)
