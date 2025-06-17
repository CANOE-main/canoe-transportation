# Convenience functions
import sqlite3

def connect_to_db(db_path):
    """
    Connects to the SQLite database at the specified path and returns the connection and cursor.
    If the database does not exist, it creates a new one.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    return conn, cursor

def act_with_db_connection(db_path, callback, **kwargs):
    """
    Safely connects to the database and executes the callback function with the connection.
    Ensures the connection is properly closed even if an error occurs.
    
    Args:
        db_path: Path to the SQLite database
        callback: Function that takes a connection object as argument
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        return callback(conn, **kwargs)
    finally:
        if conn:
            conn.close()

def remove_empty_tables(db_path=None, cursor=None):
    """
    Removes all tables from the SQLite database at db_path that have zero rows.
    """
    if db_path is None and cursor is None:
        raise ValueError("Either db_path or cursor must be provided.")
    if db_path is not None and cursor is not None:
        raise ValueError("Only one of db_path or cursor should be provided.")
    if cursor is None:
        conn, cursor = connect_to_db(db_path)

    # Get all user tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in cursor.fetchall()]
    for table in tables:
        cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
        count = cursor.fetchone()[0]
        if count == 0:
            print(f"Dropping empty table: {table}")
            cursor.execute(f'DROP TABLE "{table}"')
    if cursor is None:
        conn.commit()
        conn.close()

def get_unique_seasons(db_path=None, cursor=None):
    """
    Returns a set of unique 'season' values from all tables in the given SQLite database.
    db_path: str, path to the SQLite database file.
    """
    if db_path is None and cursor is None:
        raise ValueError("Either db_path or cursor must be provided.")
    if db_path is not None and cursor is not None:
        raise ValueError("Only one of db_path or cursor should be provided.")
    if cursor is None:
        conn, cursor = connect_to_db(db_path)
    
    unique_seasons = set()
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    for table in tables:
        # Check if table has a 'season' column
        cursor.execute(f'PRAGMA table_info("{table}")')
        columns = [col[1] for col in cursor.fetchall()]
        if 'season' in columns:
            cursor.execute(f'SELECT DISTINCT season FROM "{table}"')
            seasons = [row[0] for row in cursor.fetchall()]
            unique_seasons.update(seasons)

    if cursor is None:
        conn.close()

    return list(unique_seasons)

def get_primary_keys(cursor, table_name):
    """
    Returns a list of primary key column names for the given table.
    """
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    return [row[1] for row in cursor.fetchall() if row[5] > 0]

# Example usage:
# primary_keys = get_primary_keys(target_cursor, "Technology")
# print(primary_keys)