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
