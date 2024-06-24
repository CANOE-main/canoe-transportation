"""
Merges CANOE model sectors in sqlite format 
@author: Rashid Zetter
"""

import sqlite3
import os

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
schema = dir_path + '../canoe_schema.sql'
trn_db = dir_path + '../transportation/canoe_trn.sqlite'
merged_db = dir_path + 'canoe_merged.sqlite'

wipe_database= True


def read_files(directory):
    """ 
    Loads all available sqlite files in this directory
    """
    f_name = []
    for root, _, f_names in os.walk(directory):
        for f in f_names:
            c_name = os.path.join(root, f)
            _, file_extension = os.path.splitext(c_name)
            if (file_extension == '.sqlite'):
                f_name.append(c_name)

    return f_name

def merge_database(db):
    """
    Selects all tables from a database to be inserted into the master database from the canoe_schema.sql
    """    
    con.execute("ATTACH ? as dba", (db,))
    con.execute("BEGIN")

    master_tables = [table[1] for table in con.execute("SELECT * FROM sqlite_master WHERE type='table'")]
    tables = [table[1] for table in con.execute("SELECT * FROM dba.sqlite_master WHERE type='table'")]

    for t in tables:
        if t not in master_tables:
            print(f"Table {t} not in target database and so was ignored.")
            continue

        master_columns = [d[0] for d in con.execute(f"SELECT * FROM '{t}' WHERE 1=0;").description]
        columns = [d[0] for d in con.execute(f"SELECT * FROM dba.'{t}' WHERE 1=0;").description]

        for col in columns:
            if col not in master_columns:
                print(f"Column {col} not in target table '{t}' and so was ignored.")

        combine = f"INSERT OR IGNORE INTO '{t}' SELECT {', '.join(columns)} FROM dba.'{t}'"
        con.execute(combine)

    con.commit()
    con.execute("DETACH DATABASE dba")

def batch_merge(directory, trn_directory):
    """
    Execute merging process for each database file 
    """
    print("\nMerging into:")
    print(merged_db)
    db_files = read_files(directory) + [trn_directory] #   Include transportation database from a separate directory

    for db_file in db_files:
        filename = os.path.basename(db_file)
        if (db_file != merged_db):
            print(f"Merging {os.path.basename(db_file)}")
            merge_database(db_file)
    

if __name__ == "__main__":

    build_db = not os.path.exists(merged_db)

    con = sqlite3.connect(merged_db)
    curs = con.cursor()
    if build_db:
        with open(schema, 'r') as schema_file:
            curs.executescript(schema_file.read())
    elif wipe_database:
        tables = [t[0] for t in curs.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
        for table in tables:
            curs.execute(f"DELETE FROM '{table}'")
        print("Database wiped prior to merging process.\n")
    con.commit()

    batch_merge(dir_path, trn_db)

    con.close()
