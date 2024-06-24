import sqlite3
import shutil

sql_file = 'temoa_v2_to_v3.txt'
target = 'canoe_trn_v3.sqlite'
source = '../../transportation/canoe_trn.sqlite'

shutil.copyfile(source, target)

with open(sql_file, 'r') as file:
    sql_script = file.read()

conn = sqlite3.connect(target)
cursor = conn.cursor()
cursor.executescript(sql_script)

conn.commit()
conn.close()