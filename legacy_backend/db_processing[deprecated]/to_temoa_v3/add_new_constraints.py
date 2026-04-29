import sqlite3
import shutil
import os

db_name = 'canoe_on_12d_vanilla4'

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
sql_file = dir_path + 'new_constraints.txt'
target = dir_path + '../update_database/target_database/' + db_name + '_cons.sqlite'
source = dir_path + '../update_database/target_database/' + db_name + '.sqlite'

shutil.copyfile(source, target)

with open(sql_file, 'r') as file:
    sql_script = file.read()

conn = sqlite3.connect(target)
cursor = conn.cursor()
cursor.executescript(sql_script)

conn.commit()
conn.close()