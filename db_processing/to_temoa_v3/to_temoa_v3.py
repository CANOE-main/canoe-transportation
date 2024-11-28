import sqlite3
import shutil
import os

db_name = 'canoe_trn_bct_vanilla3'

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
sql_file = dir_path + 'temoa_v2_to_v3.txt'
target = dir_path + 'v3_database/' + db_name + '_v3.sqlite'
source = dir_path + '../../transportation/compiled_database/' + db_name + '.sqlite'

shutil.copyfile(source, target)

with open(sql_file, 'r') as file:
    sql_script = file.read()

conn = sqlite3.connect(target)
cursor = conn.cursor()
cursor.executescript(sql_script)

# Update 'unlim_cap' column
cursor.execute("""
    UPDATE Technology
    SET unlim_cap = CASE
        WHEN tech LIKE 'T_IMP%' OR 
             tech LIKE 'T_BLND%' OR 
             tech LIKE 'T_EA%' OR 
             tech LIKE 'T_OFF%' OR 
             tech LIKE 'H2_distribution' THEN 1
        ELSE 0
    END
""")

# Update 'annual' column
cursor.execute("""
    UPDATE Technology
    SET annual = CASE
        WHEN tech LIKE 'T_LDV_C%' OR
             tech LIKE 'T_LDV_LTP%' OR
             tech LIKE 'T_LDV_LTF%' OR 
             (tech LIKE '%H2%' AND tech NOT LIKE '%H2_N') OR
             tech LIKE '%CHRG' OR
             (tech LIKE 'T_BLND%' AND tech LIKE '%ELC%') OR
             tech = 'T_IMP_ELC' OR 
             tech = 'ELC_AC_DC' THEN 0
        ELSE 1
    END
""")

conn.commit()
conn.close()