import sqlite3
import shutil
import os
import pandas as pd

db_source = 'canoe_on_12d_vanilla4'
db_target = 'canoe_on_12d_vanilla4_cftnorm'

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
source = dir_path + 'target_database/' + db_source + '.sqlite'
target = dir_path + 'target_database/' + db_target + '.sqlite'
tech_name = 'T_LDV_BEV_CHRG'

shutil.copyfile(source, target)

conn = sqlite3.connect(target)
cur = conn.cursor()

# Fetch the maximum factor for the chosen technology
cur.execute(
    f"""SELECT MAX(factor)
        FROM CapacityFactorTech
        WHERE tech = ?""",
    (tech_name,))
result = cur.fetchone()
if result is None or result[0] is None:
    raise ValueError(f"No rows found for tech = {tech_name!r}")

max_val = result[0]
if max_val == 0:
    raise ZeroDivisionError("Maximum factor is zero – cannot normalise.")

print(f"Max factor for {tech_name}: {max_val}")

# 2b. Update every row for that tech in one SQL statement
cur.execute(
    f"""UPDATE CapacityFactorTech
            SET factor = factor / ?
            WHERE tech = ?""",
    (max_val, tech_name))
print(f"Normalised {cur.rowcount} rows")

# Commit the changes and close the connection
conn.commit()
conn.close()


