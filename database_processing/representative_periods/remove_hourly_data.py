import sqlite3
import os
import shutil

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
source = dir_path + '../update_database/canoe_ON_365D_2024-05-27.sqlite'
target = dir_path + 'canoe_on.sqlite'

shutil.copyfile(source, target)

con = sqlite3.connect(target)
cursor = con.cursor()
cursor.execute('DELETE FROM DemandSpecificDistribution')
cursor.execute('DELETE FROM CapacityFactorTech')
cursor.execute('DELETE FROM CapacityFactorProcess')
con.commit()

cursor.execute('VACUUM')
con.close()