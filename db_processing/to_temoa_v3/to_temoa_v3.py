import sqlite3
import shutil
import os

def convert_to_temoa_v3(
        db_name: str,    # e.g. 'canoe_trn_on_vanilla4'
        charging_dsd: bool = False,    # choose whether to represent LD EV charging demand distribution in the DSD (True) or CFT (False) Temoa tables
        dir_path = None):
    """
    Converts the Temoa v2 database to v3 format.
    """

    if dir_path is None:
        dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
    sql_file = dir_path + 'temoa_v2_to_v3.txt'
    target = dir_path + 'v3_database/' + db_name + '_v3.sqlite'
    source = dir_path + '../../transportation/compiled_database/' + db_name + '.sqlite'

    shutil.copyfile(source, target)

    with open(sql_file, 'r') as file:
        sql_script = file.read()
    
    try:
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
                    tech LIKE 'T_dummy%' OR    
                    tech LIKE 'H2_distribution' THEN 1
                ELSE 0
            END
        """)

        # Update 'annual' column
        cursor.execute("""
            UPDATE Technology
            SET annual = CASE
                WHEN tech LIKE 'T_LDV_C_BEV%' OR
                    tech LIKE 'T_LDV_LTP_BEV%' OR
                    tech LIKE 'T_LDV_LTF_BEV%' OR
                    tech LIKE 'T_LDV_M_BEV%' OR
                    tech LIKE 'I_H2%' OR
                    tech = 'T_LDV_BEV_CHRG' OR                  
                    tech = 'T_IMP_ELC' OR
                    tech = 'H2_COMP_10_100' OR
                    tech = 'H2_distribution' OR
                    tech = 'H2_storage' OR
                    tech = 'ELC_AC_DC' THEN 0
                ELSE 1
            END
        """)

        # Update 'cf_fixed' column
        if not charging_dsd:
            print('charging_dsd:', charging_dsd)
            cursor.execute("""
                UPDATE Technology
                SET cf_fixed = 1
                WHERE tech = 'T_LDV_BEV_CHRG'
            """)

        conn.commit()
        print("Conversion to Temoa v3 completed successfully.")
        print('Database saved to:', target)
    
    finally:
        conn.close()

if __name__ == "__main__":
    # Example usage
    convert_to_temoa_v3(db_name='canoe_trn_on_vanilla4', charging_dsd=False)