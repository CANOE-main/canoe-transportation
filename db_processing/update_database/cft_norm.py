import sqlite3
import shutil
import os
import pandas as pd

def normalize_cft(db_source: str,
                  db_target: str,
                  tech_names, 
                  dir_path: str = None):
    """
    Normalizes the capacity factor ('CapacityFactorTech') for a specific technology in the database.
    Divides capacity factor by the maximum value found for that technology.

    Inputs:
    - db_source: str, name of the source database to be copied. E.g. 'canoe_on_12d_vanilla4'
    - db_target: str, name of the target database where the normalization will be applied. E.g. 'canoe_on_12d_vanilla4_cftnorm'
    - tech_names: str or list, names of the technologies for which the capacity factor will be normalized.
    """

    if type(tech_names) is str: tech_names = [tech_names]    # convert to list if tech_names is a string

    db_source = 'canoe_on_12d_vanilla4'
    db_target = 'canoe_on_12d_vanilla4_cftnorm'

    dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
    source = dir_path + 'target_database/' + db_source + '.sqlite'
    target = dir_path + 'target_database/' + db_target + '.sqlite'

    shutil.copyfile(source, target)

    try:
        conn = sqlite3.connect(target)
        cur = conn.cursor()

        # Fetch the maximum factor for the chosen technology
        for tech_name in tech_names:
            print(f"Normalizing capacity factor: {tech_name}")
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
    
        # Commit the changes
        conn.commit()
    
    finally:
        # Close the connection to the database
        conn.close()

if __name__ == "__main__":
    # Example usage
    db_source = 'canoe_on_12d_vanilla4'
    db_target = 'canoe_on_12d_vanilla4_cftnorm'
    tech_names = 'T_LDV_BEV_CHRG'
    
    normalize_cft(db_source, db_target, tech_names)

