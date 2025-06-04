import sqlite3
import shutil
import os
import pandas as pd

# Function to clear and insert data into the SQLite database
def replace_data(table_name, data, cursor):
    # Clear existing data
    # cursor.execute(f'DELETE FROM {table_name}')
    # Insert new data
    columns = data.columns.tolist()
    placeholders = ', '.join(['?'] * len(columns))
    insert_sql = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({placeholders})'
    
    for _, row in data.iterrows():
        cursor.execute(insert_sql, tuple(row))

def update_constraints(db_source: str, db_target: str, constraints: str,
                       vintages = [2021, 2025, 2030, 2035, 2040, 2045, 2050]
                       ):
    """Update the constraints in the database with new data.
    Inputs:
    - db_source: str, name of the source database to be copied. E.g. 'canoe_on_12d_vanilla4'
    - db_target: str, name of the target database where the constraints will be updated. E.g. 'canoe_on_12d_lowgrowth'
    - constraints: str, name of the Excel file containing the new constraints data (without extension). E.g. 'trn_constraints_lowgrowth'
    - vintages: list, list of vintages to replace vintage=='All' in the 'LoanRate' table.
    """
    
    dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
    source = dir_path + '../db_processing/update_database/target_database/' + db_source + '.sqlite'
    target = dir_path + '../' + db_target + '.sqlite'
    sheet_path = dir_path + constraints + '.xlsx'

    shutil.copyfile(source, target)

    # Load the Excel file
    excel_data = pd.ExcelFile(sheet_path)

    # Load data from each sheet into a dictionary of DataFrames
    sheet_data = {sheet: excel_data.parse(sheet) for sheet in excel_data.sheet_names}

    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(target)
        cursor = conn.cursor()

        # Iterate over the sheets and replace data in the corresponding tables
        for sheet_name, data in sheet_data.items():
            if sheet_name == 'LoanRate':
                # cursor.execute('DELETE FROM LoanRate')
                for _, row in data.iterrows():
                    if row['vintage'] == 'All':
                        for vintage in vintages:
                            new_row = row.copy()
                            new_row['vintage'] = vintage
                            cursor.execute(
                                'INSERT INTO LoanRate (region, tech, vintage, rate, notes) VALUES (?, ?, ?, ?, ?)',
                                tuple(new_row)
                            )
                    else:
                        cursor.execute(
                            'INSERT INTO LoanRate (region, tech, vintage, rate, notes) VALUES (?, ?, ?, ?, ?)',
                            tuple(row)
                        )
            # elif sheet_name == 'GrowthRateMin':
            #     melted_data = data.melt(id_vars=['region', 'tech', 'notes', 'reference'], var_name='period', value_name='rate')
            #     replace_data(sheet_name, melted_data, cursor)
            # elif sheet_name == 'GrowthRateMax':
            #     melted_data = data.melt(id_vars=['region', 'tech', 'notes', 'reference'], var_name='period', value_name='rate')
            #     replace_data(sheet_name, melted_data, cursor)
            else:
                replace_data(sheet_name, data, cursor)

        # Commit the changes
        conn.commit()
        print(f"Updated constraints from {constraints} in {db_target} database.")
        
    finally:
        # Close the connection to the database
        conn.close()

if __name__ == "__main__":
    db_source = 'canoe_on_12d_vanilla4'
    db_target = 'canoe_on_12d_lowgrowth'
    constraints = 'trn_constraints_lowgrowth'
    update_constraints(db_source, db_target, constraints)
