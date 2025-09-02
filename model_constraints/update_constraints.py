import sqlite3
import shutil
import os
import pandas as pd

# Function to clear and insert data into the SQLite database
def replace_data(table_name, data, cursor):
    # Clear existing data
    # cursor.execute(f'DELETE FROM {table_name}')
    
    # Get existing columns in the table
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_cols = set(row[1] for row in cursor.fetchall())
    # Add missing columns
    for col in data.columns:
        if col not in existing_cols:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT")
            print(f"Added missing column '{col}' as TEXT to table '{table_name}'")
    
    # Insert new data
    columns = data.columns.tolist()
    placeholders = ', '.join(['?'] * len(columns))
    insert_sql = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({placeholders})'
    
    for _, row in data.iterrows():
        cursor.execute(insert_sql, tuple(row))

def insert_v3_1_growth_rate_seed(sheet_data, cursor):
    """Insert growth rate seed data for v3.1 database.
    If database is not v3.1, return the original sheet_data. Else, convert growth rate tables to v3.1 format and return sheet_data without growth rate tables.
    """
    growth_rate_tables = ['GrowthRateMin', 'GrowthRateMax']
    tables_in_db = []
    
    import re
    
    # Look for tables limit(de)growth(new)capacity(delta) regex
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0].lower() for row in cursor.fetchall()]
    # Regex: matches limit(de)?growth(new)?capacity(delta)?
    pattern = re.compile(r'limit(de)?growth(new)?capacity(delta)?')

    if any(pattern.match(t) for t in tables):
        print("Detected v3.1 growth-related tables in the database.")

        # MinNewCapacityShare, MinNewCapacityGroupShare: Put in LimitNewCapacityShare. Concat to single table, add operator 'ge'
        operator_added_tables = {
            'MinNewCapacityShare': ('LimitNewCapacityShare', 'ge'),
            'MinNewCapacityGroupShare': ('LimitNewCapacityShare', 'ge')}
        
        column_rename = {'min_proportion': 'share', 'tech': 'sub_group', 
                         'sub_group_name': 'sub_group', 'group_name': 'super_group'}
        
        for old_name, (new_name, operator) in operator_added_tables.items():
            data = sheet_data.pop(old_name, None)  # remove from sheet_data to avoid re-insertion later
            if data is None:
                print('Spreadsheet not found: ' + old_name)
                continue
            data = data.rename(columns=column_rename)    # Rename columns
            data['operator'] = operator    # Add operator column
            replace_data(new_name, data, cursor)    # Add to table
            print(f"Inserted data into {new_name} with operator '{operator}' from {old_name}.")
        
        # GroupGrowthRateSeed: Put in LimitGrowthCapacity
        # GroupGrowthRateMax: Put in LimitGrowthCapacity. Take average across periods. Merge with GroupGrowthRateSeed. Add operator 'le'.
        growth_rate_seed, growth_rate_max = sheet_data.pop('GroupGrowthRateSeed', None), sheet_data.pop('GroupGrowthRateMax', None)
        if (growth_rate_seed is None) or (growth_rate_max is None): print('Spreadsheet not found: GroupGrowthRateSeed or GroupGrowthRateMax')
        # Take average growth rate across periods
        if 'period' in growth_rate_max.columns:
            print('Warning: GroupGrowthRateMax has period column. Taking average across periods.')
            growth_rate_max = growth_rate_max.groupby(['region', 'group_name'], as_index=False).agg({'rate': 'mean', 'notes': 'first'})
        # Merge seed and growth max
        merged = pd.merge(growth_rate_seed.rename(columns={'units': 'seed_units'}), 
                          growth_rate_max, on=['region', 'group_name'], suffixes=('_seed', '_max'))
        merged = (merged.assign(notes = lambda x: x['notes_seed'].astype(str) + ' | ' + x['notes_max'].astype(str),
                               operator = 'le')    # Add operator column
                        .rename(columns = {'group_name': 'tech_or_group'})
                        .drop(columns=['notes_seed', 'notes_max']))
        replace_data('LimitGrowthCapacity', merged, cursor)    # Add to table
        print(f"Inserted data into LimitGrowthCapacity")

        # return sheet_data without growth rate tables if they exist
        return sheet_data

    else:
        print("No v3.1 growth-related tables detected in the database. Skipping growth rate insertion.")
        return sheet_data

def update_constraints(db_source = None, db_target = None, constraints = None,
                       db_source_path = None, db_target_path = None, constraints_path = None,
                       vintages = [2021, 2025, 2030, 2035, 2040, 2045, 2050]
                       ):
    """Update the constraints in the database with new data.
    Inputs:
    - db_source: str, name of the source database to be copied. E.g. 'canoe_on_12d_vanilla4'
    - db_target: str, name of the target database where the constraints will be updated. E.g. 'canoe_on_12d_lowgrowth'
    - constraints: str, name of the Excel file containing the new constraints data (without extension). E.g. 'trn_constraints_lowgrowth'
    - xxx_path: supply the full path if needed.
    - vintages: list, list of vintages to replace vintage=='All' in the 'LoanRate' table.
    """
    
    dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
    if db_source_path is None: source = dir_path + '../db_processing/update_database/target_database/' + db_source + '.sqlite'
    if db_target_path is None: target = dir_path + '../' + db_target + '.sqlite'
    if constraints_path is None: sheet_path = dir_path + constraints + '.xlsx'

    if db_source is None: source = db_source_path
    if db_target is None: target = db_target_path
    if constraints is None: sheet_path = constraints_path
 
    shutil.copyfile(source, target)

    # Load the Excel file
    excel_data = pd.ExcelFile(sheet_path)

    # Load data from each sheet into a dictionary of DataFrames
    sheet_data = {sheet: excel_data.parse(sheet) for sheet in excel_data.sheet_names}

    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(target)
        cursor = conn.cursor()

        # insert growth rate constraints for v3.1 database
        sheet_data = insert_v3_1_growth_rate_seed(sheet_data, cursor)    # Returns sheet_data without growth rate tables if successful

        # Iterate over the sheets and replace data in the corresponding tables
        for sheet_name, data in sheet_data.items():
            # Check if sheet_name exists in the database
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (sheet_name,))
            if cursor.fetchone() is None:
                print(f"Warning: Table '{sheet_name}' does not exist in the database. Skipping...")
                continue
            
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
        print(f"Updated constraints from {sheet_path} in {target} database.")
        
    finally:
        # Close the connection to the database
        conn.close()

if __name__ == "__main__":
    db_source = 'canoe_on_12d_vanilla4'
    db_target = 'canoe_on_12d_lowgrowth'
    constraints = 'trn_constraints_lowgrowth'
    update_constraints(db_source, db_target, constraints)
