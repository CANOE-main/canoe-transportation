import shutil
import sqlite3
import pandas as pd
import os

db_input = 'canoe_on_12d_vanilla4'
db_output = 'canoe_on_12d_life3'

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
input_db_path = dir_path + '../db_processing/update_database/target_database/' + db_input + '.sqlite'
output_db_path = dir_path + '../' + db_output + '.sqlite'

# Create a copy of the database
shutil.copyfile(input_db_path, output_db_path)

# Connect to the copied database
conn = sqlite3.connect(output_db_path)
cursor = conn.cursor()

# Define necessary data
patterns_to_duplicate = ['T_LDV_C_', 'T_LDV_LT', 'T_MDV_T', 'T_HDV_T']
periods_set = [2021, 2025, 2030, 2035, 2040, 2045, 2050]

# Function to duplicate tech entries in all relevant tables
def duplicate_tech_entries(table_name, tech_column='tech'):
    df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
    tech_entries = df[df[tech_column].str.startswith(tuple(patterns_to_duplicate))]
    
    if not tech_entries.empty:
        new_entries_s25 = tech_entries.copy()
        new_entries_s25[tech_column] += '_S25'

        new_entries_s75 = tech_entries.copy()
        new_entries_s75[tech_column] += '_S75'

        df = pd.concat([df, new_entries_s25, new_entries_s75], ignore_index=True)
        df.to_sql(table_name, conn, if_exists='replace', index=False)

# Apply duplication process to all tables with a 'tech' column
for table in cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall():
    table_name = table[0]
    try:
        columns = [desc[1] for desc in cursor.execute(f'PRAGMA table_info("{table_name}");')]
        if 'tech' in columns:
            duplicate_tech_entries(table_name)
    except sqlite3.OperationalError as e:
        print(f"Error processing table {table_name}: {e}")

# Modifications to LifetimeTech table
try:
    lifetime_df = pd.read_sql_query('SELECT * FROM "LifetimeTech"', conn)
    for index, row in lifetime_df.iterrows():
        tech = row['tech']
        if any(tech.startswith(pattern) for pattern in patterns_to_duplicate):
            if tech.endswith('_S25'):
                if tech.startswith('T_LDV_C_'):
                    lifetime_df.at[index, 'lifetime'] = 7.  # Based on the expected value of the instantaneous scrappage distribution up to the 25th percentile
                elif tech.startswith('T_LDV_LT'):
                    lifetime_df.at[index, 'lifetime'] = 8.
                elif tech.startswith('T_MDV_T'):
                    lifetime_df.at[index, 'lifetime'] = 9.
                elif tech.startswith('T_HDV_T'):
                    lifetime_df.at[index, 'lifetime'] = 9.
            elif tech.endswith('_S75'):
                if tech.startswith('T_LDV_C_'):
                    lifetime_df.at[index, 'lifetime'] = 25. # Based on the expected value of the instantaneous scrappage distribution after the 75th percentile
                elif tech.startswith('T_LDV_LT'):
                    lifetime_df.at[index, 'lifetime'] = 27.
                elif tech.startswith('T_MDV_T'):
                    lifetime_df.at[index, 'lifetime'] = 29.
                elif tech.startswith('T_HDV_T'):
                    lifetime_df.at[index, 'lifetime'] = 31.
    lifetime_df['lifetime'] = pd.to_numeric(lifetime_df['lifetime'], errors='coerce').fillna(0).astype(float)
    lifetime_df.to_sql('LifetimeTech', conn, if_exists='replace', index=False)
except sqlite3.OperationalError as e:
    print(f"Error modifying LifetimeTech table: {e}")

# Modifications to ExistingCapacity table
try:
    capacity_df = pd.read_sql_query('SELECT * FROM "ExistingCapacity"', conn)
    
    lifetime_dict = dict(zip(lifetime_df['tech'], lifetime_df['lifetime']))
    to_remove = []
    to_fix = []

    for _, row in capacity_df.iterrows():
        if row['tech'].endswith(('_S25')):
            tech = row['tech']
            vintage = int(row['vintage'])
            lifetime = float(lifetime_dict.get(tech, 0))

            if vintage + lifetime < 2021:
                to_remove.append((tech, vintage))
                tech_median = tech.replace('_S25', '')
                to_fix.append((tech_median, vintage))
        
    to_remove_df = pd.DataFrame(to_remove, columns=['tech', 'vintage'])
    capacity_df = capacity_df.merge(to_remove_df, on=['tech', 'vintage'], how='left', indicator=True)
    capacity_df = capacity_df[capacity_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    # capacity_df['capacity'] = capacity_df.apply(
    # lambda row: row['capacity'] * 0.25 if row['tech'].endswith(('_S25', '_S75'))
    # else (row['capacity'] * 0.75 if (row['tech'], row['vintage']) in set(to_fix)
    #       else (row['capacity'] * 0.5 if row['tech'].startswith(tuple(patterns_to_duplicate)) and not row['tech'].endswith(('_S25', '_S75'))
    #             else row['capacity'])),
    # axis=1
    # )
    mask_s25_s75 = capacity_df['tech'].str.endswith(('_S25', '_S75'))
    mask_to_fix = capacity_df.apply(lambda row: (row['tech'], row['vintage']) in set(to_fix), axis=1)
    mask_parent_techs = capacity_df['tech'].str.startswith(tuple(patterns_to_duplicate)) & ~capacity_df['tech'].str.endswith(('_S25', '_S75'))

    # Apply vectorized assignments based on masks
    capacity_df.loc[mask_s25_s75, 'capacity'] *= 0.25
    capacity_df.loc[mask_to_fix, 'capacity'] *= 0.75
    capacity_df.loc[mask_parent_techs & ~mask_to_fix, 'capacity'] *= 0.5

    # Append new rows to cost dataframe
    capacity_df.to_sql('ExistingCapacity', conn, if_exists='replace', index=False)

except sqlite3.OperationalError as e:
    print(f"Error modifying ExistingCapacity table: {e}")

# Modifications to Efficiency table
try:
    efficiency_df = pd.read_sql_query('SELECT * FROM "Efficiency"', conn)

    efficiency_df = efficiency_df.merge(to_remove_df, on=['tech', 'vintage'], how='left', indicator=True)
    efficiency_df = efficiency_df[efficiency_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    efficiency_df.to_sql('Efficiency', conn, if_exists='replace', index=False)

except Exception as e:
    print(f"Error modifying Efficiency table: {e}")

# Modifications to Min/MaxAnnualCapacityFactor tables
try:
    maxcf_df = pd.read_sql_query('SELECT * FROM "MaxAnnualCapacityFactor"', conn)
    to_add = []
    to_remove = []

    for _, row in maxcf_df.iterrows():
        if row['tech'].endswith(('_EX_S75')):
            tech = row['tech']
            lifetime = float(lifetime_dict.get(tech, 0))

            valid_periods = [p for p in periods_set if p < 2020 + lifetime]

            for period in valid_periods:
                if not ((maxcf_df['tech'] == tech) & (maxcf_df['period'] == period)).any():
                    latest_period_data = maxcf_df[maxcf_df['tech'] == tech].sort_values(by='period').iloc[-1]

                    new_row = latest_period_data.copy()
                    new_row['period'] = period
                    to_add.append(new_row)
    
        if row['tech'].endswith('_EX_S25'):
            tech = row['tech']
            lifetime = float(lifetime_dict.get(tech, 0))

            valid_periods = [p for p in periods_set if p < 2020 + lifetime]

            # Check if period is not in valid_periods
            if row['period'] not in valid_periods:
                to_remove.append((tech, row['period']))

    to_remove_df = pd.DataFrame(to_remove, columns=['tech', 'period'])
    maxcf_df = maxcf_df.merge(to_remove_df, on=['tech', 'period'], how='left', indicator=True)
    maxcf_df = maxcf_df[maxcf_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    maxcf_df = pd.concat([maxcf_df, pd.DataFrame(to_add)], ignore_index=True).drop_duplicates()
    maxcf_df.to_sql('MaxAnnualCapacityFactor', conn, if_exists='replace', index=False)

    mincf_df = pd.read_sql_query('SELECT * FROM "MinAnnualCapacityFactor"', conn)
    to_add = []
    to_remove = []

    for _, row in maxcf_df.iterrows():
        if row['tech'].endswith(('_EX_S75')):
            tech = row['tech']
            lifetime = float(lifetime_dict.get(tech, 0))

            valid_periods = [p for p in periods_set if p < 2020 + lifetime]

            for period in valid_periods:
                if not ((maxcf_df['tech'] == tech) & (maxcf_df['period'] == period)).any():
                    latest_period_data = maxcf_df[maxcf_df['tech'] == tech].sort_values(by='period').iloc[-1]

                    new_row = latest_period_data.copy()
                    new_row['period'] = period
                    to_add.append(new_row)
    
        if row['tech'].endswith('_EX_S25'):
            tech = row['tech']
            lifetime = float(lifetime_dict.get(tech, 0))

            valid_periods = [p for p in periods_set if p < 2020 + lifetime]

            # Check if period is not in valid_periods
            if row['period'] not in valid_periods:
                to_remove.append((tech, row['period']))
    
    to_remove_df = pd.DataFrame(to_remove, columns=['tech', 'period'])
    mincf_df = mincf_df.merge(to_remove_df, on=['tech', 'period'], how='left', indicator=True)
    mincf_df = mincf_df[mincf_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    mincf_df = pd.concat([mincf_df, pd.DataFrame(to_add)], ignore_index=True).drop_duplicates()
    mincf_df.to_sql('MinAnnualCapacityFactor', conn, if_exists='replace', index=False)

except Exception as e:
    print(f"Error modifying Min/MaxAnnualCapacityFactor tables: {e}")

# Modifications to CostVariable table
try:
    cost_df = pd.read_sql_query('SELECT * FROM "CostVariable"', conn)
    to_add = []
    to_remove = []  

    for _, row in cost_df.iterrows():
        if row['tech'].endswith(('_S75')):
            tech = row['tech']
            vintage = int(row['vintage'])
            lifetime = float(lifetime_dict.get(tech, 0))

            valid_periods = [p for p in periods_set if vintage <= p < vintage + lifetime]

            for period in valid_periods:
                if not ((cost_df['tech'] == tech) & (cost_df['vintage'] == vintage) & (cost_df['period'] == period)).any():
                    latest_period_data = cost_df[
                        (cost_df['tech'] == tech) & (cost_df['vintage'] == vintage)
                    ].sort_values(by='period').iloc[-1]

                    new_row = latest_period_data.copy()
                    new_row['period'] = period
                    to_add.append(new_row)
        
        if row['tech'].endswith('_S25'):
            tech = row['tech']
            vintage = int(row['vintage'])
            lifetime = float(lifetime_dict.get(tech, 0))
            valid_periods = [p for p in periods_set if vintage <= p < vintage + lifetime]

            # Check if period is not in valid_periods
            if row['period'] not in valid_periods:
                to_remove.append((tech, vintage, row['period']))

            # Additional condition: vintage + lifetime < 2021
            if vintage + lifetime < 2021:
                to_remove.append((tech, vintage, row['period']))      

    # Filter out rows in to_remove_df from cost_df
    to_remove_df = pd.DataFrame(to_remove, columns=['tech', 'vintage', 'period'])
    cost_df = cost_df.merge(to_remove_df, on=['tech', 'vintage', 'period'], how='left', indicator=True)
    cost_df = cost_df[cost_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    # Append new rows to cost dataframe
    cost_df = pd.concat([cost_df, pd.DataFrame(to_add)], ignore_index=True).drop_duplicates()
    cost_df.to_sql('CostVariable', conn, if_exists='replace', index=False)

except sqlite3.OperationalError as e:
    print(f"Error modifying CostVariable table: {e}")

# Insert new entries into MinNewCapacityShare table
try:
    min_new_capacity_share_df = pd.read_sql_query('SELECT * FROM "MinNewCapacityShare"', conn)
    new_entries = []

    # Filter techs to be inserted
    tech_entries = [
        tech for tech in lifetime_df['tech']
        if any(tech.startswith(pattern) for pattern in patterns_to_duplicate)
        and not (tech.endswith('_EX') or '_EX_S' in tech)
    ]

    for tech in tech_entries:
        group_name = tech.split('_N')[0]
        if tech.endswith('_S25') or tech.endswith('_S75'):
            max_proportion = 0.25
        else:
            max_proportion = 0.50

        for period in periods_set:
            new_entries.append({
                'tech': tech,
                'group_name': group_name,
                'region': 'ON',
                'period': int(period),
                'max_proportion': float(max_proportion)
            })

    # Append new entries to the dataframe
    new_entries_df = pd.DataFrame(new_entries)
    min_new_capacity_share_df = pd.concat([min_new_capacity_share_df, new_entries_df], ignore_index=True).drop_duplicates()
    min_new_capacity_share_df.to_sql('MinNewCapacityShare', conn, if_exists='replace', index=False)
except sqlite3.OperationalError as e:
    print(f"Error modifying MinNewCapacityShare table: {e}")    

# Insert new entries into TechGroupMember table
try:
    tech_group_member_df = pd.read_sql_query('SELECT * FROM "TechGroupMember"', conn)
    new_tech_group_member_entries = []

    # Create unique tech and group_name pairs to be inserted
    for tech in tech_entries:
        group_name = tech.split('_N')[0]
        new_tech_group_member_entries.append({
            'tech': tech,
            'group_name': group_name
        })

    # Append new entries to the dataframe
    new_tech_group_member_df = pd.DataFrame(new_tech_group_member_entries)
    tech_group_member_df = pd.concat([tech_group_member_df, new_tech_group_member_df], ignore_index=True).drop_duplicates()
    tech_group_member_df.to_sql('TechGroupMember', conn, if_exists='replace', index=False)
except sqlite3.OperationalError as e:
    print(f"Error modifying TechGroupMember table: {e}")

# Insert new entries into TechGroup table
try:
    tech_group_df = pd.read_sql_query('SELECT * FROM "TechGroup"', conn)
    new_tech_group_entries = [{'group_name': group_name} for group_name in set(entry['group_name'] for entry in new_tech_group_member_entries)]

    # Append new entries to the dataframe
    new_tech_group_df = pd.DataFrame(new_tech_group_entries)
    tech_group_df = pd.concat([tech_group_df, new_tech_group_df], ignore_index=True).drop_duplicates()
    tech_group_df.to_sql('TechGroup', conn, if_exists='replace', index=False)
except sqlite3.OperationalError as e:
    print(f"Error modifying TechGroup table: {e}")

# Remove blank rows in specified tables
tables_to_clean = ['LifetimeTech', 'ExistingCapacity', 'CostVariable', 'MinNewCapacityShare', 'TechGroupMember']
for table_name in tables_to_clean:
    try:
        cursor.execute(f"DELETE FROM {table_name} WHERE tech IS NULL OR trim(tech) = '';")
        conn.commit()
    except sqlite3.OperationalError as e:
        print(f"Error cleaning table {table_name}: {e}")

# Close the connection
conn.close()
