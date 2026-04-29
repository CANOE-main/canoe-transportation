import shutil
import sqlite3
import pandas as pd
import os

db_input  = 'canoe_on_12d_baseline_life' 
db_output = 'canoe_on_12d_baseline_life_7'
base_dir = os.path.dirname(os.path.abspath(__file__)) + '/'
# input_db_path  = base_dir + '../db_processing/update_database/target_database/' + db_input  + '.sqlite'
input_db_path = base_dir + '../' + db_input + '.sqlite'
output_db_path = base_dir + '../' + db_output + '.sqlite'

shutil.copyfile(input_db_path, output_db_path)
conn = sqlite3.connect(output_db_path)
cursor = conn.cursor()

# Patterns and new suffixes
patterns_to_duplicate = ['T_LDV_C_', 'T_LDV_LT', 'T_MDV_T', 'T_HDV_T']
periods_set  = [2021, 2025, 2030, 2035, 2040, 2045, 2050]
last_ex_period = 2020

# new_suffixes = ['_S25', '_S75']                                     # for 3 lifetime percentile classes (25th, 50th, 75th)
new_suffixes = ['_S12', '_S24', '_S36', '_S64', '_S76', '_S88']   # for 7 lifetime percentile classes (12th, 24th, 36th, 50th, 64th, 76th, 88th)

# Lifetime values are based on the expected value of the instantaneous scrappage distribution up to the Nth percentile
# lifetime_map = {                                                                    # See CANOE_TRN_ON_v4.xlsx Lifetime sheet for reference (cell AL1)
#     '_S25': {'T_LDV_C_': 7.,  'T_LDV_LT': 8.,  'T_MDV_T': 9.,  'T_HDV_T': 9.},
#     '_S75': {'T_LDV_C_': 24.,  'T_LDV_LT': 27.,  'T_MDV_T': 29.,  'T_HDV_T': 30.}
# }
lifetime_map = {                                                                  # See CANOE_TRN_ON_v4.xlsx Lifetime sheet for reference (cell AL45)
    '_S12': {'T_LDV_C_': 4.,  'T_LDV_LT': 5.,  'T_MDV_T': 6.,  'T_HDV_T': 7.},
    '_S24': {'T_LDV_C_': 9.,  'T_LDV_LT': 10.,  'T_MDV_T': 11.,  'T_HDV_T': 12.},
    '_S36': {'T_LDV_C_': 11.,  'T_LDV_LT': 12., 'T_MDV_T': 13., 'T_HDV_T': 15.},
    '_S64': {'T_LDV_C_': 17., 'T_LDV_LT': 19., 'T_MDV_T': 21., 'T_HDV_T': 24.},
    '_S76': {'T_LDV_C_': 20., 'T_LDV_LT': 24., 'T_MDV_T': 25., 'T_HDV_T': 27.},
    '_S88': {'T_LDV_C_': 28., 'T_LDV_LT': 32., 'T_MDV_T': 32., 'T_HDV_T': 32.}
}

# Capacity‐scaling factors for ExistingCapacity and MinNewCapacityShare
# scaling_factors = {                                                                 # Corresponding to three lifetime classes
#     'percentiles': 0.25,    # scaling factor for new suffixes
#     'parents': 0.50,        # scaling factor for parent technologies
# }
scaling_factors = {                                                               # Corresponding to seven lifetime classes   
    'percentiles': 0.12,
    'parents': 0.28,
}

# Duplicate tech entries in every table that has a 'tech' column
def duplicate_tech_entries(table_name):
    df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
    tech_entries = df[df['tech'].str.startswith(tuple(patterns_to_duplicate))]
    if tech_entries.empty:
        return

    copies = []
    for suf in new_suffixes:
        tmp = tech_entries.copy()
        tmp['tech'] = tmp['tech'] + suf
        copies.append(tmp)

    df_out = pd.concat([df] + copies, ignore_index=True)
    df_out.to_sql(table_name, conn, if_exists='replace', index=False)

for (table_name,) in cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall():
    cols = [c[1] for c in cursor.execute(f'PRAGMA table_info("{table_name}")')]
    if 'tech' in cols:
        duplicate_tech_entries(table_name)


# Modify LifetimeTech
lt_df = pd.read_sql_query('SELECT * FROM "LifetimeTech"', conn)
for idx, row in lt_df.iterrows():
    tech = row['tech']
    for suf in new_suffixes:
        if tech.endswith(suf):
            for pat in patterns_to_duplicate:
                if tech.startswith(pat):
                    lt_df.at[idx, 'lifetime'] = lifetime_map[suf][pat]      # Based on the expected value of the instantaneous scrappage distribution up to the Nth percentile

lt_df['lifetime'] = pd.to_numeric(lt_df['lifetime'], errors='coerce').fillna(0.).astype(float)
lt_df.to_sql('LifetimeTech', conn, if_exists='replace', index=False)

# Build lifetime lookup
lifetime_dict = dict(zip(lt_df['tech'], lt_df['lifetime']))


# Modify ExistingCapacity
ec_df = pd.read_sql_query('SELECT * FROM "ExistingCapacity"', conn)
to_remove = []
to_fix = []

for _, r in ec_df.iterrows():                                               # Identify removals where vintage + lifetime < first period
    tech, vintage = r['tech'], int(r['vintage'])
    for suf in new_suffixes:
        if tech.endswith(suf):
            lifetime = lifetime_dict.get(tech, 0)
            if vintage + lifetime <= periods_set[0]:
                to_remove.append((tech, vintage))
                # parent_tech = tech.replace(suf, '')
                # to_fix.append((parent_tech, vintage))

to_rem_df = pd.DataFrame(to_remove, columns=['tech','vintage'])
ec_df = ec_df.merge(to_rem_df, on=['tech','vintage'], how='left', indicator=True)
ec_df = ec_df[ec_df['_merge']=='left_only'].drop(columns=['_merge'])

# # Apply capacity scaling
# mask_suffixes = ec_df['tech'].str.endswith(tuple(new_suffixes))                            # target techs with suffix
# mask_residuals = ec_df.apply(lambda r: (r['tech'], r['vintage']) in set(to_fix), axis=1)   # target techs without older vintages
# mask_parents = ec_df['tech'].str.startswith(tuple(patterns_to_duplicate)) & \
#                 ~ec_df['tech'].str.endswith(tuple(new_suffixes))                           # target parent techs

# ec_df.loc[mask_suffixes, 'capacity'] *= scaling_factors['percentiles']
# ec_df.loc[mask_residuals, 'capacity'] *= scaling_factors['residuals']
# ec_df.loc[mask_parents & ~mask_residuals, 'capacity'] *= scaling_factors['parents']

# Compute per‐(parent,vintage) residual scaling
to_remove_suff = {}
for tech_suf, vintage in list(set(to_remove)):
    for suf in new_suffixes:
        if tech_suf.endswith(suf):
            parent = tech_suf[:-len(suf)]
            key = (parent, vintage)
            if key not in to_remove_suff:
                to_remove_suff[key] = set()
            to_remove_suff[key].add(suf)

# Now compute residual_scaling only from those
residual_scaling = {}
for (parent, vintage), sufs in to_remove_suff.items():
    n_removed = len(sufs)
    residual_scaling[(parent, vintage)] = (
        scaling_factors['parents']
        + n_removed * scaling_factors['percentiles'])

# Apply all scalings in one vectorized pass
def scale_capacity(row):
    tech, vintage, cap = row['tech'], row['vintage'], row['capacity']
    # a) any suffixed child tech
    if any(tech.endswith(suf) for suf in new_suffixes):
        return cap * scaling_factors['percentiles']

    # b) parent rows that lost children
    key = (tech, vintage)
    if key in residual_scaling:
        return cap * residual_scaling[key]

    # c) all other parent‑tech rows (start pattern, not ending suffix, not a residual)
    if (any(tech.startswith(p) for p in patterns_to_duplicate)
        and not any(tech.endswith(suf) for suf in new_suffixes)
        and key not in residual_scaling):
        return cap * scaling_factors['parents']

    # d) anything else (leave unchanged)
    return cap

ec_df['capacity'] = ec_df.apply(scale_capacity, axis=1)
ec_df.to_sql('ExistingCapacity', conn, if_exists='replace', index=False)

# Clean Efficiency table
eff_df = pd.read_sql_query('SELECT * FROM "Efficiency"', conn)
eff_df = eff_df.merge(to_rem_df, on=['tech','vintage'], how='left', indicator=True)
eff_df = eff_df[eff_df['_merge']=='left_only'].drop(columns=['_merge'])
eff_df.to_sql('Efficiency', conn, if_exists='replace', index=False)


# Modify Min/MaxAnnualCapacityFactor
def adjust_capacity_factor(table_name):
    df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
    to_add, to_remove = [], []
    
    for _, r in df.iterrows():
        tech, period = r['tech'], r['period']
        
        for suf in new_suffixes:
            pct = int(suf[-2:])     # e.g., 12 from _S12

            if tech.endswith('_EX' + suf) and pct > 50:
                lifetime = float(lifetime_dict.get(tech, 0))
                valid_periods = [p for p in periods_set if p < last_ex_period + lifetime]

                for _period in valid_periods:       # subsequent periods will inherit the last available period's data
                    if not ((df['tech'] == tech) & (df['period'] == _period)).any():
                        new_row = df[df['tech'] == tech].sort_values(by='period').iloc[-1].copy()
                        new_row['period'] = _period
                        to_add.append(new_row)

            elif tech.endswith('_EX' + suf) and pct < 50:
                lifetime = float(lifetime_dict.get(tech, 0))
                valid_periods = [p for p in periods_set if p < last_ex_period + lifetime]

                if period not in valid_periods:     # remove invalid periods
                    to_remove.append((tech, period))
    
    to_remove_df = pd.DataFrame(to_remove, columns=['tech', 'period'])
    df = df.merge(to_remove_df, on=['tech', 'period'], how='left', indicator=True)
    df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])
    df = pd.concat([df, pd.DataFrame(to_add)], ignore_index=True).drop_duplicates()

    df.to_sql(table_name, conn, if_exists='replace', index=False)

adjust_capacity_factor('MaxAnnualCapacityFactor')
adjust_capacity_factor('MinAnnualCapacityFactor')
  

# Modify CostVariable
cv_df = pd.read_sql_query('SELECT * FROM "CostVariable"', conn)
to_add, to_remove = [], []

for _, r in cv_df.iterrows():
    tech, period, vintage = r['tech'], r['period'], int(r['vintage'])
    
    for suf in new_suffixes:
        pct = int(suf[-2:])     # e.g., 12 from _S12

        if tech.endswith(suf) and pct > 50:
            lifetime = float(lifetime_dict.get(tech, 0))
            valid_periods = [p for p in periods_set if vintage <= p < vintage + lifetime]

            for _period in valid_periods:       # subsequent periods will inherit the last available period's data
                if not ((cv_df['tech'] == tech) & (cv_df['vintage'] == vintage) & (cv_df['period'] == _period)).any():
                    new_row = cv_df[
                        (cv_df['tech'] == tech) & (cv_df['vintage'] == vintage)
                    ].sort_values(by='period').iloc[-1].copy()
                    new_row['period'] = _period
                    to_add.append(new_row)

        elif tech.endswith(suf) and pct < 50:
            lifetime = float(lifetime_dict.get(tech, 0))
            valid_periods = [p for p in periods_set if vintage <= p < vintage + lifetime]

            if period not in valid_periods:     # remove invalid periods
                to_remove.append((tech, vintage, period))

            if vintage + lifetime < periods_set[0]:
                to_remove.append((tech, vintage, period))
                        
rem_cv = pd.DataFrame(to_remove, columns=['tech','vintage','period'])
cv_df = cv_df.merge(rem_cv, on=['tech','vintage','period'], how='left', indicator=True)
cv_df = cv_df[cv_df['_merge']=='left_only'].drop(columns=['_merge'])
cv_df = pd.concat([cv_df, pd.DataFrame(to_add)], ignore_index=True).drop_duplicates()

cv_df.to_sql('CostVariable', conn, if_exists='replace', index=False)


# Insert new constraints in MinNewCapacityShare
mnc_df = pd.read_sql_query('SELECT * FROM "MinNewCapacityShare"', conn)
new_entries = []
tech_percentiles = [tech for tech in lifetime_dict.keys()
                    if any(tech.startswith(pat) for pat in patterns_to_duplicate)
                    and not (tech.endswith('_EX') or '_EX_S' in tech)]

for tech in tech_percentiles:
    group = tech.split('_N')[0]
    if any(tech.endswith(suf) for suf in new_suffixes):
        max_prop = scaling_factors['percentiles']
    else: 
        max_prop = scaling_factors['parents']
    for period in periods_set:
        new_entries.append({
            'tech': tech,
            'group_name': group,
            'region': 'ON',
            'period': int(period),
            'min_proportion': float(max_prop)
        })

mnc_df = pd.concat([mnc_df, pd.DataFrame(new_entries)], ignore_index=True).drop_duplicates()
mnc_df.to_sql('MinNewCapacityShare', conn, if_exists='replace', index=False)


# Declare TechGroupMember & TechGroup
tgm_df = pd.read_sql_query('SELECT * FROM "TechGroupMember"', conn)
tge_df = pd.read_sql_query('SELECT * FROM "TechGroup"', conn)
new_tgm, new_tg = [], set()

for tech in tech_percentiles:
    group = tech.split('_N')[0]
    new_tgm.append({'tech': tech, 'group_name': group})
    new_tg.add(group)
tgm_df = pd.concat([tgm_df, pd.DataFrame(new_tgm)], ignore_index=True).drop_duplicates()
tge_df = pd.concat([tge_df, pd.DataFrame([{'group_name': g} for g in new_tg])],
                   ignore_index=True).drop_duplicates()

tgm_df.to_sql('TechGroupMember', conn, if_exists='replace', index=False)
tge_df.to_sql('TechGroup',       conn, if_exists='replace', index=False)


# Final cleanup of blank tech rows
for tbl in ['LifetimeTech','ExistingCapacity','CostVariable',
            'MinNewCapacityShare','TechGroupMember']:
    cursor.execute(f"DELETE FROM {tbl} WHERE tech IS NULL OR trim(tech) = '';")
    conn.commit()

conn.close()
print(f"Database '{db_output}.sqlite' has created successfully.")