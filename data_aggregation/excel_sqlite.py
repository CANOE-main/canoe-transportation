"""
Compiles Excel spreadsheets into Temoa sqlite format using the canoe_schema.sql 
@author: Rashid Zetter
"""

import pandas as pd
import sqlite3
import numpy as np
import os
from datetime import datetime

# Update these paths according to your files' locations
dir_path = os.path.dirname(os.path.realpath('__file__')) + "/"
database = dir_path + 'output_database/canoe_trn.sqlite'
schema = dir_path + 'data_aggregation/canoe_schema.sql'
spreadsheet = dir_path + 'input_spreadsheets/CANOE_TRN_ON.xlsx'

# Define the precision of the model parameters
epsilon = 0.0001
precision = 4

# Rewrite database from scratch if it doesn't exist
wipe_database = True

"""
##################################################
    Initial setup
##################################################
"""

def instantiate_database():
    """
    Create sqlite database from schema sql file
    """
    
    # Check if database exists or needs to be built
    build_db = not os.path.exists(database)

    # Connect to the new database file
    conn = sqlite3.connect(database)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    # Build the database if it doesn't exist. Otherwise clear all data if forced
    if build_db: curs.executescript(open(schema, 'r').read())
    elif wipe_database:
        tables = [t[0] for t in curs.execute("""SELECT name FROM sqlite_master WHERE type='table';""").fetchall()]
        for table in tables: curs.execute(f"DELETE FROM '{table}'")
        print("Database wiped prior to aggregation. See params.\n")
    
    conn.commit()
    conn.close()

def quinquennial_mapping(vintage):
    """
    Maps vintages into 5-year periods for aggregation, following this format:
    2010 -> 2010
    2011 -> 2015
    2012 -> 2015
    2013 -> 2015
    2014 -> 2015
    2015 -> 2015
    """
    # base_year = (vintage - 2000) // 5 * 5 + 2000  # Find the base 5-year period start
    # offset = vintage - base_year
    # if offset <= 2:
    #     return base_year
    # else:
    #     return base_year + 5

    return 5 * -((vintage - 2000) // -5) + 2000 #   Ceiling years to the closest multiple of 5
    
def dq_time(data_year):
    """
    Calculates time appropriateness DQI based on Data Year.
    """
    if not isinstance(data_year, (int, float)): # Check if data_year is a number
        return ""
    
    base_year = datetime.today().year  # Current year
    diff = abs(base_year - int(data_year))  # Convert to int in case of float and calculate difference

    data_quality = {
        3: 1,
        6: 2,
        10: 3,
        15: 4
    }

    for key in sorted(data_quality.keys()):  # Iterate through sorted keys to ensure correct order
        if diff <= key:
            return data_quality[key]
    
    return 5  # Return 5 for greater than 15 years difference

def cleanup():
    """
    Removes existing techs of a given vintage with no capacity
    """
    tables = ["ExistingCapacity", "Efficiency", "CostVariable"] #   Tables to check for tech-vintage pairs with exist_cap = 0
    
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    tech_vintage_remove = curs.execute("""SELECT DISTINCT tech, vintage FROM ExistingCapacity WHERE exist_cap = 0""").fetchall()
    for table in tables:
            for tech, vintage in tech_vintage_remove:
                curs.execute(f"""DELETE FROM {table} WHERE tech = ? AND vintage = ?""", (tech, vintage))

    conn.commit()
    conn.close()

    print(f"Existing techs of a given vintage with no capacity have been removed in {os.path.basename(database)}\n")

"""
##################################################
    Basic parameters
##################################################
"""

def compile_ref():
    """
    Reads references from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'References'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet)

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO "references"(reference) VALUES (?)""", (f"[Transport] {row['References']}",)) # value is treated as a single tuple containing one element by adding a comma inside the tuple
            
    conn.commit()
    conn.close()

    print(f"References compiled into {os.path.basename(database)}\n")

"""
##################################################
    Techs and commodities
##################################################
"""

def compile_techs():
    """
    Reads technologies from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'Techs'
    last_col = 'Category'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to read
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df = df.fillna('')

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute(f"""REPLACE INTO technologies(tech, flag, sector, tech_desc, tech_category, additional_notes)
                    VALUES('{row['Technology']}', '{row['Flag']}', 'Transport', '{row['Description']}', '{row['Category']}', '{row['Details']}')""")
            
    conn.commit()
    conn.close()

    print(f"Technology data compiled into {os.path.basename(database)}\n")

def compile_comms():
    """
    Reads commodities from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'Comms'
    last_col = 'Details'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df = df.fillna('')

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute(f"""REPLACE INTO commodities(comm_name, flag, comm_desc, additional_notes)
                    VALUES('{row['Commodity']}', '{row['Flag']}', '{row['Description']}', '{row['Details']}')""")
            
    conn.commit()
    conn.close()

    print(f"Commodity data compiled into {os.path.basename(database)}\n")

"""
##################################################
    Demand
##################################################
"""

def compile_demand():
    """
    Reads demands from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'Demand Calcs'
    parameter = 'Demand'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums 
    periods = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Period', value_name=parameter, value_vars=periods)

    # Drops empty parameter rows, convert years into integers, and remove NaNs from the table
    df = df.dropna(subset=[parameter])
    df = df.fillna('')

    # Round values to the nearest precision (decimal place)
    df[parameter] = df[parameter].round(precision)

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO Demand(regions, periods, demand_comm, demand, demand_units, demand_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (row['Region'], row['Period'], row['Demand Commodity'], row[parameter], row['Unit'], row['Notes'], 
                    row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))

    conn.commit()
    conn.close()

    print(f"Demand data compiled into {os.path.basename(database)}\n")

"""
##################################################
    Lifetime
##################################################
"""

def compile_lifetime():
    """
    Reads lifetimes from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'Lifetime'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df = df.fillna('')

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO LifetimeTech(regions, tech, life, life_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (row['Region'], row['Technology'], row['Lifetime'], row['Notes'], 
                    row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))

    conn.commit()
    conn.close()

    print(f"Lifetime data compiled into {os.path.basename(database)}\n")

"""
##################################################
    Existing capacity
##################################################
"""

def compile_excap():
    """
    Reads existing capacities from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'ExCap Calcs'
    parameter = 'ExistingCapacity'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums and drops rows with empty parameters
    vintages = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Vintage', value_name=parameter, value_vars=vintages)
    df = df.dropna(subset=[parameter])

    # Aggregates 2000-2020 vintages into 5-year vintages (e.g., 2002 -> 2000 and 2003 -> 2005)
    df.Vintage = df.Vintage.astype(int)
    df_ex = df[df.Vintage <= 2020]
    df_new = df[df.Vintage > 2020]
    df_ex['qVintage'] = df_ex.Vintage.apply(quinquennial_mapping)
    df_ex_agg = df_ex.groupby([i for i in df_ex.columns.tolist() if i not in ['Vintage', parameter]]).agg({parameter: 'sum'}).reset_index()
    df_ex_agg = df_ex_agg.rename(columns={'qVintage': 'Vintage'})
    df = pd.concat([df_ex_agg, df_new], ignore_index=True).reset_index(drop=True)

    # Fill NaNs as empty values
    df = df.fillna('')

    # Round values to the nearest precision (decimal place)
    df[parameter] = df[parameter].round(precision)

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO ExistingCapacity(regions, tech, vintage, exist_cap, exist_cap_units, exist_cap_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row['Region'], row['Technology'], row['Vintage'], row[parameter], row['Unit'], row['Notes'], 
                row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Existing capacity data compiled into {os.path.basename(database)}\n")

"""
##################################################
    Capacity to activity
##################################################
"""

def compile_c2a():
    """
    Reads c2a factors from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'CapToActivity'
    last_col = 'Notes'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df = df.fillna('')

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO CapacityToActivity(regions, tech, c2a, c2a_notes)
                        VALUES(?, ?, ?, ?)""",
                    (row['Region'], row['Technology'], row['Capacity to Activity'], f"[{row['Activity Unit']}/{row['Capacity Unit']}] {row['Notes']}"))

    conn.commit()
    conn.close()

    print(f"C2A factors data compiled into {os.path.basename(database)}\n")

"""
##################################################
    MaxAnnualCapacityFactor
##################################################
"""

def compile_acf():
    """
    Reads annual cap factors from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'CapFactor'
    parameter = 'MaxAnnualCapFactor'
    last_col = 'Notes'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums and drops rows with empty parameters
    vintages = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Vintage', value_name=parameter, value_vars=vintages)
    df = df.dropna(subset=[parameter])

    # Aggregates 2000-2020 vintages into 5-year vintages (e.g., 2002 -> 2000 and 2003 -> 2005)
    df.Vintage = df.Vintage.astype(int)
    df_ex = df[df.Vintage <= 2020]
    df_new = df[df.Vintage > 2020]
    df_ex['qVintage'] = df_ex.Vintage.apply(quinquennial_mapping)
    df_ex_agg = df_ex.groupby([i for i in df_ex.columns.tolist() if i not in ['Vintage', parameter]]).agg({parameter: 'mean'}).reset_index()
    df_ex_agg = df_ex_agg.rename(columns={'qVintage': 'Vintage'})
    df = pd.concat([df_ex_agg, df_new], ignore_index=True).reset_index(drop=True)

    # Fill NaNs as empty values
    df = df.fillna('')

    # Round values to the nearest precision (decimal place)
    df[parameter] = df[parameter].round(precision)

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO MaxAnnualCapacityFactor(regions, periods, tech, max_acf, max_acf_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row['Region'], row['Vintage'], row['Technology'], row[parameter], row['Notes'], 
                row['Reference'], row['Data Year'], 1, 1, dq_time(row['Data Year']), 1, 1))
    
    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO MinAnnualCapacityFactor(regions, periods, tech, min_acf, min_acf_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row['Region'], row['Vintage'], row['Technology'], row[parameter]*0.99, f"99% of MaxAnnualCapacityFactor for computational slack. {row['Notes']}", 
                row['Reference'], row['Data Year'], 1, 1, dq_time(row['Data Year']), 1, 1))
            
    conn.commit()
    conn.close()

    print(f"Max/min annual cap factors data compiled into {os.path.basename(database)}\n")

"""
##################################################
    Efficiency
##################################################
"""

def compile_efficiency():
    """
    Reads efficiencies from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'Eff Calcs'
    parameter = 'Efficiency'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums and drops rows with empty parameters
    vintages = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Vintage', value_name=parameter, value_vars=vintages)
    df = df.dropna(subset=[parameter])

    # Aggregates 2000-2020 vintages into 5-year vintages (e.g., 2002 -> 2000 and 2003 -> 2005)
    df.Vintage = df.Vintage.astype(int)
    df_ex = df[df.Vintage <= 2020]
    df_new = df[df.Vintage > 2020]
    df_ex['qVintage'] = df_ex.Vintage.apply(quinquennial_mapping)
    df_ex_agg = df_ex.groupby([i for i in df_ex.columns.tolist() if i not in ['Vintage', parameter]]).agg({parameter: 'mean'}).reset_index()
    df_ex_agg = df_ex_agg.rename(columns={'qVintage': 'Vintage'})
    df = pd.concat([df_ex_agg, df_new], ignore_index=True).reset_index(drop=True)

    # Fill NaNs as empty values
    df = df.fillna('')

    # Round values to the nearest precision (decimal place)
    df[parameter] = df[parameter].round(precision)

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row['Region'], row['Input Commodity'], row['Technology'], row['Vintage'], row['Output Commodity'], row[parameter], f"[{row['Unit']}] {row['Notes']}", 
                row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Efficiency data compiled into {os.path.basename(database)}\n")

"""
##################################################
    Investment costs
##################################################
"""

def compile_costinvest():
    """
    Reads investment costs from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'InvestCosts Calcs'
    parameter = 'InvestCosts'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums 
    vintages = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Vintage', value_name=parameter, value_vars=vintages)

    # Drops empty parameter rows, convert years into integers, and remove NaNs from the table
    df = df.dropna(subset=[parameter])
    df = df.fillna('')

    # Round values to the nearest precision (decimal place)
    df[parameter] = df[parameter].round(precision)

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO CostInvest(regions, tech, vintage, cost_invest, cost_invest_units, cost_invest_notes, data_cost_invest, data_cost_year, data_curr,
                     reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row['Region'], row['Technology'], row['Vintage'], row[parameter], f"{int(row['Currency Year'])} {row['Currency']}", row['Notes'], 
                 round(row[parameter]/row['Conversion Factor'], precision), row['Original Currency Year'], row['Original Currency'],
                row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Investment cost data compiled into {os.path.basename(database)}\n")

"""
##################################################
    Variable costs
##################################################
"""

def compile_costvariable():
    """
    Reads variable costs from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'VarCosts Calcs'
    parameter = 'VarCosts'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums 
    vintages = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Vintage', value_name=parameter, value_vars=vintages)
    df.Vintage = df.Vintage.astype(int)

    # Drops empty parameter rows, convert years into integers, and remove NaNs from the table
    df = df.dropna(subset=[parameter])
    df = df.fillna('')

    # Round values to the nearest precision (decimal place)
    df[parameter] = df[parameter].round(precision)

    # Reads technologies' lifetimes
    df_lifetime = pd.read_excel(spreadsheet, sheet_name = 'Lifetime', skiprows=[0], usecols=['Technology', 'Lifetime'])

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()
    
    for _idx, row in df.iterrows():
        
        lifetime = df_lifetime[df_lifetime.Technology == row['Technology']].Lifetime.values[0]
        if row['Period'] < row['Vintage'] or row['Vintage'] + lifetime <= row['Period']: continue # Checks for var costs outside the expected technology's lifetime

        curs.execute("""REPLACE INTO CostVariable(regions, periods, tech, vintage, cost_variable, cost_variable_units, cost_variable_notes, data_cost_variable, data_cost_year, data_curr,
                     reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row['Region'], row['Period'], row['Technology'], row['Vintage'], row[parameter], f"{int(row['Currency Year'])} {row['Currency']}", row['Notes'], 
                 round(row[parameter]/row['Conversion Factor'], precision), row['Original Currency Year'], row['Original Currency'],
                row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Variable cost data compiled into {os.path.basename(database)}\n")

"""
##################################################
    Fixed costs
##################################################
"""

def compile_costfixed():
    """
    Reads fixed costs from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'FixedCosts Calcs'
    parameter = 'FixedCosts'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums 
    vintages = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Vintage', value_name=parameter, value_vars=vintages)
    df.Vintage = df.Vintage.astype(int)

    # Drops empty parameter rows, convert years into integers, and remove NaNs from the table
    df = df.dropna(subset=[parameter])
    df = df.fillna('')

    # Round values to the nearest precision (decimal place)
    df[parameter] = df[parameter].round(precision)

    # Reads technologies' lifetimes
    df_lifetime = pd.read_excel(spreadsheet, sheet_name = 'Lifetime', skiprows=[0], usecols=['Technology', 'Lifetime'])

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()
    
    for _idx, row in df.iterrows():
        
        lifetime = df_lifetime[df_lifetime.Technology == row['Technology']].Lifetime.values[0]
        if row['Period'] < row['Vintage'] or row['Vintage'] + lifetime <= row['Period']: continue # Checks for var costs outside the expected technology's lifetime

        curs.execute("""REPLACE INTO CostFixed(regions, periods, tech, vintage, cost_fixed, cost_fixed_units, cost_fixed_notes, data_cost_fixed, data_cost_year, data_curr,
                     reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row['Region'], row['Period'], row['Technology'], row['Vintage'], row[parameter], f"{int(row['Currency Year'])} {row['Currency']}", row['Notes'], 
                 round(row[parameter]/row['Conversion Factor'], precision), row['Original Currency Year'], row['Original Currency'],
                row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Fixed cost data compiled into {os.path.basename(database)}\n")

"""
##################################################
    Tech input split
##################################################
"""

def compile_techinputsplit():
    """
    Reads tech input commodity splits from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'TechInput Split'
    parameter = 'TechInputSplit'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums and drops rows with empty parameters
    periods = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Period', value_name=parameter, value_vars=periods)
    df = df.dropna(subset=[parameter])

    # Fill NaNs as empty values
    df = df.fillna('')

    # Round values to the nearest precision (decimal place)
    df[parameter] = df[parameter].round(precision)

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO TechInputSplit(regions, periods, input_comm, tech, ti_split, ti_split_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row['Region'], row['Period'], row['Input Commodity'], row['Technology'], row[parameter], row['Notes'], 
                row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Tech input commodity split data compiled into {os.path.basename(database)}\n")

"""
##################################################
    Compile all parameters
##################################################
"""

def compile_all():
    """
    Runs all compiling functions
    """
    instantiate_database()

    compile_ref()
    compile_techs()
    compile_comms()
    compile_demand()
    compile_lifetime()
    compile_excap()
    compile_c2a()
    compile_acf()
    compile_efficiency()
    compile_costinvest()
    compile_costvariable()
    compile_costfixed()
    compile_techinputsplit()

    cleanup()

    print(f"All parameter data from {os.path.basename(spreadsheet)} compiled into {os.path.basename(database)}\n")

# instantiate_database()
# compile_techinputsplit()
# compile_all()