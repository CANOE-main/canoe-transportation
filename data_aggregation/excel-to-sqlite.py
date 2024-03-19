import pandas as pd
import sqlite3
import numpy as np
import os

# Update these paths according to your files' locations
dir_path = os.path.dirname(os.path.realpath('__file__')) + "/"
database = dir_path + 'canoe_trn.sqlite'
schema = dir_path + 'canoe_schema.sql'
spreadsheet = '../Spreadsheets/CANOE_TRN_ON.xlsx'

# Define the precision of the model parameters
epsilon = 0.0001
precision = 4

wipe_database = True

def instantiate_database():
    
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
    2011 -> 2010
    2012 -> 2010
    2013 -> 2015
    2014 -> 2015
    2015 -> 2015
    """

    base_year = (vintage - 2000) // 5 * 5 + 2000  # Find the base 5-year period start
    offset = vintage - base_year
    if offset <= 2:
        return base_year
    else:
        return base_year + 5



"""
##################################################
    Techs and commodities
##################################################
"""

def compile_techs():
    """
    Reads technologies from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'Eff Calcs'
    parameter = 'Efficiency'
    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel('../Spreadsheets/CANOE_TRN_ON.xlsx', sheet_name='Eff Calcs', header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index('Technological')

    # Imports the table on the excel sheet
    df = pd.read_excel('../Spreadsheets/CANOE_TRN_ON.xlsx', sheet_name='Eff Calcs', skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

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
    if sheet not in pd.ExcelFile(spreadsheet).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(spreadsheet, sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index('Technological')

    # Imports the table on the excel sheet
    df = pd.read_excel(spreadsheet, sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums 
    vintages = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Vintage', value_name=parameter, value_vars=vintages)

    # Aggregates 2000-2020 vintages into 5-year vintages (e.g., 2002 -> 2000 and 2003 -> 2005)
    df.Vintage = df.Vintage.astype(int)
    df_ex = df[df.Vintage <= 2020]
    df_new = df[df.Vintage > 2020]
    df_ex['qVintage'] = df_ex.Vintage.apply(quinquennial_mapping)
    df_ex_agg = df_ex.groupby([i for i in df_ex.columns.tolist() if i not in ['Vintage', parameter]]).agg({parameter: 'mean'}).reset_index()
    df_ex_agg = df_ex_agg.rename(columns={'qVintage': 'Vintage'})
    df = pd.concat([df_ex_agg, df_new], ignore_index=True).reset_index(drop=True)

    # Drops empty parameter rows, convert years into integers, and remove NaNs from the table
    df = df.dropna(subset=[parameter])
    df = df.fillna('')

    # Round values to the nearest precision (decimal place)
    df[parameter] = df[parameter].round(precision)

    # Connect with database and replace parameters
    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute(f"""REPLACE INTO Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{row['Region']}', '{row['Input Commodity']}', '{row['Technology']}', '{row['Vintage']}', '{row['Output Commodity']}', '{row['Efficiency']}', 
                    '{'[' + row['Unit'] +'] ' + row['Notes']}', 
                    '{row['Reference']}', '{row['Data Year']}', '{row['Reliability']}', '{row['Representativeness']}', '{row['Temporal']}', '{row['Geographical']}', '{row['Technological']}')""")
            
    conn.commit()
    conn.close()

    print(f"Efficiency data compiled into {os.path.basename(database)}\n")



