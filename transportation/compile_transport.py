"""
Compiles CANOE-TRN Excel spreadsheets into Temoa sqlite format using the canoe_schema.sql 
@author: Rashid Zetter
"""

import pandas as pd
import sqlite3
import numpy as np
import os
from datetime import datetime
import re
import unicodedata

import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')    # Ignore UserWarnings from openpyxl


"""
##################################################
    Initial setup
##################################################
"""

def instantiate_database(config):
    """
    Create sqlite database from schema sql file
    """
    # Check if database exists or needs to be built
    build_db = not os.path.exists(config['database'])

    # Connect to the new database file
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    # Build the database if it doesn't exist. Otherwise clear all data if forced
    if build_db:
        curs.executescript(open(config['schema'], 'r').read())
    elif config.get('wipe_database', False):
        curs.executescript(open(config['schema'], 'r').read())
        tables = [t[0] for t in curs.execute("""SELECT name FROM sqlite_master WHERE type='table';""").fetchall()]
        for table in tables:
            curs.execute(f"DELETE FROM '{table}'")
        print("Database wiped prior to aggregation.\n")
    
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

def normalize_to_ascii(text):
    """
    Function to normalize text to ASCII
    """
    # Normalize to NFKD form which separates characters from their diacritical marks
    normalized = unicodedata.normalize('NFKD', text)
    # Encode to ASCII bytes, ignore non-ASCII characters, then decode back to string
    ascii_encoded = normalized.encode('ascii', 'ignore').decode('ascii')
    # Replace special characters with ASCII equivalents
    ascii_encoded = (ascii_encoded
                     .replace('–', '-')
                     .replace('—', '-')
                     .replace('’', "'")
                     .replace('…', '...')
                     .replace('®', '(R)'))
    return ascii_encoded

def cleanup(config):
    """
    Removes existing techs of a given vintage with no capacity
    """
    tables = ["ExistingCapacity", "Efficiency", "CostVariable", "CostFixed"] #   Tables to check for tech-vintage pairs with exist_cap = 0
    
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    tech_vintage_remove = curs.execute(f"""SELECT DISTINCT tech, vintage FROM ExistingCapacity WHERE exist_cap < {config['epsilon']}""").fetchall()
    for table in tables:
            for tech, vintage in tech_vintage_remove:
                print(f"Deleted {tech} @ {vintage} in {table} because exist_cap < {config['epsilon']}")
                curs.execute(f"""DELETE FROM {table} WHERE tech = ? AND vintage = ?""", (tech, vintage))

    # Get tech-vintage pairs from Efficiency, CostVariable, and CostFixed that do not exist in ExistingCapacity
    for table in tables[1:]:  # Skip ExistingCapacity
        tech_vintage_not_in_excap = curs.execute(
            f"""SELECT DISTINCT tech, vintage FROM {table} WHERE vintage < 2021 
                AND (tech, vintage) NOT IN (SELECT tech, vintage FROM ExistingCapacity)"""
        ).fetchall()

        # Remove these pairs from Efficiency, CostVariable, and CostFixed
        for tech, vintage in tech_vintage_not_in_excap:
            print(f"Deleted {tech} @ {vintage} in {table} because not in ExistingCapacity")
            curs.execute(f"""DELETE FROM {table} WHERE tech = ? AND vintage = ?""", (tech, vintage))
    
    tables_with_vintage = ["CostVariable", "CostInvest", "CostFixed"]
    tables_with_period = ["MaxAnnualCapacityFactor", "MinAnnualCapacityFactor"]

    # Remove tech-vintage pairs from specified tables that do not exist in Efficiency
    for table in tables_with_vintage:
        tech_vintage_not_in_efficiency = curs.execute(
            r"""SELECT DISTINCT tech, vintage FROM {} 
                WHERE tech NOT LIKE '%\_EX' ESCAPE '\' AND (tech, vintage) NOT IN (SELECT tech, vintage FROM Efficiency)""".format(table)
        ).fetchall()

        for tech, vintage in tech_vintage_not_in_efficiency:
            print(f"Deleted {tech} @ {vintage} in {table} because not in Efficiency")
            curs.execute(f"""DELETE FROM {table} WHERE tech = ? AND vintage = ?""", (tech, vintage))

    # Remove tech-period pairs from specified tables that do not exist in Efficiency
    for table in tables_with_period:
        tech_period_not_in_efficiency = curs.execute(
            r"""SELECT DISTINCT tech, periods FROM {} 
                WHERE tech NOT LIKE '%\_EX' ESCAPE '\' AND (tech, periods) NOT IN (SELECT tech, vintage FROM Efficiency)""".format(table)
        ).fetchall()

        for tech, period in tech_period_not_in_efficiency:
            print(f"Deleted {tech} @ {period} in {table} because not in Efficiency")
            curs.execute(f"""DELETE FROM {table} WHERE tech = ? AND periods = ?""", (tech, period))

    conn.commit()
    conn.close()

    print(f"Cleanup complete.\n")

"""
##################################################
    Basic parameters
##################################################
"""

def insert_template(config):
    """ 
    Imports predefined template tables into the sqlite database
    """

    tables = [
    "commodity_labels", #   CommodityType
    "currencies", # 
    "dq_estimate",
    "dq_reliability",
    "dq_completeness",
    "dq_time",
    "dq_geography",
    "dq_technology",
    "regions", #            Region
    "sector_labels", #      SectorLabel
    "technology_labels", #  TechnologyType
    "time_period_labels", # TimePeriodType
    "time_periods", #       TimePeriod
    "time_season", #        TimeSeason
    "time_of_day", #        TimeofDay
    "tech_annual", #        Includes those technologies with constant annual demand [deprecated list]
    "StorageDuration" #     Assumes 8760 hours of storage for H2 to simulate unlimited supply year-round
    ]

    # Read the specified sheets into a dictionary of dataframes
    dfs = pd.read_excel(config['template'], sheet_name=tables)

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])

    # For each table, insert the data from the corresponding dataframe
    for sheet_name, df in dfs.items():
        # Convert NaNs to None to handle SQL nulls properly
        df_clean = df.where(pd.notnull(df), None)
        df_clean.to_sql(sheet_name, conn, if_exists='replace', index=False)
            
    conn.commit()
    conn.close()

    print(f"Template tables inserted into {os.path.basename(config['database'])}\n")

def compile_ref(config):
    """
    Reads references from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'References'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Imports the table on the excel sheet and normalizes references to ASCII
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet)
    df['References'] = df['References'].apply(normalize_to_ascii)

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO "references"(reference) VALUES (?)""", (f"[Transport] {row['References']}",)) # value is treated as a single tuple containing one element by adding a comma inside the tuple
            
    conn.commit()
    conn.close()

    print(f"References compiled into {os.path.basename(config['database'])}\n")

def compile_techs(config):
    """
    Reads technologies from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'Techs'
    last_col = 'Category'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to read
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df = df.fillna('')

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute(f"""REPLACE INTO technologies(tech, flag, sector, tech_desc, tech_category, additional_notes)
                    VALUES('{row['Technology']}', '{row['Flag']}', 'Transport', '{row['Description']}', '{row['Category']}', '{row['Details']}')""")
            
    conn.commit()
    conn.close()

    print(f"Technology data compiled into {os.path.basename(config['database'])}\n")

def compile_comms(config):
    """
    Reads commodities from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'Comms'
    last_col = 'Details'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df = df.fillna('')

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute(f"""REPLACE INTO commodities(comm_name, flag, comm_desc, additional_notes)
                    VALUES('{row['Commodity']}', '{row['Flag']}', '{row['Description']}', '{row['Details']}')""")
            
    conn.commit()
    conn.close()

    print(f"Commodity data compiled into {os.path.basename(config['database'])}\n")

def compile_demand(config):
    """
    Reads demands from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'Demand'
    parameter = 'Demand'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums 
    periods = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Period', value_name=parameter, value_vars=periods)

    # Drops empty parameter rows, convert years into integers, and remove NaNs from the table
    df = df.dropna(subset=[parameter])
    df = df.fillna('')

    # Round values to the nearest precision (decimal place) and normalize references to ASCII
    df[parameter] = df[parameter].round(config['precision'])
    df['Reference'] = df['Reference'].apply(normalize_to_ascii)

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO Demand(regions, periods, demand_comm, demand, demand_units, demand_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (config['province'], row['Period'], row['Demand Commodity'], row[parameter], row['Unit'], row['Notes'], 
                    row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))

    conn.commit()
    conn.close()

    print(f"Demand data compiled into {os.path.basename(config['database'])}\n")

def compile_dsd(config):
    """
    Reads charging demand distribution from RAMP-mobility simulation results and compiles them into the .sqlite format 
    """
    sheet = 'DemandDist'
    last_col = 'Technological'
    n_demands = 3

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the metadata on the excel sheet
    metadata = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1), nrows=n_demands) # Number of demands that are affected by the dsd

    # Imports the template format of the DSD table
    dsd_template = pd.read_excel(config['template'], sheet_name = 'DemandSpecificDistribution', header=None, nrows=1).iloc[0].values.tolist()

    # Imports the charging profiles from the RAMP-mobility results
    cp = pd.read_csv(config['ldv_profile'], index_col=0)
    cp.index = pd.to_datetime(cp.index, utc=True)
    
    # Converts simulation results time series into ET time zone, resamples into hourly resolution, and normalizes distribution
    cp = cp.set_index(cp.index.tz_convert('America/Toronto'))
    cp = cp[cp.index.year == config['weather_year']]
    cp = cp.resample('H').mean()
    cp = cp/cp.sum()

    # Labels time series into the desired format
    cp['Day'] = cp.index.strftime('D%j')
    # cp['Hour'] = cp.index.strftime('H%H')

    cp.reset_index(inplace=True)
    cp.rename(columns={'index': 'Timestamp'}, inplace=True)
    cp['Hour'] = (cp['Timestamp'].dt.hour + 1).astype(str).str.zfill(2).apply(lambda x: f'H{x}') # Hour labels from H01 to H24
    cp.set_index('Timestamp', inplace=True)    
    
    # Creates DSD dataframe from the template and fills in the DSD from the RAMP-mobility results along with the metadata from the spreadsheet database
    df = pd.DataFrame(columns=dsd_template)
    df['dsd'] = cp['Charging Profile'].round(config['precision']).values # DSDs rounded to 10 decimals
    df['season_name'] = cp['Day'].values
    df['time_of_day_name'] = cp['Hour'].values

    df['demand_name'] = metadata['Target Demand'].values[0]
    df['regions'] = metadata['Region'].values[0]
    df.loc[df['time_of_day_name'] == 'H01', 'dsd_notes'] = metadata['Notes'].values[0] #    Only shown every 24th hour to reduce database size
    df.loc[df['time_of_day_name'] == 'H01', 'reference'] = metadata['Reference'].apply(normalize_to_ascii).values[0] #    Only shown every 24th hour to reduce database size
    df['data_year'] = metadata['Data Year'].astype(int).values[0]
    df['dq_rel'] = metadata['Reliability'].astype(int).values[0]
    df['dq_comp'] = metadata['Representativeness'].astype(int).values[0]
    df['dq_time'] = metadata['Temporal'].astype(int).values[0]
    df['dq_geog'] = metadata['Geographical'].astype(int).values[0]
    df['dq_tech'] = metadata['Technological'].astype(int).values[0]

    # Inserts the remaining affected demands into the dataframe
    df_merge = pd.DataFrame(columns=dsd_template)

    for i in range(1, n_demands):
        df2 = df.copy()
        df2['demand_name'] = metadata['Target Demand'].values[i]
        df_merge = pd.concat([df_merge, df2], axis=0)

    df_merged = pd.concat([df, df_merge], axis=0)

    # Convert NaNs to None to handle SQL nulls properly
    df_merged = df_merged.where(pd.notnull(df_merged), None)

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])

    # Insert the dataframe into the sqlite database
    df_merged.to_sql('DemandSpecificDistribution', conn, if_exists='replace', index=False)
            
    conn.commit()
    conn.close()

    print(f"Demand specific distributions compiled into {os.path.basename(config['database'])}\n")

"""
##################################################
    Capacity factor tech (LD EV charging demand)
##################################################
"""

def compile_cft(config):
    """
    Reads charging demand distribution from RAMP-mobility simulation results and compiles them into the .sqlite format 
    """
    # Imports the template format of the DSD table
    cft_template = pd.read_excel(config['template'], sheet_name = 'CapacityFactorTech', header=None, nrows=1).iloc[0].values.tolist()

    # Imports the charging profiles from the RAMP-mobility results
    cp = pd.read_csv(config['ldv_profile'], index_col=0)
    cp.index = pd.to_datetime(cp.index, utc=True)
    
    # Converts simulation results time series into ET time zone, resamples into hourly resolution, and normalizes distribution
    cp = cp.set_index(cp.index.tz_convert('America/Toronto'))
    cp = cp[cp.index.year == config['weather_year']]
    cp = cp.resample('H').mean()
    cp = cp/cp.max()                # normalize by the largest datapoint since the charging distribution will go to capacity factor tech

    # Labels time series into the desired format
    cp['Day'] = cp.index.strftime('D%j')
    # cp['Hour'] = cp.index.strftime('H%H')

    cp.reset_index(inplace=True)
    cp.rename(columns={'index': 'Timestamp'}, inplace=True)
    cp['Hour'] = (cp['Timestamp'].dt.hour + 1).astype(str).str.zfill(2).apply(lambda x: f'H{x}') # Hour labels from H01 to H24
    cp.set_index('Timestamp', inplace=True)    
    
    # Creates the charging dist dataframe from the template and fills in the charging dist from the RAMP-mobility results along with the metadata from the spreadsheet database
    df = pd.DataFrame(columns=cft_template)
    df['cf_tech'] = cp['Charging Profile'].round(config['precision']).values # the charging dists rounded to 10 decimals
    df['season_name'] = cp['Day'].values
    df['time_of_day_name'] = cp['Hour'].values

    df['tech'] = 'T_LDV_BEV_CHRG'
    df['regions'] = 'ON'
    cft_notes = (
        "This distribution represents the hourly variation of electricity demand from light-duty BEV charging. By using the stochastic aggregation framework RAMP-mobility to characterize "
        "daily travel needs and battery consumption, and consequently, charging loads from 2,500 vehicles. Using travel survey data from the Tomorrow Transportation Survey 2016, Ontario "
        "population-weighted temperature profiles from renewables.ninja, and other technical parameters described in the spreadsheet database."
    )
    df.loc[df['time_of_day_name'] == 'H01', 'cf_tech_notes'] = cft_notes #    Only shown every 24th hour to reduce database size
    df.loc[df['time_of_day_name'] == 'H01', 'reference'] = 'Data Management Group. (2018). Transportation Tomorrow Survey (TTS) 2016. Department of Civil Engineering, University of Toronto. https://dmg.utoronto.ca/' #    Only shown every 24th hour to reduce database size
    df['data_year'] = 2018
    df['dq_rel'] = 1
    df['dq_comp'] = 2
    df['dq_time'] = 1
    df['dq_geog'] = 1
    df['dq_tech'] = 1

    # Convert NaNs to None to handle SQL nulls properly
    df = df.where(pd.notnull(df), None)

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])

    # Insert the dataframe into the sqlite database
    df.to_sql('CapacityFactorTech', conn, if_exists='replace', index=False)
            
    conn.commit()
    conn.close()

    print(f"Capacity factor distributions compiled into {os.path.basename(config['database'])}\n")

"""
##################################################
    Lifetime
##################################################
"""

def compile_lifetime(config):
    """
    Reads lifetimes from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'Lifetime'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet and normalize references to ASCII
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df = df.fillna('')
    df['Reference'] = df['Reference'].apply(normalize_to_ascii)

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO LifetimeTech(regions, tech, life, life_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (config['province'], row['Technology'], row['Lifetime'], row['Notes'], 
                    row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))

    conn.commit()
    conn.close()

    print(f"Lifetime data compiled into {os.path.basename(config['database'])}\n")

"""
##################################################
    Existing capacity
##################################################
"""

def compile_excap(config):
    """
    Reads existing capacities from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'ExCap'
    parameter = 'ExistingCapacity'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums and drops rows with empty parameters
    vintages = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Vintage', value_name=parameter, value_vars=vintages)
    df = df.dropna(subset=[parameter])

    # Fill NaNs as empty values; allowing for consistent grouping of vintages
    df = df.fillna('')

    if config['aggregate_excap']:
        # Aggregates 2000-2020 vintages into 5-year vintages (e.g., 2002 -> 2000 and 2003 -> 2005)
        df.Vintage = df.Vintage.astype(int)
        df['qVintage'] = df.Vintage.apply(quinquennial_mapping)
        df = df.groupby([i for i in df.columns.tolist() if i not in ['Vintage', parameter]]).agg({parameter: 'sum'}).reset_index()
        df = df.rename(columns={'qVintage': 'Vintage'})

    # Round values to the nearest precision (decimal place) and normalize references to ASCII
    df[parameter] = df[parameter].round(config['precision'])
    df['Reference'] = df['Reference'].apply(normalize_to_ascii)

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO ExistingCapacity(regions, tech, vintage, exist_cap, exist_cap_units, exist_cap_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (config['province'], row['Technology'], row['Vintage'], row[parameter], row['Unit'], row['Notes'], 
                row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Existing capacity data compiled into {os.path.basename(config['database'])}\n")

"""
##################################################
    Capacity to activity
##################################################
"""

def compile_c2a(config):
    """
    Reads c2a factors from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'Cap2Act'
    last_col = 'Notes'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df = df.fillna('')

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO CapacityToActivity(regions, tech, c2a, c2a_notes)
                        VALUES(?, ?, ?, ?)""",
                    (config['province'], row['Technology'], row['Capacity to Activity'], f"[{row['Activity Unit']}/{row['Capacity Unit']}] {row['Notes']}"))

    conn.commit()
    conn.close()

    print(f"C2A factors data compiled into {os.path.basename(config['database'])}\n")

"""
##################################################
    Max/Min AnnualCapacityFactor
##################################################
"""

def compile_acf(config):
    """
    Reads annual cap factors from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'CapFactor'
    parameter = 'MaxAnnualCapFactor'
    last_col = 'Notes'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the period columns into period and parameter colums 
    periods = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Period', value_name=parameter, value_vars=periods)
    df.Period = df.Period.astype(int)
    df = df.dropna(subset=[parameter])

    # Fill NaNs as empty values
    df = df.fillna('')

    # Round values to the nearest precision (decimal place) and normalize references to ASCII
    df[parameter] = df[parameter].round(config['precision'])
    df['Reference'] = df['Reference'].apply(normalize_to_ascii)

     # Reads technologies' lifetimes and last period of exsiting technologies
    df_lifetime = pd.read_excel(config['spreadsheet'], sheet_name = 'Lifetime', skiprows=[0], usecols=['Technology', 'Lifetime'])
    period_0 = pd.read_excel(config['template'], sheet_name='time_periods')
    period_0 = period_0[period_0['flag'] == 'e'].max().values[0]

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()
    
    for _idx, row in df.iterrows():

        if row['Technology'].endswith('_EX'): #  Applies only for residual technologies
            
            # Attempt to find the lifetime for the given technology
            lifetime_rows = df_lifetime[df_lifetime.Technology == row['Technology']].Lifetime
            if len(lifetime_rows) > 0:
                lifetime = lifetime_rows.values[0]
            else:
                lifetime = 40  # Default lifetime if not specified
            
            if period_0 + lifetime <= row['Period']: continue # Checks for capacity factors outside existing technologies' lifetimes

        curs.execute("""REPLACE INTO MaxAnnualCapacityFactor(regions, periods, tech, output_comm, max_acf, max_acf_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (config['province'], row['Period'], row['Technology'], row['Output Commodity'], row[parameter], row['Notes'], 
                row['Reference'], row['Data Year'], 1, 1, dq_time(row['Data Year']), 1, 1))
        
        curs.execute("""REPLACE INTO MinAnnualCapacityFactor(regions, periods, tech, output_comm, min_acf, min_acf_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (config['province'], row['Period'], row['Technology'], row['Output Commodity'], row[parameter]*0.99, f"99% of MaxAnnualCapacityFactor for computational slack. {row['Notes']}", 
                row['Reference'], row['Data Year'], 1, 1, dq_time(row['Data Year']), 1, 1))
            
    conn.commit()
    conn.close()

    print(f"Max/min annual cap factors data compiled into {os.path.basename(config['database'])}\n")

"""
##################################################
    Efficiency
##################################################
"""

def compile_efficiency(config):
    """
    Reads efficiencies from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'Efficiency'
    parameter = 'Efficiency'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums and drops rows with empty parameters
    vintages = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Vintage', value_name=parameter, value_vars=vintages)
    df = df.dropna(subset=[parameter])

    # Fill NaNs as empty values; allowing for consistent grouping of vintages
    df = df.fillna('')

    if config['aggregate_excap']:
        # Aggregates 2000-2020 vintages into 5-year vintages (e.g., 2002 -> 2000 and 2003 -> 2005)
        df.Vintage = df.Vintage.astype(int)
        df_ex = df[df.Vintage <= 2020]
        df_new = df[df.Vintage > 2020]
        df_ex['qVintage'] = df_ex.Vintage.apply(quinquennial_mapping)
        df_ex_agg = df_ex.groupby([i for i in df_ex.columns.tolist() if i not in ['Vintage', parameter]]).agg({parameter: 'min'}).reset_index()
        df_ex_agg = df_ex_agg.rename(columns={'qVintage': 'Vintage'})
        df = pd.concat([df_ex_agg, df_new], ignore_index=True).reset_index(drop=True)

    # Round values to the nearest precision (decimal place) and normalize references to ASCII
    df[parameter] = df[parameter].round(config['precision'])
    df['Reference'] = df['Reference'].apply(normalize_to_ascii)

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (config['province'], row['Input Commodity'], row['Technology'], row['Vintage'], row['Output Commodity'], row[parameter], f"[{row['Unit']}] {row['Notes']}", 
                row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Efficiency data compiled into {os.path.basename(config['database'])}\n")

"""
##################################################
    Investment costs
##################################################
"""

def compile_costinvest(config):
    """
    Reads investment costs from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'CostInvest'
    parameter = 'CostInvest'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums 
    vintages = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Vintage', value_name=parameter, value_vars=vintages)

    # Drops empty parameter rows, convert years into integers, and remove NaNs from the table
    df = df.dropna(subset=[parameter])
    df = df.fillna('')

    # Round values to the nearest precision (decimal place) and normalize references to ASCII
    df[parameter] = df[parameter].round(config['precision'])
    df['Reference'] = df['Reference'].apply(normalize_to_ascii)

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO CostInvest(regions, tech, vintage, cost_invest, cost_invest_units, cost_invest_notes, data_cost_invest, data_cost_year, data_curr,
                     reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (config['province'], row['Technology'], row['Vintage'], row[parameter], f"{int(row['Currency Year'])} {row['Currency']} ({row['Unit']})", row['Notes'], 
                 round(row[parameter]/row['Conversion Factor'], config['precision']), row['Original Currency Year'], row['Original Currency'],
                row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Investment cost data compiled into {os.path.basename(config['database'])}\n")

"""
##################################################
    Variable costs
##################################################
"""

def compile_costvariable(config):
    """
    Reads variable costs from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'CostVariable'
    parameter = 'CostVariable'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
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

    # Round values to the nearest precision (decimal place) and normalize references to ASCII
    df[parameter] = df[parameter].round(config['precision'])
    df['Reference'] = df['Reference'].apply(normalize_to_ascii)

    # Reads technologies' lifetimes
    df_lifetime = pd.read_excel(config['spreadsheet'], sheet_name = 'Lifetime', skiprows=[0], usecols=['Technology', 'Lifetime'])

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()
    
    for _idx, row in df.iterrows():

        # Attempt to find the lifetime for the given technology
        lifetime_rows = df_lifetime[df_lifetime.Technology == row['Technology']].Lifetime
        if len(lifetime_rows) > 0:
            lifetime = lifetime_rows.values[0]
        else:
            lifetime = 40  # Default lifetime if not specified

        if row['Period'] < row['Vintage'] or row['Vintage'] + lifetime <= row['Period']: continue # Checks for var costs outside the expected technology's lifetime

        curs.execute("""REPLACE INTO CostVariable(regions, periods, tech, vintage, cost_variable, cost_variable_units, cost_variable_notes, data_cost_variable, data_cost_year, data_curr,
                     reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (config['province'], row['Period'], row['Technology'], row['Vintage'], row[parameter], f"{int(row['Currency Year'])} {row['Currency']} ({row['Unit']})", row['Notes'], 
                 round(row[parameter]/row['Conversion Factor'], config['precision']), row['Original Currency Year'], row['Original Currency'],
                row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Variable cost data compiled into {os.path.basename(config['database'])}\n")

"""
##################################################
    Fixed costs
##################################################
"""

def compile_costfixed(config):
    """
    Reads fixed costs from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'CostFixed'
    parameter = 'CostFixed'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
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

    # Round values to the nearest precision (decimal place) and normalize references to ASCII
    df[parameter] = df[parameter].round(config['precision'])
    df['Reference'] = df['Reference'].apply(normalize_to_ascii)

    # Reads technologies' lifetimes
    df_lifetime = pd.read_excel(config['spreadsheet'], sheet_name = 'Lifetime', skiprows=[0], usecols=['Technology', 'Lifetime'])

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()
    
    for _idx, row in df.iterrows():
        
        # Attempt to find the lifetime for the given technology
        lifetime_rows = df_lifetime[df_lifetime.Technology == row['Technology']].Lifetime
        if len(lifetime_rows) > 0:
            lifetime = lifetime_rows.values[0]
        else:
            lifetime = 40  # Default lifetime if not specified
            
        if row['Period'] < row['Vintage'] or row['Vintage'] + lifetime <= row['Period']: continue # Checks for fixed costs outside the expected technology's lifetime

        curs.execute("""REPLACE INTO CostFixed(regions, periods, tech, vintage, cost_fixed, cost_fixed_units, cost_fixed_notes, data_cost_fixed, data_cost_year, data_curr,
                     reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (config['province'], row['Period'], row['Technology'], row['Vintage'], row[parameter], f"{int(row['Currency Year'])} {row['Currency']} ({row['Unit']})", row['Notes'], 
                 round(row[parameter]/row['Conversion Factor'], config['precision']), row['Original Currency Year'], row['Original Currency'],
                row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Fixed cost data compiled into {os.path.basename(config['database'])}\n")

"""
##################################################
    Emission Activity
##################################################
"""

def compile_emissionact(config):
    """
    Reads emission factors from activities from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'EmissionAct'
    parameter = 'EmissionActivity'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums and drops rows with empty parameters
    vintages = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Vintage', value_name=parameter, value_vars=vintages)
    df = df.dropna(subset=[parameter])

    # Fill NaNs as empty values; allowing for consistent grouping of vintages
    df = df.fillna('')

    # Round values to the nearest precision (decimal place) and normalize references to ASCII
    df[parameter] = df[parameter].round(config['precision'])
    df['Reference'] = df['Reference'].apply(normalize_to_ascii)

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        if row['Emission Commodity'] in ['ch4', 'n2o'] and config['convert_emission_units']:
            curs.execute("""REPLACE INTO EmissionActivity(regions, emis_comm, input_comm, tech, vintage, output_comm, emis_act, emis_act_units, emis_act_notes, 
                        reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (config['province'], row['Emission Commodity'], row['Input Commodity'], row['Technology'], row['Vintage'], row['Output Commodity'], row[parameter]*1000, row['Unit'].replace('kt', 't'), row['Notes'], 
                        row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
        else:
            curs.execute("""REPLACE INTO EmissionActivity(regions, emis_comm, input_comm, tech, vintage, output_comm, emis_act, emis_act_units, emis_act_notes, 
                        reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (config['province'], row['Emission Commodity'], row['Input Commodity'], row['Technology'], row['Vintage'], row['Output Commodity'], row[parameter], row['Unit'], row['Notes'], 
                        row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Emission factors from activity data compiled into {os.path.basename(config['database'])}\n")

"""
##################################################
    Emission Embodied
##################################################
"""

def compile_emissionemb(config):
    """
    Reads emission factors from capacities from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'EmissionEmb'
    parameter = 'EmissionEmbodied'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Copies the values from 2021 to re-create vintages for 2025-2050
    df['2025'], df['2030'], df['2035'], df['2040'], df['2045'], df['2050'] = df['2021'], df['2021'], df['2021'], df['2021'], df['2021'], df['2021']

    # Melts the vintage columns into vintage and parameter colums and drops rows with empty parameters
    vintages = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Vintage', value_name=parameter, value_vars=vintages)
    df = df.dropna(subset=[parameter])

    # Fill NaNs as empty values; allowing for consistent grouping of vintages
    df = df.fillna('')

    # Round values to the nearest precision (decimal place) and normalize references to ASCII
    df[parameter] = df[parameter].round(config['precision'])
    df['Reference'] = df['Reference'].apply(normalize_to_ascii)

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    # Creates the EmissionEmbodied table
    # curs.execute("""CREATE TABLE EmissionEmbodied(
    #                 regions      TEXT,
    #                 emis_comm   TEXT
    #                     REFERENCES commodities (comm_name),
    #                 tech        TEXT
    #                     REFERENCES technologies (tech),
    #                 vintage     INTEGER
    #                     REFERENCES time_periods (t_periods),
    #                 value       REAL,
    #                 units       TEXT,
    #                 notes       TEXT, reference, data_year, data_flags, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech, additional_notes,
    #                 PRIMARY KEY (regions, emis_comm, tech, vintage))""")

    for _idx, row in df.iterrows():
        if row['Emission Commodity'] in ['ch4', 'n2o'] and config['convert_emission_units']:
            curs.execute("""REPLACE INTO EmissionEmbodied(regions, emis_comm, tech, vintage, value, units, notes, 
                        reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (config['province'], row['Emission Commodity'], row['Technology'], row['Vintage'], row[parameter]*1000, row['Unit'].replace('kt', 't'), row['Notes'], 
                        row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
        else:
            curs.execute("""REPLACE INTO EmissionEmbodied(regions, emis_comm, tech, vintage, value, units, notes, 
                        reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (config['province'], row['Emission Commodity'], row['Technology'], row['Vintage'], row[parameter], row['Unit'], row['Notes'], 
                        row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Emission factors from capacity data compiled into {os.path.basename(config['database'])}\n")

"""
##################################################
    Tech input split
##################################################
"""

def compile_techinputsplit(config):
    """
    Reads tech input commodity splits from the .xlsx file and compiles them into .sqlite format 
    """
    sheet = 'InputSplit'
    parameter = 'TechInputSplit'
    last_col = 'Technological'

    if sheet not in pd.ExcelFile(config['spreadsheet']).sheet_names:
        return None
    
    # Reads excel sheet columns and limits the number of columns to the last DQI
    cols = pd.read_excel(config['spreadsheet'], sheet_name = sheet, header=None, skiprows=[0], nrows=1).iloc[0].values.tolist()
    ncols = cols.index(last_col) #    Last column to read

    # Imports the table on the excel sheet
    df = pd.read_excel(config['spreadsheet'], sheet_name = sheet, skiprows=[0], usecols=range(ncols + 1))
    df.columns = df.columns.astype(str)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Melts the vintage columns into vintage and parameter colums and drops rows with empty parameters
    periods = [col for col in df.columns if col.isdigit()]
    params = [col for col in df.columns if not col.isdigit()]
    df = pd.melt(df, id_vars=params, var_name='Period', value_name=parameter, value_vars=periods)
    df = df.dropna(subset=[parameter])

    # Fill NaNs as empty values
    df = df.fillna('')

    # Round values to the nearest precision (decimal place) and normalize references to ASCII
    df[parameter] = df[parameter].round(config['precision'])
    df['Reference'] = df['Reference'].apply(normalize_to_ascii)

    # Connect with database and replace parameters
    conn = sqlite3.connect(config['database'])
    curs = conn.cursor()

    for _idx, row in df.iterrows():
        curs.execute("""REPLACE INTO TechInputSplit(regions, periods, input_comm, tech, ti_split, ti_split_notes, reference, data_year, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (config['province'], row['Period'], row['Input Commodity'], row['Technology'], row[parameter], row['Notes'], 
                row['Reference'], row['Data Year'], row['Reliability'], row['Representativeness'], dq_time(row['Data Year']), row['Geographical'], row['Technological']))
            
    conn.commit()
    conn.close()

    print(f"Tech input commodity split data compiled into {os.path.basename(config['database'])}\n")

def update_cost_variable_entries(config):
    # Connect to the SQLite database
    conn = sqlite3.connect(config['database'])

    # Fetch data from ExistingCapacity and CostVariable tables
    existing_capacity_df = pd.read_sql_query("SELECT tech, vintage FROM ExistingCapacity", conn)
    cost_variable_df = pd.read_sql_query("SELECT tech, vintage FROM CostVariable", conn).drop_duplicates()

    # Define thresholds for vintage
    vintage_thresholds = [2005, 2010, 2015, 2020]

    # Loop over each vintage threshold
    for threshold in vintage_thresholds:
        # Find all tech-vintage pairs missing in CostVariable for the current threshold
        missing_pairs = existing_capacity_df.merge(cost_variable_df, on=['tech', 'vintage'], how='left', indicator=True)
        missing_pairs = missing_pairs[(missing_pairs['_merge'] == 'left_only') & (missing_pairs['vintage'] <= threshold)].drop(columns=['_merge'])

        # Loop through each missing pair to find and insert corresponding entries
        for _, missing_row in missing_pairs.iterrows():
            tech = missing_row['tech']
            vintage = missing_row['vintage']

            # Find the closest matching vintage in CostVariable that is greater than or equal to the current vintage
            matched_rows = pd.read_sql_query(f"""
                SELECT * FROM CostVariable
                WHERE tech = '{tech}' AND vintage >= {vintage}
                ORDER BY vintage ASC LIMIT 1
            """, conn)
            
            if not matched_rows.empty:
                matched_vintage = matched_rows['vintage'].iloc[0]
                matched_cost_variable_entries = pd.read_sql_query(f"""
                    SELECT * FROM CostVariable
                    WHERE tech = '{tech}' AND vintage = {matched_vintage}
                """, conn)

                # Prepare new rows for the missing pair using the matched entries
                new_rows = []
                for _, entry_row in matched_cost_variable_entries.iterrows():
                    # Check if the entry already exists
                    existing_entry = pd.read_sql_query(f"""
                        SELECT COUNT(*) as count FROM CostVariable
                        WHERE tech = '{tech}' AND vintage = {vintage} AND periods = {entry_row['periods']}
                    """, conn)
                    
                    if existing_entry['count'].iloc[0] == 0:
                        new_row = entry_row.copy()
                        new_row['vintage'] = vintage
                        new_row['cost_variable_notes'] = 'Assumed the same value as the next quinquennium vintage (e.g., 2019 -> 2020)'
                        new_rows.append(new_row)
                
                # Convert new rows to DataFrame and insert them
                if new_rows:
                    new_rows_df = pd.DataFrame(new_rows)
                    new_rows_df.to_sql('CostVariable', conn, if_exists='append', index=False)

    # Verify the insertion
    new_entries_count = pd.read_sql_query("SELECT COUNT(*) FROM CostVariable WHERE cost_variable_notes='Inserted by script based on threshold match'", conn)
    print(f"Inserted {new_entries_count['COUNT(*)'].iloc[0]} new entries into the CostVariable table.")

    # Close the connection
    conn.close()


"""
##################################################
    Compile all parameters
##################################################
"""

def compile_transport(
    province: str = 'ON',  # Default province, can be overridden by the user
    spreadsheet_name_format: str = 'CANOE_TRN_<r>_v4',
    db_name_format: str = 'canoe_trn_<r>_vanilla4',
    ldv_profile_name: str = 'ON-2016TTS_no-we_2018_v4_2023-batteries',
    # ON-2016TTS_no-we_2018_v4_2023-batteries
    # ON-2022NHTS_2018_v4_2023-batteries
    dir_path = None,
    # RAMP-mobility simulation results to compile
    weather_year = 2018,
    charging_dsd = False,       # choose whether to represent LD EV charging demand distribution in the DSD (True) or CFT (False) Temoa tables
    # Aggregate existing capacities and efficiencies into 5-year vintages
    aggregate_excap = True,
    # Create EmissionEmbodied table
    create_emission_embodied = False,
    # Convert CH4 and N2O units of ktonnes into tonnes (to harmonize with other sectors)
    convert_emission_units = True,
    # Define the precision of the model parameters
    epsilon = 1e-4,  # For cleaning existing capacities that are too small
    precision = 9,   # For consistent precision across the model
    # Rewrite database from scratch if it already exists
    wipe_database = True
):
    """
    Runs all compiling functions
    """

    spreadsheet_name = spreadsheet_name_format.replace('<r>', province)
    db_name = db_name_format.replace('<r>', province.lower())

    # Update these paths according to your files' locations
    if dir_path is None:
        dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
    
    database = dir_path + 'compiled_database/' + db_name + '.sqlite'
    schema = dir_path + '../canoe_schema.sql'
    template = dir_path + 'canoe_trn_template.xlsx'

    # Spreadsheet database to compile
    spreadsheet = dir_path + 'spreadsheet_database/' + spreadsheet_name + '.xlsx'

    # RAMP-mobility simulation results to compile
    ldv_profile = dir_path + '../charging_profiles/ramp_mobility/results/' + ldv_profile_name + '.csv'

    # Packing all parameters into a config dictionary to be passed to functions below
    config = dict(
        province=province,
        spreadsheet=spreadsheet,
        database=database,
        schema=schema,
        template=template,
        ldv_profile=ldv_profile,
        weather_year=weather_year,
        charging_dsd=charging_dsd,
        aggregate_excap=aggregate_excap,
        create_emission_embodied=create_emission_embodied,
        convert_emission_units=convert_emission_units,
        epsilon=epsilon,
        precision=precision,
        wipe_database=wipe_database
    )

    # print for checking
    [print(k, ':', v) for k, v in config.items()]
    
    instantiate_database(config)

    insert_template(config)
    compile_ref(config)
    compile_techs(config)
    compile_comms(config)
    compile_demand(config)

    if charging_dsd: compile_dsd(config)
    else: compile_cft(config)

    compile_lifetime(config)
    compile_excap(config)
    compile_c2a(config)
    compile_acf(config)
    compile_efficiency(config)
    compile_costinvest(config)
    compile_costvariable(config)
    compile_costfixed(config)
    compile_emissionact(config)

    if create_emission_embodied: compile_emissionemb(config)

    compile_techinputsplit(config)

    if not aggregate_excap: update_cost_variable_entries(config)

    cleanup(config)

    print(f"All parameter data from {os.path.basename(spreadsheet)} compiled into {os.path.basename(database)}\n")

if __name__ == "__main__":
    compile_transport()