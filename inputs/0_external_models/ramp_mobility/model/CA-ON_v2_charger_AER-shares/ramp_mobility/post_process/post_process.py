# -*- coding: utf-8 -*-

#%% Import required libraries
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytz
import copy
import matplotlib.ticker as mtick
from pathlib import Path
import pickle
import os
from ramp_mobility.utils import tot_users_calc, tot_battery_cap_calc


# from initialise import tot_users_calc, tot_battery_cap_calc
import datetime as dt
#import enlopy as el

#%% Post-processing
'''
Just some additional code lines to calculate useful indicators and generate plots
'''
def Profile_formatting(stoch_profiles):
    Profile_avg = np.zeros(1440)
    for pr in stoch_profiles:
        Profile_avg = Profile_avg + pr
    Profile_avg = Profile_avg/len(stoch_profiles)
    
    Profile_kW = []
    for kW in stoch_profiles: 
        Profile_kW.append(kW/1000)
    
    # Profile_series = np.array([])
    # for iii in stoch_profiles:
    #     Profile_series = np.append(Profile_series,iii)
    Profile_series = np.array([])
    Profile_series = np.hstack(stoch_profiles)

    return (Profile_avg, Profile_kW, Profile_series)

def Profiles_user_formatting(stoch_profiles):
    Profiles_user_format = {}
    for us_type in stoch_profiles[0]:
        Profiles_user_format[us_type] = []
        for day in range(len(stoch_profiles)):
            try:
                temp = np.stack(stoch_profiles[day][us_type], axis=-1)
            except:
                temp = np.zeros((1440,1))
            Profiles_user_format[us_type].append(temp)
        Profiles_user_format[us_type] = np.vstack(Profiles_user_format[us_type])
    return Profiles_user_format

def Usage_formatting(stoch_profiles):
    Usage_avg = np.zeros(1440)
    for pr in stoch_profiles:
        Usage_avg = Usage_avg + pr
    Usage_avg = Usage_avg/len(stoch_profiles)
      
    # Usage_series = np.array([])
    # for iii in stoch_profiles:
    #     Usage_series = np.append(Usage_series,iii)
    
    Usage_series = np.array([])
    Usage_series = np.hstack(stoch_profiles)

    return (Usage_avg, Usage_series)

def Profile_cloud_plot(stoch_profiles,stoch_profiles_avg):
    #x = np.arange(0,1440,5)
    plt.figure(figsize=(10,5))
    for n in stoch_profiles:
        plt.plot(np.arange(1440),n,'#b0c4de')
        plt.xlabel('Time [h])')
        plt.ylabel('Power [W]')
        plt.title('Cloud plot')
        plt.ylim(ymin=0)
        #plt.ylim(ymax=5000)
        plt.margins(x=0)
        plt.margins(y=0)
    plt.plot(np.arange(1440),stoch_profiles_avg,'#4169e1')
    plt.xticks([0,240,480,(60*12),(60*16),(60*20),(60*24)],[0,4,8,12,16,20,24])
    #plt.savefig('profiles.eps', format='eps', dpi=1000)
    plt.show()

def Profile_series_plot(stoch_profiles_series):
    #x = np.arange(0,1440,5)
    plt.figure(figsize=(10,5))
    plt.plot(np.arange(len(stoch_profiles_series)),stoch_profiles_series, '#4169e1')    #plt.xlabel('Time (hours)')
    plt.ylabel('Power (W)')
    plt.ylim(ymin=0)
    #plt.ylim(ymax=5000)
    plt.margins(x=0)
    plt.margins(y=0)
    #plt.xticks([0,240,480,(60*12),(60*16),(60*20),(60*24)],[0,4,8,12,16,20,24])
    #plt.savefig('profiles.eps', format='eps', dpi=1000)
    plt.show()

def Usage_series_plot(stoch_profiles_series):
    #x = np.arange(0,1440,5)
    plt.figure(figsize=(10,5))
    plt.plot(np.arange(len(stoch_profiles_series)),stoch_profiles_series)    #plt.xlabel('Time (hours)')
    plt.ylabel('Usage ')
    plt.ylim(ymin=0)
    #plt.ylim(ymax=5000)
    plt.margins(x=0)
    plt.margins(y=0)
    #plt.xticks([0,240,480,(60*12),(60*16),(60*20),(60*24)],[0,4,8,12,16,20,24])
    #plt.savefig('profiles.eps', format='eps', dpi=1000)
    plt.show()

def Profile_df_plot(Profile_df, year, country, start = '01-01 00:00:00', end = '12-31 23:59:00'):
    
    start_plot = str(year) + ' ' + start
    end_plot = str(year) + ' ' + end

    Profiles_df_plot = Profile_df[start_plot : end_plot]/1000   #Convert to kW
    
    figsize = (10,5)
    ax = Profiles_df_plot.plot(kind='line', color='blue', rot=0, fontsize=15, legend=False, figsize = figsize)
    ax.set_ylabel('Power [kW]', fontsize = 15)
    ax.set_title("Transport Demand Profile - " + country, fontsize = 15) 
    
    return ax

def Charging_Profile_df_plot(Profile_df, year, country, color = 'blue', start = '01-01 00:00:00', end = '12-31 23:59:00'):
    
    start_plot = str(year) + ' ' + start
    end_plot = str(year) + ' ' + end

    Profiles_df_plot = Profile_df[start_plot : end_plot]   
    
    figsize = (10,5)
    ax = Profiles_df_plot.plot(kind='line', color=color, rot=0, fontsize=15, legend=False, figsize = figsize)
    ax.set_ylabel('Power [kW]', fontsize = 15)
    ax.set_title("Charging Demand Profile - " + country, fontsize = 15) 
    
    return ax

def Comparison_plot(Profile_df, Charging_Profile_df, year, country, start = '01-01 00:00:00', end = '12-31 23:59:00'):
    
    start_plot = str(year) + ' ' + start
    end_plot = str(year) + ' ' + end

    Profiles_df_plot = Profile_df[start_plot : end_plot]/1000
    Charging_Profiles_df_plot = Charging_Profile_df[start_plot : end_plot]   

    figsize = (10,5)
    
    ax = Profiles_df_plot.plot(kind='line', color='royalblue', rot=0, fontsize=15, alpha = 0.7, legend=True, figsize = figsize)
    ax = Charging_Profiles_df_plot.plot(kind='line', color='orange', ax = ax, rot=0, fontsize=15, alpha = 0.7, legend=True, figsize = figsize)
    
    ax.set_ylabel('Power [kW]', fontsize = 15)
    ax.set_title("Comparison of Charging and Transport Profiles - " + country, fontsize = 15) 

    return ax

def Usage_df_plot(Usage_df, year, country, User_list, start = '01-01 00:00:00', end = '12-31 23:59:00'):
    
    tot_users = tot_users_calc(User_list)
    
    start_plot = str(year) + ' ' + start
    end_plot = str(year) + ' ' + end

    # Plot of the Usage in percentage of the total population
    Usage_df_plot = Usage_df[start_plot : end_plot]  #Divide by 10 because a value of 10 is assigned for each user to avoid the filter 

    figsize = (10,5)
    ax = ((Usage_df_plot/tot_users)*100).plot(kind='line', color= 'orange', rot=0, fontsize=15, legend=False, figsize = figsize)
    ax.set_ylabel('Usage [% of Total Users]', fontsize = 15)
    ax.set_title("Usage Profile - " + country, fontsize = 15)     
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))

    return ax

def Profile_dataframe(Profiles_series, year):
    
    minutes = pd.date_range(start=str(year) + '-01-01', periods = len(Profiles_series), freq='T')
    
    if Profiles_series.ndim == 1:
        Profiles_df = pd.DataFrame(Profiles_series, columns = ['Mobility Profile'])
    else: 
        Profiles_df = pd.DataFrame(Profiles_series)   
    
    Profiles_df.set_index(minutes, inplace = True)
   
    return Profiles_df

def Ch_Profile_df(Profiles_series, year):
    
    minutes = pd.date_range(start=str(year) + '-01-01', periods = len(Profiles_series), freq='T')
    
    if Profiles_series.ndim == 1:
        Profiles_df = pd.DataFrame(Profiles_series, columns = ['Charging Profile'])
    else: 
        Profiles_df = pd.DataFrame(Profiles_series)   
    
    Profiles_df.set_index(minutes, inplace = True)
   
    return Profiles_df

def AF_dataframe(Profiles_series, year):
    
    minutes = pd.date_range(start=str(year) + '-01-01', periods = len(Profiles_series), freq='T')
    
    if Profiles_series.ndim == 1:
        Profiles_df = pd.DataFrame(Profiles_series, columns = ['Availability Factor'])
    else: 
        Profiles_df = pd.DataFrame(Profiles_series)   
    
    Profiles_df.set_index(minutes, inplace = True)
   
    return Profiles_df

def Profile_user_dataframe(Profiles_user, year):
    
    minutes = pd.date_range(start=str(year) + '-01-01', periods = len(Profiles_user[list(Profiles_user.keys())[0]]), freq='T')
    
    Profiles_user_df = {}
    
    for us_type in Profiles_user.keys():
        Profiles_user_df[us_type] = pd.DataFrame(Profiles_user[us_type])
        Profiles_user_df[us_type].set_index(minutes, inplace = True)
       
    return Profiles_user_df

def Usage_dataframe(Profiles_series, year):
    
    minutes = pd.date_range(start=str(year) + '-01-01', periods = len(Profiles_series), freq='min')
    
    Profiles_df = pd.DataFrame(Profiles_series, columns = ['Usage'])
    Profiles_df.set_index(minutes, inplace = True)
   
    return Profiles_df

def temp_import(country, year, inputfile_temp = r"..\database\temp_ninja_pop.csv"):
    """Read source-native hourly UTC timestamps and hold each temperature for its hour."""
    weather = pd.read_csv(inputfile_temp, usecols=['time', country])
    weather.index = pd.to_datetime(weather.pop('time'), utc=True, errors='raise')
    if weather.index.has_duplicates or not weather.index.is_monotonic_increasing:
        raise ValueError('Weather timestamps must be unique and ordered')
    start = pd.Timestamp(year=year - 1, month=1, day=1, tz='UTC')
    end = pd.Timestamp(year=year + 2, month=1, day=1, tz='UTC')
    weather = weather.loc[(weather.index >= start) & (weather.index < end)]
    hours = pd.date_range(start, end, freq='h', inclusive='left')
    if not weather.index.equals(hours) or not np.isfinite(weather.to_numpy()).all():
        raise ValueError('Weather must cover the three-year working interval with finite hourly values')
    minutes = pd.date_range(start, end, freq='min', inclusive='left')
    return weather.reindex(minutes, method='ffill')


def _weather_coefficients(temp_profile, timestamps):
    """Look up UTC weather without relabelling or mutating the shared source frame."""
    if not isinstance(temp_profile.index, pd.DatetimeIndex) or temp_profile.index.tz is None:
        raise ValueError('Weather requires a timezone-aware UTC index')
    if timestamps.tz is None:
        raise ValueError('Weather lookup requires timezone-aware timestamps')
    temperatures = temp_profile.reindex(timestamps.tz_convert('UTC')).iloc[:, 0].to_numpy()
    if not np.isfinite(temperatures).all():
        raise ValueError('Weather is missing at a required simulation minute')
    return np.where(temperatures < 15, 1.12 - 0.01 * temperatures,
                    np.where(temperatures > 20, 0.63 + 0.02 * temperatures, 1.0))


def Profile_temp(Profiles_df, temp_profile, year=2016):
    coefficients = _weather_coefficients(temp_profile, Profiles_df.index)
    return Profiles_df.mul(coefficients, axis=0)


def Profile_temp_users(Profiles_user, temp_profile, year=2016, dummy_days=1,
                       timezone='America/Toronto', inplace=False):
    """Keep simulation arrays in local clock order; convert only weather lookup times.

    The legacy simulator creates each wall-clock minute once. For weather only,
    nonexistent spring times use 03:00 and repeated fall times use standard time.
    Final export retains its existing Toronto/DST convention.
    """
    start_day = dt.datetime(year, 1, 1) - dt.timedelta(days=dummy_days)
    n_periods = len(Profiles_user['Working - Large car'])
    local_minutes = pd.date_range(start=start_day, periods=n_periods, freq='min')
    lookup_times = local_minutes.tz_localize(timezone, nonexistent='shift_forward', ambiguous=False)
    coefficients = _weather_coefficients(temp_profile, lookup_times)
    result = Profiles_user if inplace else {}
    for user, values in Profiles_user.items():
        if inplace:
            np.multiply(values, coefficients[:, None], out=values)
            if isinstance(values, np.memmap):
                values.flush()
        else:
            result[user] = values * coefficients[:, None]
    return result


def Time_correction(df, country, year):
    
    df_c = copy.deepcopy(df)   
    
    if country == 'EL':
        country = 'GR'
    if country == 'UK':
        country = 'GB'
        
    ######################################## the 6th entry of 'CA' corresponds to 'America/Toronto' #################################################################
        
    timezone = 'America/Toronto' if country == 'CA' else pytz.country_timezones[country][0]
    ind = df_c.index.tz_localize(timezone, nonexistent = 'NaT', ambiguous='NaT')
    
    ind_utc = ind.tz_convert('utc')
    temp_utc = df_c.set_index(ind_utc)
    
    ind_year = pd.date_range(start=str(year) + '-01-01', end=str(max(temp_utc.index).date()) + ' 23:59:00', freq = ind.to_series().diff().min(), tz = 'utc')
    temp_year = pd.DataFrame([np.nan] * len(ind_year), index = ind_year)
    
    df_utc_final = temp_utc.join(temp_year, how='outer')
    df_utc_final = df_utc_final.dropna(axis=1, how='all')
    df_utc_final = df_utc_final.loc[df_utc_final.index.notnull()]
    df_utc_final = df_utc_final.ffill()
    df_utc_final = df_utc_final[df_utc_final.index >= str(year) + '-01-01']
    
    df_utc_final = pd.DataFrame(df_utc_final)
    
    return df_utc_final

def Resample(df):
    
    df = df.resample('H').mean()
    
    return df

def Charging_user_formatting(Ch_dict, dummy_days):
    
    Charging_dict = {}
    dummy_minutes = dummy_days * 1440
    
    for key in Ch_dict:
        Charging_dict[key] = np.transpose(np.vstack(Ch_dict[key]))
        Charging_dict[key] = Charging_dict[key][dummy_minutes:-dummy_minutes, :]
    
    return Charging_dict
    
def Availability_factors(en_system, User_list, security_margin = 0.5):
    
    tot_Battery_cap_min = tot_battery_cap_calc(User_list) * 60 
    
    AF = (en_system * security_margin) / tot_Battery_cap_min
    
    return AF


# Function to calculate the Availability factors, only with the "Perfect Foresight" charging strategy
#     if af_calculation: 
#         AF = pp.Availability_factors(en_sys_tot, User_list, security_margin = 0.5)
# #            del en_sys_tot

#         # Create AF dataframe
#         AF_df = pp.AF_dataframe(AF, year) 
# #            del AF
        
#         AF_df_utc = pp.Time_correction(AF_df, country, year) 
# #            del AF_df

#         pp.export_csv('Availability Factors', AF_df_utc, inputfile, simulation_name)

# ax = pp.Charging_Profile_df_plot(AF_df, start = '01-01 00:00:00', end = '12-31 23:59:00', year = year, country = country)
# ax.set_title('AF - Perfect Foresight charging mode')
# ax.set_ylabel('')

#%% Export individual profiles
'''
for i in range (len(Profile)):
    np.save('p0%d.npy' % (i), Profile[i])
'''

# Export Profiles

def export_csv(filename, variable, inputfile, simulation_name):
    
    if simulation_name:
        simulation = f'/{simulation_name}/'
    else:
        simulation = '/'
        
    folder = str(Path(os.environ.get('RAMP_RESULTS_DIR', '../results')) / inputfile) + simulation 
    Path(folder).mkdir(parents=True, exist_ok=True) 
    variable.to_csv(f'{folder}{filename}.csv')
    
def export_pickle(filename, variable, inputfile, simulation_name):
    
    if simulation_name:
        simulation = f'/{simulation_name}/'
    else:
        simulation = '/'

    folder = str(Path(os.environ.get('RAMP_RESULTS_DIR', '../results')) / inputfile) + simulation  
    Path(folder).mkdir(parents=True, exist_ok=True) 

    file = open(f'{folder}{filename}.pkl','wb')
    pickle.dump(variable, file, protocol=4)
    file.close()
    