# -*- coding: utf-8 -*-
"""
Created on Mon Feb 3 09:30:00 2020
This is the code for the open-source stochastic model for the generation of 
electric vehicles load profiles in Europe, called RAMP-mobility.

@authors:
- Andrea Mangipinto, Politecnico di Milano
- Francesco Lombardi, TU Delft
- Francesco Sanvito, Politecnico di Milano
- Sylvain Quoilin, KU Leuven
- Emanuela Colombo, Politecnico di Milano

Copyright 2020 RAMP, contributors listed above.
Licensed under the European Union Public Licence (EUPL), Version 1.2;
you may not use this file except in compliance with the License.

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations
under the License.
"""

#%% Import required modules

from .core_model.stochastic_process_mobility import Stochastic_Process_Mobility
from .core_model.charging_process import Charging_Process
from .post_process import post_process as pp
from .core_model.profile_store import close_profile_store

import pandas as pd
from datetime import datetime

def main(*, year, charging_mode, logistic, infr_prob, Ch_stations,
         simulation_name, temperature_file, plots=False, profile_store=None):
    Profiles_user = None
    try:
        # Set code starting time, to compute execution time
        startTime = datetime.now()

        '''
        Calls the stochastic process and saves the result in a list of stochastic profiles [kW]
        In this default example, the model runs for 1 input files ("IT"),
        but single or multiple files can be run increasing the list of countries to be run
        and naming further input files with corresponding country code
        '''
        #%% Inputs definition ############################### 2020 #################################

        charging = True         # True or False to select to activate the calculation of the charging profiles 
        write_variables = True  # Choose to write variables to csv
        full_year = True       # Choose if simulating the whole year (True) or not (False), if False, the console will ask how many days should be simulated.

        countries = ['CA']

        for c in countries:
            # Define folder where results are saved, it will be:
            # "results/inputfile/simulation_name" leave simulation_name False (or "")
            # to avoid the creation of the additional folder
            inputfile = f'North America/{c}'
    
            # Define country and year to be considered when generating profiles
            country = f'{c}'
    
            # Define attributes for the charging profiles
    
            #inputfile for the temperature data: 
            inputfile_temp = str(temperature_file)
    
            ## If simulating the RES Integration charging strategy, a file with the residual load curve should be included in the folder
            try:
                inputfile_residual_load = fr"..\database\residual_load\residual_load_{c}.csv"
                residual_load = pd.read_csv(inputfile_residual_load, index_col = 0)
            except FileNotFoundError:      
                residual_load = pd.DataFrame(0, index=range(1), columns=range(1))
    
    
            #%% Call the functions for the simulation
    
            # Simulate the mobility profile 
            (Profiles_list, Usage_list, User_list, Profiles_user_list, dummy_days
             ) = Stochastic_Process_Mobility(inputfile, country, year, full_year, profile_store=profile_store)
    
            # Post-processes the results and generates plots
            Profiles_avg, Profiles_list_kW, Profiles_series = pp.Profile_formatting(
                Profiles_list)
            Usage_avg, Usage_series = pp.Usage_formatting(Usage_list)
            Profiles_user = (Profiles_user_list if isinstance(Profiles_user_list, dict)
                             else pp.Profiles_user_formatting(Profiles_user_list))
            del Profiles_user_list
    
            # If more than one daily profile is generated, also cloud plots are shown
            if plots and len(Profiles_list) > 1:
                pp.Profile_cloud_plot(Profiles_list, Profiles_avg)
    
            # Create a dataframe with the profile
            Profiles_df = pp.Profile_dataframe(Profiles_series, year) 
            Usage_df = pp.Usage_dataframe(Usage_series, year)
    
            # Time zone correction for profiles and usage
            Profiles_utc = pp.Time_correction(Profiles_df, country, year) 
            Usage_utc = pp.Time_correction(Usage_df, country, year)    
    
            # By default, profiles and usage are plotted as a DataFrame
            if plots:
                pp.Profile_df_plot(Profiles_df, start = '01-01 00:00:00', end = '12-31 23:59:00', year = year, country = country)
            if plots:
                pp.Usage_df_plot(Usage_utc, start = '01-01 00:00:00', end = '12-31 23:59:00', year = year, country = country, User_list = User_list)
    
            # Add temperature correction to the Power Profiles 
            # To be done after the UTC correction because the source data for Temperatures have time in UTC
            temp_profile = pp.temp_import(country, year, inputfile_temp) #Import temperature profiles, change the default path to the custom one
            Profiles_temp = pp.Profile_temp(Profiles_utc, year = year, temp_profile = temp_profile)
    
            # Resampling the UTC Profiles
            Profiles_temp_h = pp.Resample(Profiles_temp)
    
            #Exporting all the main quantities
            if write_variables:
                pp.export_csv('Mobility Profiles', Profiles_temp, inputfile, simulation_name)
                pp.export_csv('Mobility Profiles Hourly', Profiles_temp_h, inputfile, simulation_name)
                pp.export_csv('Usage', Usage_utc, inputfile, simulation_name)
            #   pp.export_pickle('Profiles_User', Profiles_user_temp, inputfile, simulation_name)
        
            if charging:
        
                Profiles_user_temp = pp.Profile_temp_users(Profiles_user, temp_profile,
                                                           year, dummy_days, inplace=True)
     
                # Charging process function: if no problem is detected, only the cumulative charging profile is calculated. Otherwise, also the user specific quantities are included. 
                (Charging_profile, Ch_profile_user, SOC_user) = Charging_Process(
                    Profiles_user_temp, User_list, country, year,dummy_days, 
                    residual_load, charging_mode, logistic, infr_prob, Ch_stations, retain_diagnostics=False)        
    
                Charging_profile_df = pp.Ch_Profile_df(Charging_profile, year) 
                
                # Postprocess of charging profiles 
                Charging_profiles_utc = pp.Time_correction(Charging_profile_df, 
                                                           country, year) 
    
                # Export charging profiles in csv
                pp.export_csv('Charging Profiles', Charging_profiles_utc, inputfile, simulation_name)
    
                # Plot the charging profile
                if plots:
                    pp.Charging_Profile_df_plot(Charging_profiles_utc, color = 'green', start = '01-01 00:00:00', end = '12-31 23:59:00', year = year, country = country)
                
            print('\nExecution Time:', datetime.now() - startTime)

        return {"negative_soc_users": sum(len(values) for values in SOC_user.values()),
                "soc_diagnostics": SOC_user}
    finally:
        if isinstance(Profiles_user, dict):
            close_profile_store(Profiles_user)


if __name__ == "__main__":
    raise SystemExit("Use uv run --locked --offline python run_simulation.py from the project root; add --run to simulate.")
