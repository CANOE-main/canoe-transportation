# Canadian Open Energy Model (CANOE) - Transportation Sector With Fuel Supply
Framework for the transportation energy system database of the Canadian Open Energy Model

This repository contains the compiled input data of the transportation sector used for the CANOE project.
- **this directory** - contains the original schema and alternative scenarios of the CANOE-transportation database, including the _reference_ (ref) scenario. 
- **transportation** - contains the annotated transport sector databases in Excel spreadsheet format and a compiler that inserts .xlsx tables into the .sqlite of the vanilla model.
- **db_processing** - contains scripts used to process the compiled database from the Excel spreadsheets into usable formats for the Temoa framework.
- **model_constraints** - contains Excel spreasheets with the different explicit (user-defined) constraints applied to the vanilla CANOE-transportation model.
- **charging_profiles** - contains the input parameters used to simulate the LDV charging profiles with the RAMP-mobility framework.
- **results_analysis** - contains the Jupyter notebooks used to analyze CANOE-transportation scenario results.
- **fuel_supply** - contains fuel supply input data used in the spreadsheet databases.

testing