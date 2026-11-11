import sqlite3, shutil, os, pandas as pd

db_source   = 'canoe_on_12d_vanilla4_dual_carriers'
db_target   = 'canoe_on_12d_baseline_dual_carriers'
constraints = 'trn_constraints_freegrowth'

dir_path  = os.path.dirname(os.path.abspath(__file__)) + '/'
source    = f'{dir_path}../db_processing/update_database/target_database/{db_source}.sqlite'
target    = f'{dir_path}../{db_target}.sqlite'
sheet_path = f'{dir_path}{constraints}.xlsx'

shutil.copyfile(source, target)

YEARS_TO_EXPAND   = [2021, 2025, 2030, 2035, 2040, 2045, 2050]
YEAR_COL_CANDIDATES = ('vintage', 'period')          # never both at once

def expand_all_years(df: pd.DataFrame) -> pd.DataFrame:
    """Replace rows whose year field == 'All' with duplicates for each target year."""
    year_col = next((c for c in YEAR_COL_CANDIDATES if c in df.columns), None)
    if year_col is None:
        return df                                               # nothing to do

    mask_all = df[year_col].astype(str).str.lower() == 'all'    # tolerate 'All' or 'all'
    if not mask_all.any():
        return df

    # rows with explicit years stay as-is
    keep = df.loc[~mask_all]

    # replicate the 'All' rows for each year
    replicated = pd.concat(
        [df.loc[mask_all].assign(**{year_col: yr}) for yr in YEARS_TO_EXPAND],
        ignore_index=True
    )
    return pd.concat([keep, replicated], ignore_index=True)

# ----------------------------------------------------------------------
excel_data = pd.ExcelFile(sheet_path)
sheet_data = {sheet: excel_data.parse(sheet) for sheet in excel_data.sheet_names}

conn, cur = sqlite3.connect(target), None
with conn:
    cur = conn.cursor()

    for sheet_name, df in sheet_data.items():
        df = expand_all_years(df)                 # <-- generic expansion

        # example: keep your custom melt for GrowthRate* if needed
        # if sheet_name in ('GrowthRateMin', 'GrowthRateMax'):
        #     df = df.melt(id_vars=['region', 'tech', 'notes', 'reference'],
        #                  var_name='period', value_name='rate')

        # bulk insert ----------------------------------------------------
        cols = df.columns.tolist()
        placeholders = ','.join(['?'] * len(cols))
        sql = f'INSERT INTO {sheet_name} ({", ".join(cols)}) VALUES ({placeholders})'
        conn.executemany(sql, df.itertuples(index=False, name=None))

conn.close()
