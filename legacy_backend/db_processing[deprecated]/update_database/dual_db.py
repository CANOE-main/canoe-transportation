import sqlite3
import os
import shutil

db_name = "canoe_on_12d_vanilla4"

dir_path  = os.path.dirname(os.path.abspath(__file__))
input_db = f'{dir_path}/target_database/{db_name}.sqlite'
output_db = f'{dir_path}/target_database/{db_name}_dual_carriers.sqlite'

shutil.copyfile(input_db, output_db)

technology_data = [
    # to capture shadow price of transport commodities
    ("T_D_GSL", "p", "Transport", 1, 0),
    ("T_D_DSL", "p", "Transport", 1, 0),
    ("T_D_ELC", "p", "Transport", 1, 0),
    ("T_D_H2", "p", "Transport", 1, 0),
    ("T_D_ELC_DIST", "p", "Transport", 1, 0),
    ("T_D_H2_DIST", "p", "Transport", 1, 0),

    # to capture shadow price of demand commodities
    # ("T_D_PKM_LDV_C_BEV", "d", "Transport", 1, 0),
    # ("T_D_PKM_LDV_C_GSL", "d", "Transport", 1, 0),
    # ("T_D_PKM_LDV_C_HEV", "d", "Transport", 1, 0),
    # ("T_D_PKM_LDV_C_PHEV35", "d", "Transport", 1, 0),

    # ("T_D_PKM_LDV_T_BEV", "d", "Transport", 1, 0),
]

commodity_data = [
    # to capture shadow price of transport commodities
    ("T_D_gsl", "d"),
    ("T_D_dsl", "d"),
    ("T_D_elc", "d"),
    ("T_D_h2", "d"),
    ("T_D_elc_dist", "d"),
    ("T_D_h2_dist", "d"),

    # to capture shadow price of demand commodities
    # ("T_D_pkm_ldv_c_bev", "d"),
    # ("T_D_pkm_ldv_c_gsl", "d"),
    # ("T_D_pkm_ldv_c_hev", "d"),
    # ("T_D_pkm_ldv_c_phev35", "d"),

    # ("T_D_pkm_ldv_t_bev", "d"),
]

efficiency_data = [
    # to capture shadow price of transport commodities
    ("ON", "gsl", "T_D_GSL", 2021, "T_D_gsl", 1),
    ("ON", "dsl", "T_D_DSL", 2021, "T_D_dsl", 1),
    ("ON", "T_elc", "T_D_ELC", 2021, "T_D_elc", 1),
    ("ON", "h2_700", "T_D_H2", 2021, "T_D_h2", 1),
    ("ON", "T_elc_ldv_bev_chrg", "T_D_ELC_DIST", 2021, "T_D_elc_dist", 1),
    ("ON", "T_h2_hdv", "T_D_H2_DIST", 2021, "T_D_h2_dist", 1),

    # to capture shadow price of demand commodities
    # ("ON", "T_elc_ldv_bev_chrg", "T_D_ELC", 2021, "T_D_elc", 1),
]

demand_data = []
for comm in [
    "T_D_gsl", "T_D_dsl", "T_D_elc", "T_D_h2", "T_D_elc_dist", "T_D_h2_dist"
    ]:
    for year in [2021, 2025, 2030, 2035, 2040, 2045, 2050]:
        demand_data.append(("ON", year, comm, 1))

# Connect and insert
def inject_entries():
    conn = sqlite3.connect(output_db)
    cursor = conn.cursor()

    # Insert into Technology
    cursor.executemany("""
        INSERT OR IGNORE INTO Technology (tech, flag, sector, unlim_cap, annual)
        VALUES (?, ?, ?, ?, ?);
    """, technology_data)

    # Insert into Commodity
    cursor.executemany("""
        INSERT OR IGNORE INTO Commodity (name, flag)
        VALUES (?, ?);
    """, commodity_data)

    # Insert into Efficiency
    cursor.executemany("""
        INSERT OR IGNORE INTO Efficiency (region, input_comm, tech, vintage, output_comm, efficiency)
        VALUES (?, ?, ?, ?, ?, ?);
    """, efficiency_data)

    # Insert into Demand
    cursor.executemany("""
        INSERT OR IGNORE INTO Demand (region, period, commodity, demand)
        VALUES (?, ?, ?, ?);
    """, demand_data)

    conn.commit()
    conn.close()

# Run the function
inject_entries()
