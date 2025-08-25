"""
Updates the matching content of a target .sqlite database with a source .sqlite exclusively of matches from a subset .sqlite.
This script is designed to replace rows in the target database with those from the source database, based
on the existence of corresponding entries in the subset database. It purges empty values and removes duplicates
from the target database after the replacement.
@author: Rashid Zetter
"""

import sqlite3
import os
import shutil

# ---------------------------------------------------------------------------
# ‑‑‑ User configurable paths -------------------------------------------------
# ---------------------------------------------------------------------------

target_name = 'canoe_on_12d_vanilla4_charger_batteries'
source_name = 'canoe_trn_on_vanilla4_charger_batteries_v3'
subset_name = 'canoe_trn_on_vanilla4_v3'

# Define the paths for the source, target, and subset databases
DIR_PATH = os.path.dirname(os.path.abspath(__file__)) + '/'
target = DIR_PATH + 'target_database/' + target_name + '.sqlite'      # Database to be updated
source = DIR_PATH + '../to_temoa_v3/v3_database/' + source_name + '.sqlite'  # Where new datapoints come from
subset = DIR_PATH + '../to_temoa_v3/v3_database/' + subset_name + '.sqlite'  # To identify datapoints to replace
log    = DIR_PATH + 'update_log.txt'

# ---------------------------------------------------------------------------
# ‑‑‑ Connect to databases ----------------------------------------------------
# ---------------------------------------------------------------------------

source_conn = sqlite3.connect(source)
target_conn = sqlite3.connect(target)
subset_conn = sqlite3.connect(subset)

source_cursor = source_conn.cursor()
target_cursor = target_conn.cursor()
subset_cursor = subset_conn.cursor()

# ---------------------------------------------------------------------------
# ‑‑‑ Prepare log file --------------------------------------------------------
# ---------------------------------------------------------------------------

log_file = open(log, 'w')

# ---------------------------------------------------------------------------
# ‑‑‑ Table lists -------------------------------------------------------------
# ---------------------------------------------------------------------------

tech_tables = [
    'Technology',
    'LifetimeTech',
    'ExistingCapacity',
    'CapacityToActivity',
    # 'CapacityFactorProcess',
    # 'CapacityFactorTech',
    'MaxAnnualCapacityFactor',
    'MinAnnualCapacityFactor',
    'Efficiency',
    'CostInvest',
    'CostFixed',
    'CostVariable',
    'EmissionActivity',
    'EmissionEmbodied',
    'TechInputSplit',
    # 'StorageDuration'
]

commodity_tables = {
    'Commodity': 'name',
    'Demand':    'commodity',
    # 'DemandSpecificDistribution': 'demand_name'
}

# ---------------------------------------------------------------------------
# ‑‑‑ Helper: purge rows with empty key values --------------------------------
# ---------------------------------------------------------------------------

def _purge_empty_rows(table_name: str, column_name: str):
    """Delete rows whose *column_name* value is NULL, empty/whitespace,
    or lacks any alphabetical character."""
    target_cursor.execute(
        f'''DELETE FROM "{table_name}" 
            WHERE {column_name} IS NULL
               OR TRIM({column_name}) = ''
               OR LOWER({column_name}) NOT GLOB '*[a-z]*'
        '''
    )
    log_file.write(f"Purged empty values from {table_name}.{column_name}\n")

# ---------------------------------------------------------------------------
# ‑‑‑ Core replace functions --------------------------------------------------
# ---------------------------------------------------------------------------

def replace_tech_rows(table_name: str):
    """Replace rows whose *tech* exists in the subset DB, then insert rows
    from the source DB, followed by a purge of empty tech values."""

    # Gather tech list from subset
    subset_cursor.execute('SELECT tech FROM "Technology"')
    techs = [r[0] for r in subset_cursor.fetchall()]

    # Delete matching techs in target
    for tech in techs:
        target_cursor.execute(f'DELETE FROM "{table_name}" WHERE tech = ?', (tech,))

    # Insert fresh rows from source
    source_cursor.execute(f'SELECT * FROM "{table_name}"')
    rows = source_cursor.fetchall()
    source_cursor.execute(f'PRAGMA table_info("{table_name}")')
    columns = [col[1] for col in source_cursor.fetchall()]

    placeholders = ', '.join(['?'] * len(columns))
    cols_str     = ', '.join(columns)

    for row in rows:
        try:
            target_cursor.execute(f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({placeholders})', row)
            log_file.write(f"Inserted new row in {table_name} for tech: {row[columns.index('tech')]}\n")
        except sqlite3.IntegrityError as e:
            log_file.write(f"Insert failed for {table_name} with row {row}; error: {e}\n")

    # --- Purge empty tech entries ----------------------------------------
    _purge_empty_rows(table_name, 'tech')


def replace_commodity_rows(table_name: str, column_name: str):
    """Replace rows whose *column_name* exists in the subset DB, insert rows
    from the source DB, then purge empty values in *column_name*."""

    # Gather names from subset Commodity table
    subset_cursor.execute('SELECT name FROM "Commodity"')
    names = [r[0] for r in subset_cursor.fetchall()]

    # Delete matching names in target
    for name in names:
        target_cursor.execute(f'DELETE FROM "{table_name}" WHERE {column_name} = ?', (name,))

    # Insert fresh rows
    source_cursor.execute(f'SELECT * FROM "{table_name}"')
    rows = source_cursor.fetchall()
    source_cursor.execute(f'PRAGMA table_info("{table_name}")')
    columns = [col[1] for col in source_cursor.fetchall()]

    placeholders = ', '.join(['?'] * len(columns))
    cols_str     = ', '.join(columns)

    for row in rows:
        try:
            target_cursor.execute(f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({placeholders})', row)
            log_file.write(
                f"Inserted new row in {table_name} for {column_name}: {row[columns.index(column_name)]}\n"
            )
        except sqlite3.IntegrityError as e:
            log_file.write(f"Insert failed for {table_name} with row {row}; error: {e}\n")

    # --- Purge empty key values ------------------------------------------
    _purge_empty_rows(table_name, column_name)

# ---------------------------------------------------------------------------
# ‑‑‑ Special handling: references table -------------------------------------
# ---------------------------------------------------------------------------

def replace_references():
    source_cursor.execute('SELECT * FROM "references"')
    src_refs = source_cursor.fetchall()

    target_cursor.execute('SELECT reference FROM "references"')
    tgt_refs = {r[0] for r in target_cursor.fetchall()}

    for ref_row in src_refs:
        if ref_row[0] not in tgt_refs:
            try:
                target_cursor.execute('INSERT INTO "references" (reference) VALUES (?)', ref_row)
                log_file.write(f"Inserted new reference: {ref_row[0]}\n")
            except sqlite3.IntegrityError as e:
                log_file.write(f"Insert failed for references: {ref_row}; error: {e}\n")

# ---------------------------------------------------------------------------
# ‑‑‑ Helper: remove duplicate rows from every table -------------------------
# ---------------------------------------------------------------------------

def remove_duplicates():
    target_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    for (table_name,) in target_cursor.fetchall():
        target_cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns = [col[1] for col in target_cursor.fetchall()]
        if not columns:
            continue
        cols_str = ', '.join(columns)
        target_cursor.execute(f'''CREATE TABLE IF NOT EXISTS temp_{table_name} AS
                                   SELECT DISTINCT {cols_str} FROM "{table_name}"''')
        target_cursor.execute(f'DROP TABLE "{table_name}"')
        target_cursor.execute(f'ALTER TABLE temp_{table_name} RENAME TO "{table_name}"')
        log_file.write(f"Removed duplicates from {table_name}\n")

# ---------------------------------------------------------------------------
# ‑‑‑ Orchestrate replacement -------------------------------------------------
# ---------------------------------------------------------------------------

for tbl in tech_tables:
    replace_tech_rows(tbl)

for tbl, col in commodity_tables.items():
    replace_commodity_rows(tbl, col)

replace_references()
remove_duplicates()

# ---------------------------------------------------------------------------
# ‑‑‑ Commit, optimise & close ----------------------------------------------
# ---------------------------------------------------------------------------

target_conn.commit()

target_cursor.execute('VACUUM')  # reclaim unused space

source_conn.close()
target_conn.close()
subset_conn.close()
log_file.close()
