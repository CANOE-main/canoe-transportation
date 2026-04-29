#!/usr/bin/env python3
"""
Sync entries in CapacityFactorTech between two CANOE SQLite databases.

Rows are matched on (region, season, tod, tech).  For every match,
ALL OTHER COLUMNS in the target are overwritten by the values
in the source.  Unmatched rows are left untouched and no new rows
are inserted.

Author : 2025-06-11
"""

import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# 1.  Database locations – adjust names or paths as required
# ---------------------------------------------------------------------------
TARGET_NAME = "canoe_on_12d_vanilla4_charger_batteries"            # gets updated on selected days
SOURCE_NAME = "canoe_trn_on_vanilla4_charger_batteries_v3"         # has full 8760-h profile, provides data

BASE_DIR   = Path(__file__).absolute().parent
TARGET_DB  = BASE_DIR / "target_database" / f"{TARGET_NAME}.sqlite"
SOURCE_DB  = BASE_DIR / "../to_temoa_v3/v3_database" / f"{SOURCE_NAME}.sqlite"

# ---------------------------------------------------------------------------
# 2.  Key columns that uniquely identify a row
# ---------------------------------------------------------------------------
KEY_COLS = ["region", "season", "tod", "tech"]

# ---------------------------------------------------------------------------
# 3.  Main routine
# ---------------------------------------------------------------------------
def main() -> None:
    if not SOURCE_DB.exists():
        raise FileNotFoundError(f"Source DB not found: {SOURCE_DB}")
    if not TARGET_DB.exists():
        raise FileNotFoundError(f"Target DB not found: {TARGET_DB}")

    with sqlite3.connect(SOURCE_DB) as src, sqlite3.connect(TARGET_DB) as tgt:
        src.row_factory = sqlite3.Row
        s_cur, t_cur = src.cursor(), tgt.cursor()

        # --- discover full column list so we can update "everything" ----------
        cols = [row[1] for row in s_cur.execute(
            "PRAGMA table_info(CapacityFactorTech)"
        )]
        non_key_cols = [c for c in cols if c not in KEY_COLS]

        # --- build statements -------------------------------------------------
        select_sql  = "SELECT " + ", ".join(cols) + " FROM CapacityFactorTech"
        set_clause  = ", ".join(f"{c}=?" for c in non_key_cols)
        where_clause = " AND ".join(f"{k}=?" for k in KEY_COLS)
        update_sql  = f"UPDATE CapacityFactorTech SET {set_clause} WHERE {where_clause}"

        updated, skipped = 0, 0

        for row in s_cur.execute(select_sql):
            # parameters: all non-key columns first, then the key columns
            params = tuple(row[c] for c in non_key_cols) + tuple(row[k] for k in KEY_COLS)
            t_cur.execute(update_sql, params)

            if t_cur.rowcount:
                updated += t_cur.rowcount   # row really changed
            else:
                skipped += 1                # no matching row in target

        tgt.commit()

    print(
        f"Finished.\n"
        f"  Rows updated : {updated:,}\n"
        f"  Rows skipped : {skipped:,} (no match in target)"
    )


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
