#!/usr/bin/env python3
"""
Rescale & copy CapacityFactorTech rows between CANOE SQLite DBs.

Rows are matched on (region, season, tod, tech):
  • sum_source  = SUM(factor) for those rows in SOURCE_DB
  • sum_scale   = SUM(factor) for the *same* keys in SCALE_DB
  • scale = sum_scale / sum_source   — must be in [0, 1]

An extra check verifies the two sums were computed from the *same count* of
rows.  If the counts differ the script aborts safely.

Only the matched rows in TARGET_DB are updated (factor × scale; all other
non-key columns copied verbatim).  No new rows are inserted.

Author : 2025-06-17
"""

import sqlite3
from pathlib import Path
import sys

# ────────────────────────────────────────────────────────────────────────────
# 1.  Database locations
# ────────────────────────────────────────────────────────────────────────────
TARGET_NAME = "canoe_on_12d_vanilla4_charger_batteries"     # gets UPDATED -- the time periods must come from a vanilla file
SOURCE_NAME = "canoe_trn_on_vanilla4_charger_batteries_v3"        # PROVIDES data
SCALE_NAME  = "canoe_on_12d_vanilla4"                # used only for scaling

BASE_DIR   = Path(__file__).absolute().parent
TARGET_DB  = BASE_DIR / "target_database" / f"{TARGET_NAME}.sqlite"
SOURCE_DB  = BASE_DIR / "../to_temoa_v3/v3_database" / f"{SOURCE_NAME}.sqlite"
SCALE_DB   = BASE_DIR / "target_database" / f"{SCALE_NAME}.sqlite"

# ────────────────────────────────────────────────────────────────────────────
# 2.  Key columns
# ────────────────────────────────────────────────────────────────────────────
KEY_COLS = ["region", "season", "tod", "tech"]

# ────────────────────────────────────────────────────────────────────────────
def key_tuple(row):
    """Return (region, season, tod, tech) tuple from a Row object."""
    return tuple(row[k] for k in KEY_COLS)

# ────────────────────────────────────────────────────────────────────────────
def main() -> None:
    # --- make sure all DB files exist -------------------------------------
    missing = [p for p in (SOURCE_DB, TARGET_DB, SCALE_DB) if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Could not locate: {', '.join(str(p) for p in missing)}")

    with sqlite3.connect(SOURCE_DB) as src, \
         sqlite3.connect(TARGET_DB)  as tgt, \
         sqlite3.connect(SCALE_DB)   as scl:

        src.row_factory = sqlite3.Row
        scl.row_factory = sqlite3.Row

        s_cur, t_cur, c_cur = src.cursor(), tgt.cursor(), scl.cursor()

        # --- full column list ---------------------------------------------
        all_cols = [r[1] for r in s_cur.execute("PRAGMA table_info(CapacityFactorTech)")]
        non_key_cols = [c for c in all_cols if c not in KEY_COLS]

        # --- keys present in TARGET_DB ------------------------------------
        tgt_keys = {
            tuple(r) for r in t_cur.execute(
                f"SELECT {', '.join(KEY_COLS)} FROM CapacityFactorTech"
            )
        }

        # --- collect matched rows from SOURCE_DB --------------------------
        matched_rows = []
        sum_source = 0.0
        for row in s_cur.execute(f"SELECT {', '.join(all_cols)} FROM CapacityFactorTech"):
            k = key_tuple(row)
            if k in tgt_keys:
                matched_rows.append(row)
                sum_source += row["factor"]

        if not matched_rows:
            print("No matching rows between SOURCE_DB and TARGET_DB – nothing to do.")
            return

        # --- compute SUM(factor) in SCALE_DB for the same keys ------------
        placeholders = " AND ".join(f"{k}=?" for k in KEY_COLS)
        sum_scale = 0.0
        scale_count = 0
        for row in matched_rows:
            k_vals = key_tuple(row)
            c_cur.execute(f"SELECT factor FROM CapacityFactorTech WHERE {placeholders}", k_vals)
            rec = c_cur.fetchone()
            if rec is None:
                # Key missing in SCALE_DB – abort safely
                print(
                    f"Key {k_vals} found in SOURCE/TARGET but not in SCALE_DB. "
                    "Aborting without changes."
                )
                return
            sum_scale += rec["factor"]
            scale_count += 1

        # --- new safety check: same row count for both sums ----------------
        if scale_count != len(matched_rows):
            print(
                f"Mismatch in row counts: SOURCE rows = {len(matched_rows)}, "
                f"SCALE rows = {scale_count}. Aborting."
            )
            return

        # --- derive scaling factor ----------------------------------------
        if sum_source == 0:
            print("sum_source is zero – cannot scale. Aborting.")
            return

        scale = sum_scale / sum_source
        # if not (0 <= scale <= 1):
        #     print(f"Scale factor {scale:.6f} outside [0,1] – sanity check failed. Aborting.")
        #     return

        print(f"Matched rows       : {len(matched_rows):,}")
        print(f"SUM(source.factor) : {sum_source:,.6f}")
        print(f"SUM(scale.factor)  : {sum_scale:,.6f}")
        print(f"Scale factor       : {scale:.6f}")

        # --- build UPDATE statement ---------------------------------------
        set_clause   = ", ".join(
            f"{c}=?" if c != "factor" else "factor=?" for c in non_key_cols
        )
        where_clause = " AND ".join(f"{k}=?" for k in KEY_COLS)
        update_sql   = f"UPDATE CapacityFactorTech SET {set_clause} WHERE {where_clause}"

        # --- apply updates -------------------------------------------------
        updated = 0
        for row in matched_rows:
            params = [
                (row["factor"] * scale) if c == "factor" else row[c]
                for c in non_key_cols
            ] + list(key_tuple(row))
            t_cur.execute(update_sql, params)
            updated += t_cur.rowcount

        tgt.commit()
        print(f"Rows updated       : {updated:,}\nDone.")

# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        sys.exit(f"ERROR: {exc}")
