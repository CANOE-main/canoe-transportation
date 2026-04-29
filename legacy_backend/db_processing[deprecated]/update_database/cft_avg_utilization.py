#!/usr/bin/env python3
"""
Rescale capacity-factor rows for a single technology so the
average factor becomes a user-defined target (default = 0.20).

Usage
-----
python scale_cf.py /path/to/canoe_on_12d_vanilla4.sqlite \
                   --tech T_LDV_BEV_CHRG --target 0.2
"""
import argparse
import os
import sqlite3
import sys
from pathlib import Path

db_name = 'canoe_on_12d_vanilla4_charger_batteries'
tech = 'T_LDV_BEV_CHRG'  # technology to scale
target = 0.20  # target average factor

dir_path = os.path.dirname(os.path.abspath(__file__)) + '/'
db_path = dir_path + 'target_database/' + db_name + '.sqlite'


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--dbfile", default=db_path, 
                   help="SQLite database file to edit")
    p.add_argument("--tech", default=tech,
                   help="Technology name to scale (default: T_LDV_BEV_CHRG)")
    p.add_argument("--target", type=float, default=target,
                   help="Desired average factor (default: 0.20)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would happen but do NOT write changes")
    return p.parse_args()


def main():
    args = parse_args()
    db_path = Path(args.dbfile).expanduser().resolve()
    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # 1. Current mean for the chosen technology
        cur.execute(
            "SELECT AVG(factor) FROM CapacityFactorTech WHERE tech = ?",
            (args.tech,),
        )
        current_avg = cur.fetchone()[0]
        if current_avg is None:
            sys.exit(f"No rows found with tech = '{args.tech}'")

        # 2. Scaling multiplier so new mean = target
        scale = args.target / current_avg
        print(f"Current average: {current_avg:.6f}")
        print(f"Target average : {args.target:.6f}")
        print(f"Scaling factor : {scale:.6f}")

        # 3. Apply the update
        if args.dry_run:
            print("--dry-run specified: no data written.")
            return

        cur.execute(
            "UPDATE CapacityFactorTech "
            "SET factor = factor * ? "
            "WHERE tech = ?",
            (scale, args.tech),
        )
        affected = cur.rowcount
        conn.commit()
        print(f"Updated {affected} rows.  Done.")


if __name__ == "__main__":
    main()
