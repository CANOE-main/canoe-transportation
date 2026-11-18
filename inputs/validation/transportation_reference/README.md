# Historical transportation comparison inputs

These inputs support the transportation validation notebook's Ontario comparisons.
The production build derives its parameters from the configured sources.

- `sales_cohorts/` preserves the original researcher notebook and workbooks read
  by the sales, activity, growth, survival and utilization calculations. The two
  SQLite files retain the historical rows and columns consumed by the notebook
  and charging comparison. The sales variant and the supplied recipe are separate
  evidence of what was stored and what the inspected calculation prescribes.
- `charger_costs/` preserves the original investment and fixed-cost rows. The
  variable-cost export contained 11,937 rows, with zero entries for the four
  compared technologies. Its curated file records the native header, and the
  manifest preserves that coverage. The exact run that produced the exports
  remains unresolved.

Each `manifest.json` records the source revision, original paths, original and
retained checksums, and projection selections and counts. The SQLite files are
table projections for read-only comparison. Both retain source keys and units
within the documented selection. The original databases' integrity and
foreign-key results are recorded in the manifest.

The inherited sales snapshot substitutes for the workbook recipe's unavailable
all-region parameter database. The older Ontario extract contributes historical
comparison rows and non-cost charger context. Neither identity establishes the
original runs' exact lineage. Related charging research is documented in
[CANOE transportation PR #1](https://github.com/CANOE-main/canoe-transportation/pull/1).

The notebook's preparation instructions regenerate the build and report tables
from the current scenario, registered sources, and the historical comparison
inputs above. The reports and notebook captures are local build products.
[Transportation validation notebook](../../../docs/insights/transportation_validation_diagnosis.py)
