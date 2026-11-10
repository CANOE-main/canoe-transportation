# Lih-Wei legacy comparison evidence

The retained `CostInvest.csv`, `CostFixed.csv` and `CostVariable_ALL.csv` are
researcher-supplied comparison snapshots. The charging-cost diagnostic reads these
three files through `transportation_validation_charging_inputs` in
[`config/paths.yaml`](../../config/paths.yaml); they do not parameterize production.
The folder was renamed from `legacy_vatiant_charger_costs` without changing the CSV
contents. Their exact producing run is not established by this folder or its name.

[PR #1](https://github.com/CANOE-main/canoe-transportation/pull/1), reviewed at
[`62cf8b1`](https://github.com/CANOE-main/canoe-transportation/commit/62cf8b16078f53ac045569b76be7ab01d5a1c58e),
contains a related long-haul truck and charging research variant. It explores
freight segmentation, payload and battery scaling, charging profiles, and scenario
sensitivities. Its external data, solved runs and original environment are
incomplete here. The author continued that work in a separate repository.
No additional notebooks, workbooks, result databases, plots or production changes
from that PR were adopted. The existing three CSVs remain the bounded comparison
evidence; the PR is a pointer to the separate research context, not proof of their
exact derivation.

Recorded build and publication manifests retain their historical paths and hashes.
A fresh comparison requires the supported scenario rebuild and report regeneration;
renaming this folder does not justify rewriting recorded identities.
