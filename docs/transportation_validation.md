---
title: Transportation validation reading guide
role: Links to canonical methods and current diagnostic evidence.
status: implemented
verify: Current executable configuration and recorded build identities govern findings.
---

# Transportation validation and diagnostic evidence

Use the canonical explanations alongside their recorded diagnostic evidence:

- [Fleet activity and utilisation](model/activity.md) explains stock–activity accounting and activity-calibration closure.
- [Fuel consumption and service efficiency](model/energy.md) explains optional energy calibration, residuals, accounting boundaries and the distinction between calibration and independent validation.
- [Supporting infrastructure](model/infrastructure.md) explains charger costs and the comparison basis of legacy evidence.
- [Scenario configuration](../config/scenarios/README.md) owns supported selections and commands.
- [Preparing publication evidence](publication.md) owns the serial build, capture and export sequence.

The interactive [transportation validation notebook](insights/transportation_validation_diagnosis.py) reads compatible recorded comparisons and rejects stale build identities. Historical evaluations remain in the completed ExecPlan records; their numerical findings do not describe a newly built scenario.
