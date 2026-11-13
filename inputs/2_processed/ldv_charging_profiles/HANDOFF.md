# Ontario charging-profile alternatives for Thiago

Six mutually exclusive alternatives: NHTS2022 or TTS2016, each with uncontrolled, night or day charging. Choose **one survey/strategy folder per scenario**, not all six factor files at once.

Each folder contains 17,520 `capacity_factor_tech` rows: 8,760 physical hours for Ontario and each of `T_LDV_CHRG_EX` and `T_LDV_CHRG_N`. Both technologies have a reviewed annual mean factor of 0.20; the alternatives change timing, not that annual utilization. Factors are dimensionless CANOE v4 rows, not the raw RAMP aggregate kW.

Use the companion `time_season.csv` and `time_of_day.csv` with the hourly representation. `hourly_calendar.csv` relates elapsed-hour keys to UTC and Toronto local timestamps, including DST; `hourly_mapping.csv` records the model projection. If a model uses representative days/slices, prepare an explicit complete mapping through `charging_profiles.time_mapping` first. Do not paste the full-hour rows into incompatible time axes. Replace the selected charger factors rather than appending conflicting alternatives.

`data_set.csv`, `data_source.csv` and `data_source_label.csv` carry the published IDs, DQ and notes used by the factor rows. The sharing archive also contains the six complete scenario YAMLs and integrity reports; raw simulation metadata/aggregate input databases remain in the registered external-model catalogue.

| Strategy | Preferred local charging period | Scheduling |
|---|---|---|
| Uncontrolled | Any parked time | Starts at arrival if SOC/access conditions permit; sampled charger power. |
| Night | 22:00–07:00 | Spreads required energy across eligible parked minutes, capped by charger power. |
| Day | 07:00–18:00 | Same controlled scheduler using daytime minutes. |

Low SOC or insufficient energy for the next trip permits emergency charging outside the preferred windows. All cases retain the same vehicle/driver/charger assumptions. TTS repeats weekday evidence on weekends; NHTS retains the legacy unweighted filtered sample and intentional midpoint trip-occurrence distributions. The reruns correct SOC efficiency and UTC weather lookup, with no multi-hour shift found relative to legacy uncontrolled profiles.

## Reproduce the six charging exports

From the repository root, run serially:

```powershell
Get-ChildItem config/scenarios/validation_charging_*.yaml | Sort-Object Name | ForEach-Object {
    uv run --offline python -m parameterization.ldv_charging_profiles --scenario $_.FullName
    if ($LASTEXITCODE -ne 0) { throw "Charging preparation failed" }
}
```

This uses the existing parameter owner and writes these survey/strategy folders without SQLite insertion. To export **every** prepared transport table for any scenario instead, use `uv run python src/build_transport.py --scenario <scenario.yaml> --processed-only`. For the same table export alongside a normal SQLite build, add `--export-processed`.

The archive is a sharing convenience; these CSVs, scenario YAMLs and the registered source metadata are the reproducible records. No SQLite database was replaced when preparing this handoff.
