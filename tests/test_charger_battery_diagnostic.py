"""Truck battery coverage, physical units and service-activity weighting."""

from copy import deepcopy
from types import SimpleNamespace

import pandas as pd
import pytest

from validation import charger_battery_diagnostic as battery


def source_rows():
    return pd.DataFrame([
        {"year": year, "trajectory": "Mid", "metric_key": "usable_battery_capacity_kwh",
         "vehicle_weight_category": "Medium/Heavy Duty", "vehicle_class": label,
         "vehicle_detail": "Battery Electric Vehicle", "value": float(value), "unit": "kWh",
         "source_id": "nlr_atb_transportation", "source_member": "output/vehicles.csv"}
        for year, medium, heavy in [(2025, 100, 400), (2030, 90, 350), (2050, 80, 300)]
        for label, value in [("Class 3 Medium Van", medium), ("Class 8 Longhaul Sleeper", heavy)]
    ])


def select(frame, years=None):
    return battery.select_battery_rows(
        frame, years=years or [2025, 2030, 2050], trajectory="Mid",
        metric_key="usable_battery_capacity_kwh", weight_category="Medium/Heavy Duty",
        vehicle_detail="Battery Electric Vehicle",
    )


@pytest.mark.parametrize("field,value", [
    ("unit", "Wh"), ("source_id", "unregistered"),
    ("value", 0), ("value", -10), ("value", float("nan")), ("value", float("inf")),
])
def test_battery_physical_and_source_contracts_are_required(field, value):
    frame = source_rows()
    frame.loc[0, field] = value
    with pytest.raises(ValueError):
        select(frame)


def test_source_points_are_exact_and_duplicates_are_rejected():
    frame = source_rows()
    with pytest.raises(ValueError, match="exact requested source year"):
        select(frame, [2025, 2040])
    with pytest.raises(ValueError, match="duplicate"):
        select(pd.concat([frame, frame.iloc[[0]]], ignore_index=True))
    result = select(pd.concat([frame, frame.assign(trajectory="Advanced")], ignore_index=True))
    assert len(result) == 6
    assert set(result.trajectory) == {"Mid"}


def test_configured_vocation_weights_conserve_battery_mean_and_keep_audit():
    frame = source_rows().loc[lambda rows: rows.year.eq(2025)].copy()
    extra = frame.iloc[[0]].assign(vehicle_class="Class 4 Medium Box", value=300)
    frame = pd.concat([frame, extra], ignore_index=True)
    weights = {("ON", "medium_trucks"): {"Class 3 Medium Van": 0.75, "Class 4 Medium Box": 0.25},
               ("ON", "heavy_trucks"): {"Class 8 Longhaul Sleeper": 1}}
    original = deepcopy(weights)
    classes, audit = battery.aggregate_battery_classes(select(frame, [2025]), weights, years=[2025])
    medium = classes.loc[classes.road_class.eq("medium_trucks")].iloc[0]
    assert medium.battery_kwh == 150
    assert medium.battery_kwh != 200  # Equal-class averaging would contradict the configured mix.
    assert len(audit) == 3
    assert audit.groupby("road_class").class_weight.sum().eq(1).all()
    assert weights == original
    with pytest.raises(ValueError, match="coverage is incomplete"):
        battery.aggregate_battery_classes(frame.loc[frame.vehicle_class.ne("Class 4 Medium Box")], weights, years=[2025])
    with pytest.raises(ValueError, match="sum to one"):
        battery.aggregate_battery_classes(frame, {("ON", "medium_trucks"): {"Class 3 Medium Van": 0.2}}, years=[2025])


def regional_frames():
    classes = pd.DataFrame([
        {"year": 2025, "region": region, "road_class": road_class, "battery_kwh": size}
        for region, road_class, size in [("ON", "medium_trucks", 100), ("ON", "heavy_trucks", 400),
                                        ("QC", "medium_trucks", 200), ("QC", "heavy_trucks", 500)]
    ])
    activity = classes.drop(columns="battery_kwh").assign(activity_bn=[1.0, 3.0, 2.0, 4.0], units="bn tonne-km")
    return classes, activity


def test_pooling_uses_compatible_service_activity_without_implying_vehicle_count():
    classes, activity = regional_frames()
    aggregate, weights, pooled = battery.pool_batteries_by_activity(classes, activity)
    assert aggregate.set_index("road_class").loc["medium_trucks", "battery_kwh"] == pytest.approx(500 / 3)
    assert aggregate.set_index("road_class").loc["heavy_trucks", "battery_kwh"] == pytest.approx(3200 / 7)
    assert pooled.iloc[0].battery_kwh == 370
    assert weights.set_index("road_class").loc["heavy_trucks", "activity_share"] == pytest.approx(0.7)
    assert weights.activity_share.sum() == 1
    assert set(weights.units) == {"bn tonne-km"}


@pytest.mark.parametrize("change", ["units", "zero", "negative", "infinite", "duplicate", "missing"])
def test_pooling_rejects_incompatible_or_incomplete_activity(change):
    classes, activity = regional_frames()
    if change == "units":
        activity.loc[0, "units"] = "bn passenger-km"
    elif change in {"zero", "negative", "infinite"}:
        activity.loc[0, "activity_bn"] = {"zero": 0, "negative": -1, "infinite": float("inf")}[change]
    elif change == "duplicate":
        activity = pd.concat([activity, activity.iloc[[0]]], ignore_index=True)
    else:
        activity = activity.iloc[1:]
    with pytest.raises(ValueError):
        battery.pool_batteries_by_activity(classes, activity)


@pytest.fixture
def configured_battery_inputs(tmp_path, monkeypatch):
    frame = source_rows()
    atb_path = tmp_path / "vehicles.csv"
    frame.to_csv(atb_path, index=False)
    macro_path = tmp_path / "macro.csv"
    pd.DataFrame({"scenario": ["Current Measures"] * 3, "region": ["Canada"] * 3,
                  "variable_key": ["real_gdp"] * 3, "year": [2025, 2030, 2050],
                  "value": [100, 120, 200], "unit": ["index"] * 3}).to_csv(macro_path, index=False)
    for name in ("scenario.yaml", "paths.yaml", "sources.yaml", "rules.yaml", "truck_rules.yaml", "weights.csv", "ceud.csv"):
        (tmp_path / name).write_text("fixture", encoding="utf-8")
    bundle = SimpleNamespace(
        scenario_path=tmp_path / "scenario.yaml", paths_path=tmp_path / "paths.yaml",
        sources_path=tmp_path / "sources.yaml",
        scenario=SimpleNamespace(periods=SimpleNamespace(base_year=2025, step=5),
                                 geography=SimpleNamespace(regions=["ON"]),
                                 existing_capacity=SimpleNamespace(vehicle_population_year=2025)),
    )
    rules = {
        "nlr_atb_autonomie": {"interim_subdir": "atb", "components": {"vehicles": {
            "output_file": "vehicles.csv", "metric_keys": {"Useable Energy (kWh)": "usable_battery_capacity_kwh"}}}},
        "efficiencies": {"atb": {"weight_categories": {"mhdv": "Medium/Heavy Duty"}},
                         "mhdv_archetypes": {"bev": {"detail": "Battery Electric Vehicle"}}},
        "cer_enerfuture": {"interim_subdir_template": "cer/{edition}",
                           "components": {"macro-indicators": {"output_file": "macro.csv"}}},
        "road_stocks_and_demands": {"demand": {"region_output_map": {}}},
    }
    monkeypatch.setattr(battery, "load_harmonization_rules", lambda _, key: rules[key])
    monkeypatch.setattr(battery, "resolve_input_path", lambda _, layer, folder, filename: tmp_path / filename)
    monkeypatch.setattr(battery, "harmonization_rule_paths", lambda _: (
        tmp_path / "rules.yaml", tmp_path / "truck_rules.yaml"))
    monkeypatch.setattr(battery, "ceud_input_path", lambda *_: tmp_path / "ceud.csv")
    monkeypatch.setattr(battery, "configured_trajectory", lambda _: "Mid")
    monkeypatch.setattr(battery, "configured_edition", lambda _: 2023)
    monkeypatch.setattr(battery, "configured_scenario", lambda *_: "Current Measures")
    fleet = SimpleNamespace(paths=(tmp_path / "weights.csv",),
                            medium_weights=lambda region, classes: {"Class 3 Medium Van": 1},
                            heavy_weights=lambda region: {"Class 8 Longhaul Sleeper": 1})
    def load_truck_evidence(_, *, include_ldv):
        assert include_ldv is False
        return fleet

    monkeypatch.setattr(battery, "load_fleet_aggregation_evidence", load_truck_evidence)
    baseline = pd.DataFrame({"region": ["ON", "ON"], "service": ["medium_trucks", "heavy_trucks"],
                             "projected_base_year_demand": [10, 30], "units": ["bn tonne-km"] * 2})
    monkeypatch.setattr(battery, "road_base_year_activity", lambda *_, **__: (baseline, {"base_year": 2025}))
    return bundle


def test_load_reuses_current_owners_with_calendar_years_and_performs_no_writes(tmp_path, configured_battery_inputs):
    bundle = configured_battery_inputs
    before = {path: path.read_bytes() for path in tmp_path.iterdir()}

    result = battery.load_battery_evidence(bundle, years=[2025, 2030, 2050])

    assert result.pooled_battery.set_index("year").loc[2025, "battery_kwh"] == 325
    assert result.activity_weights.loc[result.activity_weights.year.eq(2050), "activity_bn"].tolist() == [60, 20]
    assert set(result.component_audit.year) == {2025, 2030, 2050}
    assert result.metadata["aggregation_population_year"] == 2025
    assert "not BEV, vehicle-count or charging-session" in result.metadata["activity_weight_basis"]
    assert all(len(item["sha256"]) == 64 for item in result.metadata["files"])
    assert {item["path"] for item in result.metadata["configuration"]} >= {
        str(tmp_path / "rules.yaml"), str(tmp_path / "truck_rules.yaml"),
    }
    assert {path: path.read_bytes() for path in tmp_path.iterdir()} == before


@pytest.mark.parametrize("known_coverage", [True, False])
def test_freight_readiness_preserves_reason_and_only_known_coverage_gets_preparation(
        configured_battery_inputs, monkeypatch, known_coverage):
    bundle = configured_battery_inputs
    reason = "Incomplete freight haul evidence for AB" if known_coverage else "Invalid independent scientific quantity"

    def unavailable(region):
        raise ValueError(reason)

    fleet = SimpleNamespace(medium_weights=lambda region, classes: {"Class 3 Medium Van": 1},
                            heavy_weights=unavailable)
    def load_truck_evidence(_, *, include_ldv):
        assert include_ldv is False
        return fleet

    monkeypatch.setattr(battery, "load_fleet_aggregation_evidence", load_truck_evidence)

    with pytest.raises(ValueError, match=reason) as caught:
        battery.load_battery_evidence(bundle, years=[2025, 2030, 2050])

    assert ("Prepare with:" in str(caught.value)) is known_coverage
    if known_coverage:
        assert "uv run python src/build_transport.py --scenario" in str(caught.value)
        assert bundle.scenario_path.as_posix() in str(caught.value)
