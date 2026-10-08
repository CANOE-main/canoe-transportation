from dataclasses import replace
import sqlite3

from canoe_schema.v4_0 import (
    DataSet,
    CapacityFactorTech,
    TimeSeason,
    TimeOfDay,
    TechnologyLabel,
)
import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from build_transport import (
    prepare_transport_contribution,
    insert_transport_contribution,
)
from fetching.legacy_charging_profiles import (
    ProfileEvidence,
    build_request,
    transform_hourly,
)
from parameterization.ldv_charging_profiles import (
    prepare_charging_profile_rows,
    project_hours,
)
from utils import load_config_bundle, resolve_input_path
from validation.config_models import ScenarioConfig
from validation.insertion import insert_models
from validation.schema_contract import create_v4_schema


@pytest.fixture
def bundle():
    b = load_config_bundle("config/scenarios/legacy_reproduction.yaml")
    payload = b.scenario.model_dump()
    payload["geography"]["regions"] = ["ON"]
    payload["embodied_emissions"] = False
    return replace(b, scenario=ScenarioConfig.model_validate(payload))


@pytest.fixture
def evidence(bundle):
    number = np.arange(8760)
    hourly = pd.DataFrame(
        dict(
            hour_index=number,
            season=[f"D{i // 24 + 1:03d}" for i in number],
            tod=[f"H{i % 24 + 1:02d}" for i in number],
            factor=(1 + number % 24) / 24,
        )
    )
    return ProfileEvidence(
        hourly, {"profile": "nhts"}, build_request(bundle, "nhts").expected_sha256
    )


@pytest.mark.parametrize(
    "profile,peak,total",
    [
        ("nhts", 1161.9009046687445, 4911640.616091287),
        ("tts", 1029.305808138049, 4051710.331486839),
    ],
)
def test_native_hourly_equivalence_peak_energy_dst_and_embedded_composition(
    bundle, profile, peak, total
):
    request = build_request(bundle, profile)
    raw = pd.read_csv(request.registered_path, index_col=0)
    result = transform_hourly(
        raw,
        year=2018,
        timezone="America/Toronto",
        value_column="Charging Profile",
        precision=10,
    )
    legacy = raw.copy()
    legacy.index = pd.to_datetime(legacy.index, utc=True).tz_convert("America/Toronto")
    legacy = legacy[legacy.index.year == 2018].resample("h").mean()
    expected = (legacy.iloc[:, 0] / legacy.iloc[:, 0].max()).round(10)
    np.testing.assert_array_equal(result.hourly.factor, expected)
    assert len(result.hourly) == 8760
    assert result.audit["annual_hourly_peak"] == peak
    assert result.audit["hourly_integral"] == pytest.approx(total, abs=1e-8)
    assert result.audit["excluded_nulls_outside_local_year"] == 300
    assert result.audit["repeated_wall_time_keys"] == 1
    assert not result.hourly.duplicated(["season", "tod"]).any()
    assert result.audit["embedded_range_shares"]
    assert not result.audit["market_share_reweighting"]


def test_invalid_native_samples_or_resolution_are_not_filled(bundle):
    request = build_request(bundle, "nhts")
    raw = pd.read_csv(request.registered_path, index_col=0)
    raw.iloc[1000, 0] = np.nan
    with pytest.raises(ValueError, match="missing/nonfinite"):
        transform_hourly(
            raw,
            year=2018,
            timezone="America/Toronto",
            value_column="Charging Profile",
            precision=10,
        )
    with pytest.raises(ValueError, match="one-minute"):
        transform_hourly(
            raw.iloc[::2],
            year=2018,
            timezone="America/Toronto",
            value_column="Charging Profile",
            precision=10,
        )


def test_explicit_inherited_projection_preserves_integral_and_checks_durations(
    evidence,
):
    seasons = [TimeSeason(season="annual", segment_fraction=1)]
    times = [TimeOfDay(tod=f"H{i + 1:02d}", hours=1) for i in range(24)]
    mapping = evidence.hourly[["hour_index", "season", "tod"]].assign(season="annual")
    projected, _ = project_hours(
        evidence, seasons=seasons, times_of_day=times, mapping=mapping, tolerance=1e-8
    )
    assert projected.physical_hours.tolist() == [365] * 24
    assert np.dot(projected.factor, projected.physical_hours) == pytest.approx(
        evidence.hourly.factor.sum()
    )
    with pytest.raises(ValueError, match="label coverage"):
        project_hours(
            evidence, seasons=seasons, times_of_day=times, mapping=None, tolerance=1e-8
        )
    bad = times.copy()
    bad[0] = TimeOfDay(tod="H01", hours=2)
    with pytest.raises(ValueError, match="durations"):
        project_hours(
            evidence, seasons=seasons, times_of_day=bad, mapping=mapping, tolerance=1e-8
        )
    mapping.iloc[-1, 0] = 0
    with pytest.raises(ValueError, match="every physical hour"):
        project_hours(
            evidence,
            seasons=seasons,
            times_of_day=times,
            mapping=mapping,
            tolerance=1e-8,
        )


def test_preparation_can_reuse_evidence_and_has_complete_dq_and_labels(
    bundle, evidence, monkeypatch
):
    def fail(*a, **kw):
        raise AssertionError("unnecessary evidence rebuild")

    monkeypatch.setattr(
        "parameterization.ldv_charging_profiles.normalize_profile", fail
    )
    result = prepare_charging_profile_rows(bundle, evidence=evidence, publish=False)
    assert len(result.rows) == 17520
    assert len(result.seasons) == 365 and len(result.times_of_day) == 24
    assert all(r.tech in {"T_LDV_CHRG_EX", "T_LDV_CHRG_N"} for r in result.rows)
    assert all(
        r.data_source == "T31" and r.dq_cred == 5 and r.dq_time == 5
        for r in result.rows
    )


def contribution(connection, bundle, charging=None):
    return prepare_transport_contribution(
        connection,
        bundle=bundle,
        template_dir=resolve_input_path(bundle, "template"),
        include_existing_capacity=False,
        include_demand=False,
        include_road_utilization=False,
        include_lifetimes=False,
        include_efficiencies=False,
        include_costs=False,
        include_emission_embodied=False,
        charging_profiles=charging,
    )


def test_caller_insertion_preserves_shared_axes_other_rows_repeats_switches_and_rolls_back(
    bundle, evidence
):
    c = sqlite3.connect(":memory:", isolation_level=None)
    create_v4_schema(c)
    result = prepare_charging_profile_rows(bundle, evidence=evidence, publish=False)
    # Caller-owned axes have their own sequence/notes; preparation must inherit them.
    seasons = [s.model_copy(update={"notes": "caller-owned"}) for s in result.seasons]
    insert_models(c, seasons)
    insert_models(c, result.times_of_day)
    inherited = prepare_charging_profile_rows(
        bundle,
        evidence=evidence,
        seasons=seasons,
        times_of_day=result.times_of_day,
        publish=False,
    )
    assert not inherited.seasons and not inherited.times_of_day
    insert_models(c, [DataSet(data_id="other", label="other sector")])
    insert_models(c, [TechnologyLabel(tech="OTHER")])
    unrelated = CapacityFactorTech(
        region="ON", season="D001", tod="H01", tech="OTHER", factor=0.4, data_id="other"
    )
    insert_models(c, [unrelated])
    before = c.execute("SELECT * FROM time_season").fetchall()
    c.execute("BEGIN")
    prepared = contribution(c, bundle, inherited)
    insert_transport_contribution(c, prepared)
    assert c.in_transaction
    insert_transport_contribution(c, prepared, conflict="ignore_identical")
    assert c.execute("SELECT COUNT(*) FROM capacity_factor_tech").fetchone()[0] == 17521
    tts_selection = bundle.scenario.charging_profiles.model_copy(
        update={"travel_behavior_source": "tts"}
    )
    tts_bundle = replace(
        bundle,
        scenario=bundle.scenario.model_copy(
            update={"charging_profiles": tts_selection}
        ),
    )
    tts_evidence = ProfileEvidence(
        evidence.hourly.assign(factor=evidence.hourly.factor * 0.5),
        {"profile": "tts"},
        build_request(bundle, "tts").expected_sha256,
    )
    changed_profile = prepare_charging_profile_rows(
        tts_bundle,
        evidence=tts_evidence,
        seasons=seasons,
        times_of_day=result.times_of_day,
        publish=False,
    )
    changed = contribution(c, tts_bundle, changed_profile)
    insert_transport_contribution(c, changed, conflict="ignore_identical")
    assert c.execute("SELECT COUNT(*) FROM capacity_factor_tech").fetchone()[0] == 17521
    payload = bundle.scenario.model_dump()
    payload["charging_profiles"]["travel_behavior_source"] = "none"
    disabled = replace(bundle, scenario=ScenarioConfig.model_validate(payload))
    insert_transport_contribution(
        c, contribution(c, disabled), conflict="ignore_identical"
    )
    assert c.execute("SELECT COUNT(*) FROM capacity_factor_tech").fetchone()[0] == 1
    assert c.execute("SELECT * FROM time_season").fetchall() == before
    assert c.execute("PRAGMA foreign_key_check").fetchall() == []
    c.rollback()
    assert c.execute("SELECT COUNT(*) FROM technology").fetchone()[0] == 0
    assert c.execute("SELECT COUNT(*) FROM capacity_factor_tech").fetchone()[0] == 1
    c.close()


def test_unrelated_row_conflict_and_incomplete_preparation_fail_before_cleanup(
    bundle, evidence
):
    c = sqlite3.connect(":memory:", isolation_level=None)
    create_v4_schema(c)
    result = prepare_charging_profile_rows(bundle, evidence=evidence, publish=False)
    insert_models(c, result.seasons)
    insert_models(c, result.times_of_day)
    insert_models(c, [TechnologyLabel(tech="T_LDV_CHRG_N")])
    insert_models(c, [DataSet(data_id="other", label="shared")])
    insert_models(
        c,
        [
            result.rows[8760].model_copy(
                update={"data_id": "other", "data_source": None}
            )
        ],
    )
    prepared = contribution(c, bundle, result)
    c.execute("BEGIN")
    with pytest.raises(ValueError, match="unrelated/shared"):
        insert_transport_contribution(c, prepared)
    assert c.execute("SELECT COUNT(*) FROM data_set").fetchone()[0] == 1
    with pytest.raises(ValueError, match="coverage mismatch"):
        insert_transport_contribution(
            c,
            replace(prepared, charging_profiles=replace(result, rows=result.rows[:-1])),
        )
    mutated = [r.model_copy(update={"factor": r.factor * 0.5}) for r in result.rows]
    with pytest.raises(ValueError, match="preserved hourly"):
        insert_transport_contribution(
            c, replace(prepared, charging_profiles=replace(result, rows=mutated))
        )
    c.rollback()


def test_all_regions_use_shared_chargers_without_application_selectors(
    bundle, evidence
):
    payload = bundle.scenario.model_dump()
    payload["geography"]["regions"] = ["ON", "QC"]
    b = replace(bundle, scenario=ScenarioConfig.model_validate(payload))
    result = prepare_charging_profile_rows(b, evidence=evidence, publish=False)
    assert len(result.rows) == 35040
    assert {r.region for r in result.rows} == {"ON", "QC"}
    assert {r.tech for r in result.rows} == {"T_LDV_CHRG_EX", "T_LDV_CHRG_N"}
    assert set(b.scenario.charging_profiles.model_dump()) == {
        "travel_behavior_source",
        "time_mapping",
    }


@pytest.mark.parametrize(
    "key,value",
    [
        ("profile", "nhts"),
        ("regional_application", "ontario_only"),
        ("technology_application", "bev_only"),
        ("time_mapping", ""),
    ],
)
def test_obsolete_charging_selectors_and_empty_mapping_are_rejected(bundle, key, value):
    payload = bundle.scenario.model_dump()
    payload["charging_profiles"][key] = value
    with pytest.raises(ValidationError):
        ScenarioConfig.model_validate(payload)


def test_mapping_csv_and_supplied_mapping_use_the_same_projection(
    bundle, evidence, tmp_path
):
    seasons = [TimeSeason(season="annual", segment_fraction=1)]
    times = [TimeOfDay(tod=f"H{i + 1:02d}", hours=1) for i in range(24)]
    mapping = evidence.hourly[["hour_index", "season", "tod"]].assign(season="annual")
    path = tmp_path / "time_mapping.csv"
    mapping.to_csv(path, index=False)
    payload = bundle.scenario.model_dump()
    payload["charging_profiles"]["time_mapping"] = str(path)
    b = replace(bundle, scenario=ScenarioConfig.model_validate(payload))
    from_csv = prepare_charging_profile_rows(
        b, evidence=evidence, seasons=seasons, times_of_day=times, publish=False
    )
    supplied = prepare_charging_profile_rows(
        b,
        evidence=evidence,
        seasons=seasons,
        times_of_day=times,
        time_mapping=mapping,
        publish=False,
    )
    assert [r.model_dump() for r in from_csv.rows] == [
        r.model_dump() for r in supplied.rows
    ]
    assert from_csv.audit["projection_sha256"] == supplied.audit["projection_sha256"]
    assert len(from_csv.rows) == 48
    assert not from_csv.seasons and not from_csv.times_of_day


def test_existing_capacity_preparation_keeps_charging_provenance(
    bundle, evidence, monkeypatch
):
    from canoe_schema.v4_0 import ExistingCapacity
    from validation.insertion import validate_parameter_rows
    from validation.provenance import resolve_provenance

    context = resolve_provenance(
        bundle.sources,
        source_key="nrcan_ceud_transport_provincial",
        component_key=21,
        transformation="Test stock prerequisite",
        transformation_version="1",
    )
    rows = validate_parameter_rows(
        ExistingCapacity,
        [
            {
                "region": "ON",
                "tech": "T_LDV_C_BEV_EX",
                "vintage": 2020,
                "capacity": 1,
                "units": "k vehicles",
            }
        ],
        context,
    )
    monkeypatch.setattr(
        "parameterization.build_existing_capacity.prepare_existing_capacity_rows",
        lambda b: (rows, [context], {}),
    )
    charging = prepare_charging_profile_rows(bundle, evidence=evidence, publish=False)
    c = sqlite3.connect(":memory:")
    create_v4_schema(c)
    result = prepare_transport_contribution(
        c,
        bundle=bundle,
        template_dir=resolve_input_path(bundle, "template"),
        include_ev_chargers=False,
        include_demand=False,
        include_road_utilization=False,
        include_lifetimes=False,
        include_efficiencies=False,
        include_costs=False,
        include_emission_embodied=False,
        charging_profiles=charging,
    )
    assert {x.source_id for x in result.provenance_contexts} == {"T01", "T31"}
    c.close()
