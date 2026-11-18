"""Registered offline builds preserve common road timing, weights and insertion."""

from pathlib import Path
import sqlite3

import pandas as pd
import pytest
import yaml

from build_transport import build_from_scenario
from utils import file_sha256, load_config_bundle, load_harmonization_rules, resolve_artifact_path


@pytest.mark.parametrize("period_mode,curves,base_year", [
    ("legacy", False, 2023), ("prospective", True, 2023), ("prospective", True, 2025),
])
def test_registered_road_cohorts_share_weights_and_period_timing(
    isolated_legacy_bundle, period_mode, curves, base_year,
):
    """Run the supported lifecycle with private registered inputs and no network."""
    original = isolated_legacy_bundle
    payload = original.scenario.model_dump(mode="json", by_alias=True)
    payload["geography"]["regions"] = ["ON", "NL"]
    payload["periods"].update(
        period_mode=period_mode,
        base_year=base_year,
        existing=([2000, 2005, 2010, 2015, 2020, 2023] if period_mode == "legacy"
                  else [2000, 2005, 2010, 2015, 2020]),
    )
    payload["sources"]["nrcan_ceud_beyond_observed"] = "constant" if base_year > 2023 else "error"
    payload["lifetimes"]["survival_curves"] = curves
    payload["comparison"]["mode"] = "none"
    payload["outputs"]["sqlite_name"] = f"road_cohorts_{period_mode}_{base_year}.sqlite"
    payload["outputs"]["validation_report"] = f"outputs/validation/road_cohorts_{period_mode}_{base_year}.json"
    original.scenario_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    report, report_path = build_from_scenario(original.scenario_path)
    assert report["ok"]
    assert report_path.is_file()
    assert report["parameter_support"]["ok"], report["parameter_support"]
    assert report["orchestration"] == {"prerequisites": "replayed", "download_sources": False}
    bundle = load_config_bundle(original.scenario_path)
    rules = load_harmonization_rules(bundle, "efficiencies")
    road_rules = load_harmonization_rules(bundle, "road_stocks_and_demands")["existing_capacity"]
    stock_path = resolve_artifact_path(bundle, "existing_capacity_interim", road_rules["annual_stock_file"])
    assert stock_path.is_relative_to(bundle.repo_root)
    descriptor = report["existing_capacity"]["annual_stock_evidence"]
    assert (bundle.repo_root / Path(descriptor["path"])).resolve() == stock_path.resolve()
    assert descriptor["sha256"] == file_sha256(stock_path)
    assert report["efficiencies"]["annual_stock_evidence"] == descriptor
    assert report["existing_capacity"]["annual_stock_outside_historical_coverage"]["cohort_rows"] > 0
    stock = pd.read_csv(stock_path)
    assert not stock.duplicated(["region", "tech", "vintage_year"]).any()
    assert set(stock.vintage) <= set(payload["periods"]["existing"])
    annual = pd.read_csv(resolve_artifact_path(bundle, "efficiencies_interim", rules["files"]["annual"]))
    periods = pd.read_csv(resolve_artifact_path(bundle, "efficiencies_interim", rules["files"]["periods"]))
    historical = annual.loc[
        annual["mode"].isin(rules["road_classes"])
        & annual.tech.str.endswith("_EX")
        & annual.range_representative_component.ne(True)
    ]
    assert set(historical["mode"]) == set(rules["road_classes"])
    assert historical.temporal_weight_basis.eq("age_distribution_cohort_stock").all()
    new_classes = {"school_buses", "urban_transit", "inter_city_buses", "motorcycles"}
    assert new_classes <= set(historical["mode"])
    for (region, tech, vintage), observations in historical.groupby(["region", "tech", "vintage"]):
        # Enumerate the expected period support independently of historical_years.
        labels = payload["periods"]["existing"]
        index = labels.index(vintage)
        if period_mode == "legacy":
            first = labels[index - 1] + 1 if index else vintage - 4
            last = vintage
        else:
            first = vintage + 1 if index else vintage
            last = min(labels[index + 1] if index + 1 < len(labels) else 2025, base_year)
        assert observations.source_year.between(first, last).all()
        raw = stock.loc[stock.region.eq(region) & stock.tech.eq(tech) & stock.vintage.eq(vintage)
                        & stock.vintage_year.between(first, last) & stock.raw_capacity.gt(0)]
        observed_stock = observations.merge(
            raw[["vintage_year", "raw_capacity"]], left_on="source_year", right_on="vintage_year",
            validate="one_to_one",
        )
        assert len(observed_stock) == len(observations)
        assert observed_stock.cohort_stock_k_vehicles.tolist() == pytest.approx(observed_stock.raw_capacity)
        assert observed_stock.cohort_weight.tolist() == pytest.approx(
            observed_stock.raw_capacity / raw.raw_capacity.sum(),
        )
        assert observations.temporal_weight.sum() == pytest.approx(1)
        if observations["mode"].iloc[0] in new_classes:
            selected = observations.loc[observations.included_in_period]
            expected = ((selected.load_factor * selected.temporal_weight).sum()
                        / (selected.native_intensity * selected.temporal_weight).sum())
            period = periods.loc[periods.region.eq(region) & periods.tech.eq(tech)
                                 & periods.vintage.eq(vintage)].iloc[0]
            assert period.efficiency == pytest.approx(expected)
    future = annual.loc[annual["mode"].isin(rules["road_classes"]) & annual.tech.str.endswith("_N")]
    assert set(future["mode"]) == set(rules["road_classes"])
    assert future.cohort_stock_k_vehicles.isna().all()
    if period_mode == "legacy":
        assert future.evaluation_window_end.eq(future.vintage).all()
        assert future.temporal_weight_basis.eq("new_cohort_equal_supported_years").all()
        for (_, _, vintage), rows in future.groupby(["region", "tech", "vintage"]):
            assert sorted(rows.evaluation_year) == list(range(vintage - 4, vintage + 1))
            assert rows.loc[rows.included_in_period, "temporal_weight"].tolist() == pytest.approx(
                [1 / rows.included_in_period.sum()] * rows.included_in_period.sum()
            )
            assert rows.loc[~rows.included_in_period, "temporal_weight"].eq(0).all()
    else:
        assert (future.evaluation_year == future.vintage + 5).all()
        assert future.temporal_weight.eq(1).all()
        assert future.temporal_weight_basis.eq("future_interval_end").all()
    with sqlite3.connect(report["database"]) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute(
            "SELECT COUNT(*) FROM existing_capacity WHERE tech GLOB '*_N'"
        ).fetchone() == (0,)
        inserted = pd.read_sql_query(
            "SELECT e.region,e.tech,e.vintage,e.efficiency FROM efficiency e "
            "JOIN technology t ON e.tech=t.tech WHERE t.category IN "
            "('school_buses','urban_transit','inter_city_buses','motorcycles')",
            connection,
        )
        expected = periods.loc[periods["mode"].isin(new_classes), ["region", "tech", "vintage", "efficiency"]]
        comparison = inserted.merge(expected, on=["region", "tech", "vintage"], validate="one_to_one")
        assert len(comparison) == len(inserted) == len(expected)
        assert comparison.efficiency_x.tolist() == pytest.approx(comparison.efficiency_y)
