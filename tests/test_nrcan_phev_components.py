"""NRCan charge-depleting component parsing preserves electricity and liquid fuel."""

from pathlib import Path

import pandas as pd
import pytest

from parameterization.ldv_ev_ranges import module_rules as range_module_rules
from parameterization.road_efficiencies import (
    classify_ratings,
    distinct_rating_models,
    parse_nrcan_phev_cd_components,
)
from utils import load_config_bundle, load_conversion_factors, load_harmonization_rules


@pytest.fixture(scope="module")
def bundle():
    return load_config_bundle(
        "config/scenarios/notebook_diagnostics.yaml",
        repo_root=Path(__file__).resolve().parents[1],
    )


@pytest.fixture(scope="module")
def contract(bundle):
    return load_harmonization_rules(bundle, "nlr_atb_autonomie")["components"][
        "phev_efficiency"
    ]["future_nrcan_phev_contract"]


def test_cd_components_keep_explicit_electricity_and_liquid_fuel(contract):
    raw = pd.Series(
        [
            "2.5 (22.3 kWh/100 km)",
            "2.7 ([23.2 kWh + 0.1 L]/100 km)",
            "4.4 ([38.7 kWh + 0.0 L]/100 km)",
            "7.3 ([35.1 kWh + 3.4 L]/100 km)",
        ],
        index=[12, 44, 58, 90],
        name="Combined Le/100 km",
    )
    original = raw.copy(deep=True)
    result = parse_nrcan_phev_cd_components(raw, contract=contract)
    pd.testing.assert_frame_equal(
        result,
        pd.DataFrame(
            {
                "cd_electricity_kwh_per_100km": [22.3, 23.2, 38.7, 35.1],
                "cd_fuel_l_per_100km": [0.0, 0.1, 0.0, 3.4],
            },
            index=raw.index,
        ),
    )
    pd.testing.assert_series_equal(raw, original)


@pytest.mark.parametrize(
    "unsupported",
    [
        None,
        float("nan"),
        "2.5",
        "2.5 Le/100 km",
        "2.5 (22.3 kWh/100 km) unexpected",
        "2.5 (22.3 Wh/100 km)",
        "2.5 ([22.3 kWh/100 km)",
        "2.5 (22.3 kWh + 0.1 L/100 km)",
        "2.5 ([22.3 kWh]/100 km)",
        "2.5 ([-22.3 kWh + 0.1 L]/100 km)",
        "2.5 ([22.3 kWh + -0.1 L]/100 km)",
        "2.5 ([inf kWh + 0.1 L]/100 km)",
    ],
)
def test_cd_components_reject_incomplete_or_unsupported_source_text(contract, unsupported):
    raw = pd.Series(["2.5 (22.3 kWh/100 km)", unsupported], index=[10, 27])
    with pytest.raises(ValueError, match=r"Unsupported NRCan PHEV CD.*1 rows.*27"):
        parse_nrcan_phev_cd_components(raw, contract=contract)


def test_cd_components_reject_numeric_overflow(contract):
    raw = pd.Series([f"2.5 ({'9' * 400} kWh/100 km)"])
    with pytest.raises(ValueError, match="finite and nonnegative"):
        parse_nrcan_phev_cd_components(raw, contract=contract)


def test_cd_components_require_configured_component_captures(contract):
    changed = {**contract, "combined_cd_components_pattern": r"(?P<wrong>.*)"}
    with pytest.raises(ValueError, match="missing captures"):
        parse_nrcan_phev_cd_components(pd.Series(["2.5 (22.3 kWh/100 km)"]), contract=changed)


@pytest.fixture
def rules(bundle):
    return load_harmonization_rules(bundle, "efficiencies")


def test_classifier_filters_model_means_after_source_concatenation(rules, bundle):
    source = pd.DataFrame([
        {"source_row": index, "component": "conventional", "Model year": 2023,
         "Make": "Fixture", "Model": model, "Vehicle class": "Compact",
         "Fuel type": "X", "Combined (L/100 km)": consumption, "Engine size (L)": 12}
        for index, model, consumption in [(2, "A", 2), (3, "B", 10), (4, "C", 11),
                                          (5, "D", 12), (6, "E", 13), (7, "F", 14),
                                          (8, "G", 40), (9, "G", 50)]
    ])
    def classify(frames):
        return classify_ratings(
            frames, pd.DataFrame(columns=rules["ratings"]["evidence_columns"]),
            rules=rules, conversions=load_conversion_factors(bundle), range_rules=range_module_rules(bundle),
        )
    result = classify([source])
    partitioned = classify([source.iloc[:4].copy(), source.iloc[4:].copy()])
    pd.testing.assert_frame_equal(result, partitioned)
    assert len(result) == 8
    assert result.included.sum() == 5
    assert set(result.loc[result.outlier_excluded, "Model"]) == {"A", "G"}
    assert result.outlier_cohort_model_count.eq(7).all()
    assert result["Engine size (L)"].eq(12).all()
    assert not {"engine_litres", "motor_kw", "cylinders", "performance_excluded"} & set(result)
    models = distinct_rating_models(result, rules=rules)
    assert models.Model.tolist() == ["B", "C", "D", "E", "F"]
    assert models.native_consumption.tolist() == [10, 11, 12, 13, 14]


def _classify_phev(rules, bundle, contract, utility_factors):
    source = pd.DataFrame([{
        "source_row": 2, "component": "phev", "Model year": 2023,
        "Make": "Fixture", "Model": "PHEV", "Vehicle class": "Compact", "Range 1 (km)": 50,
        "Combined (L/100 km)": 6.1, "Combined Le/100 km": "2.7 ([23.2 kWh + 0.1 L]/100 km)",
        "Fuel type 2": "X",
    }])
    return classify_ratings(
        [source], pd.DataFrame(columns=rules["ratings"]["evidence_columns"]),
        rules=rules, conversions=load_conversion_factors(bundle), range_rules=range_module_rules(bundle),
        phev_contract=contract, uf_by_powertrain=utility_factors,
    ).iloc[0]


@pytest.mark.parametrize("uf", [0.0, 0.5699, 1.0])
def test_phev_total_energy_includes_cd_gasoline_and_cs_without_double_counting(rules, bundle, contract, uf):
    result = _classify_phev(rules, bundle, contract, {"phev_35_miles": uf})
    hhv = load_conversion_factors(bundle)["fuel_hhv"]["gasoline_mj_per_litre"]
    electricity = uf * 23.2 * 3.6 / 100
    fuel = (uf * 0.1 + (1 - uf) * 6.1) * hhv / 100
    assert result.utility_weighted_electricity_mj_per_vkm == pytest.approx(electricity)
    assert result.utility_weighted_fuel_mj_per_vkm == pytest.approx(fuel)
    assert result.consumption_mj_per_vkm == pytest.approx(electricity + fuel)
    assert result.native_consumption == 2.7
    assert result.cs_fuel_l_per_100km == 6.1
    assert result.fleet_utility_factor == uf
    assert result.utility_factor_basis == "ATB_range_bucket"


@pytest.mark.parametrize(
    "utility_factors",
    [None, {}, {"phev_50_miles": 0.6887}, {"phev_35_miles": -0.1},
     {"phev_35_miles": 1.1}, {"phev_35_miles": float("nan")}, {"phev_35_miles": float("inf")}],
)
def test_phev_total_energy_requires_complete_valid_explicit_utility_factors(rules, bundle, contract, utility_factors):
    with pytest.raises(ValueError, match="utility factors"):
        _classify_phev(rules, bundle, contract, utility_factors)
