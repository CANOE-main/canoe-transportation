import pandas as pd
import pytest

from parameterization.stocks_and_demands import (
    derive_ldv_age_distributions,
    median_lifetime_map,
)


def mapped_stock_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "report_year": [2025] * 6,
            "MODEL_YEAR": [2025, 2011, 2010, 1994, 2026, 2000],
            "FIT_ACTIVE": [10, 20, 30, 40, 50, 60],
            "nrcan_ceud_class": [
                "Car",
                "Car",
                "Car",
                "Car",
                "Light Truck",
                "Light Truck",
            ],
            "nlr_atb_class": [
                "Compact",
                "Compact",
                "Compact",
                "Compact",
                "Small SUV",
                "Small SUV",
            ],
            "mapping_accepted": [True] * 6,
        }
    )


def test_median_mode_truncates_by_class_and_reports_exclusions() -> None:
    distribution, findings = derive_ldv_age_distributions(
        mapped_stock_fixture(),
        survival_curves=False,
        maximum_age=30,
        median_lifetimes={"Car": 14, "Light Truck": 15},
    )

    car = distribution.loc[distribution["nrcan_ceud_class"].eq("Car")]
    assert car["age"].tolist() == [0, 14]
    light_truck = findings.loc[
        findings["nrcan_ceud_class"].eq("Light Truck")
    ].iloc[0]
    assert light_truck["negative_age_fit_active_stock"] == 50
    assert light_truck["over_cutoff_fit_active_stock"] == 60
    assert findings.loc[
        findings["nrcan_ceud_class"].eq("Car"),
        "pre_2000_fit_active_stock",
    ].item() == 40
    assert car["age_distribution"].sum() == pytest.approx(1.0)


def test_survival_curve_mode_uses_configured_maximum_age() -> None:
    distribution, findings = derive_ldv_age_distributions(
        mapped_stock_fixture(),
        survival_curves=True,
        maximum_age=30,
        median_lifetimes={"Car": 14, "Light Truck": 15},
    )

    car = distribution.loc[distribution["nrcan_ceud_class"].eq("Car")]
    assert car["age"].tolist() == [0, 14, 15]
    assert set(findings["cutoff_basis"]) == {"scenario_survival_curve_max_age"}
    assert set(findings["cutoff_age"]) == {30.0}


def test_median_lifetime_map_rejects_duplicate_ceud_rows() -> None:
    medians = pd.DataFrame(
        {
            "target_system": ["nrcan_ceud", "nrcan_ceud"],
            "target_class": ["Car", "Car"],
            "median_equivalent_age": [14, 15],
        }
    )

    with pytest.raises(ValueError, match="Duplicate"):
        median_lifetime_map(medians)
