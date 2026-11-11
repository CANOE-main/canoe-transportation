"""Cost equations preserve reviewed source units and the MSRP basis."""

from pathlib import Path
from math import exp

import pandas as pd
import pytest

from parameterization.road_capex_opex import bean_maintenance_per_mile, ldv_repair_per_mile


ROOT = Path(__file__).resolve().parents[1]
ATB = ROOT / "inputs/1_interim/fetched_nlr_atb_autonomie"


def test_burnham_uses_msrp_and_caps_age() -> None:
    frames = {
        "baseline": pd.read_csv(ATB / "atb_maintenance_ldv_baseline_repair_cost.csv"),
        "class_multipliers": pd.read_csv(ATB / "atb_maintenance_ldv_class_multipliers.csv"),
        "powertrain_multipliers": pd.read_csv(ATB / "atb_maintenance_ldv_powertrain_multipliers.csv"),
        "maintenance": pd.read_csv(ATB / "atb_maintenance_ldv_maintenance_cost.csv"),
    }
    kwargs = dict(
        vehicle_class="Compact", powertrain="gasoline", **frames,
        powertrain_labels={"gasoline": "Gasoline"}, price_exponent=0.00002,
        max_age=14,
    )
    cost = ldv_repair_per_mile(age=14, msrp=30000, **kwargs)
    baseline = float(frames["baseline"].loc[
        frames["baseline"].year_index.eq(14), "baseline_repair_cost($/mi)"
    ].iloc[0])
    assert cost["repair"] == pytest.approx(baseline * exp(0.00002 * 30000))
    assert ldv_repair_per_mile(age=30, msrp=30000, **kwargs) == cost
    assert ldv_repair_per_mile(age=14, msrp=20000, **kwargs)["repair"] < cost["repair"]


def test_bean_uses_registered_class_coefficients() -> None:
    data = pd.read_csv(ATB / "anl_bean_mhdv_maintenance_coefficients.csv")
    result = bean_maintenance_per_mile(
        data, vehicle_class="BoxMedium 4", powertrain="Conv", age=5
    )
    assert result["source_cost_per_mile"] == pytest.approx(0.09 * 5 + 0.2618)
    with pytest.raises(ValueError, match="Incomplete BEAN"):
        bean_maintenance_per_mile(
            data, vehicle_class="BoxMedium 3", powertrain="Conv", age=5
        )
