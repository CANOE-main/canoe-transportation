"""Cost rows keep new-only investment and two-dimensional variable indexing."""

import pytest
from canoe_schema.v4_0 import CostInvest, CostVariable

from parameterization.build_costs import validate_cost_outputs


def test_schema_cost_dimensions_and_coverage() -> None:
    investment = CostInvest(
        region="ON", tech="T_LDV_C_GSL_N", vintage=2025, cost=20.0,
        units="$M 2020CAD / k vehicles", data_id="test-cost", data_source="T01",
        dq_cred=5, dq_geog=5, dq_struc=5, dq_tech=5, dq_time=5,
    )
    variable = [
        CostVariable(
            region="ON", period=period, tech="T_LDV_C_GSL_N", vintage=2025,
            cost=4.0 + period - 2025, units="$M 2020CAD / bn passenger-km",
            data_id="test-cost", data_source="T01",
            dq_cred=5, dq_geog=5, dq_struc=5, dq_tech=5, dq_time=5,
        )
        for period in (2025, 2030)
    ]
    assert validate_cost_outputs(
        [investment], variable,
        expected_invest={("ON", "T_LDV_C_GSL_N", 2025)},
        expected_variable={
            ("ON", 2025, "T_LDV_C_GSL_N", 2025),
            ("ON", 2030, "T_LDV_C_GSL_N", 2025),
        },
        reference_year=2020,
    )["period_vintage_pairs"] == 2
    with pytest.raises(ValueError, match="Cost coverage mismatch"):
        validate_cost_outputs(
            [investment], variable[:1],
            expected_invest={("ON", "T_LDV_C_GSL_N", 2025)},
            expected_variable={
                ("ON", 2025, "T_LDV_C_GSL_N", 2025),
                ("ON", 2030, "T_LDV_C_GSL_N", 2025),
            },
            reference_year=2020,
        )
    wrong_unit = variable[0].model_copy(update={"units": "$M 2020CAD / k vehicles"})
    with pytest.raises(ValueError, match="per service activity"):
        validate_cost_outputs(
            [investment], [wrong_unit, variable[1]],
            expected_invest={("ON", "T_LDV_C_GSL_N", 2025)},
            expected_variable={
                ("ON", 2025, "T_LDV_C_GSL_N", 2025),
                ("ON", 2030, "T_LDV_C_GSL_N", 2025),
            }, reference_year=2020,
        )


@pytest.mark.parametrize(("fixed", "curves"), [(None, []), ([], None), (None, None)])
def test_curve_cost_handoff_rejects_unmatched_capacity_before_io(fixed, curves):
    from types import SimpleNamespace
    from parameterization.build_costs import prepare_cost_rows

    bundle = SimpleNamespace(scenario=SimpleNamespace(lifetimes=SimpleNamespace(survival_curves=True)))
    with pytest.raises(ValueError, match="matching prepared lifetime batches"):
        prepare_cost_rows(bundle, existing_capacity_rows=[],
                          fixed_lifetime_rows=fixed, survival_curve_rows=curves)
