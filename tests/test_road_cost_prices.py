"""NLR road purchase price selection and RPE basis checks."""

import pandas as pd
import pytest

from parameterization.road_capex_opex import select_nlr_purchase_prices
from utils import load_config_bundle, load_harmonization_rules


@pytest.mark.parametrize("trajectory", ["Mid", "Advanced", "Conservative"])
def test_selected_atb_prices_keep_msrp_and_recover_manufacturing_cost(trajectory) -> None:
    bundle = load_config_bundle("config/scenarios/legacy_reproduction.yaml")
    efficiency_rules = load_harmonization_rules(bundle, "efficiencies")
    cost_rules = load_harmonization_rules(bundle, "costs")
    vehicles = pd.read_csv(
        "inputs/1_interim/fetched_nlr_atb_autonomie/atb_vehicles.csv"
    )
    price_source = bundle.sources.sources["nlr_atb_transportation"].component(
        "vehicles"
    )
    selected = select_nlr_purchase_prices(
        vehicles,
        trajectory=trajectory,
        efficiency_rules=efficiency_rules,
        rpe_markups=cost_rules["nlr_road_retail_price_equivalent_markup"],
        source_currency=price_source.adapter["native_cost_currency"],
        source_dollar_year=price_source.adapter["native_cost_dollar_year"],
    )
    assert not selected.empty
    assert set(selected.source_currency) == {"USD"}
    assert set(selected.source_dollar_year) == {2022}
    assert not selected.loc[selected.family.eq("mhdv"), "powertrain"].isin(
        ["gasoline", "cng"]
    ).any()
    for family, markup in (("ldv", 1.5), ("mhdv", 1.2)):
        rows = selected.loc[selected.family.eq(family)]
        assert not rows.empty and rows.rpe_markup.eq(markup).all()
        assert rows.manufacturing_cost_usd_2022_per_vehicle.equals(
            rows.vehicle_price_usd_2022_per_vehicle / markup
        )
    source_prices = vehicles.loc[
        vehicles.trajectory.eq(trajectory) & vehicles.metric_key.eq("modeled_vehicle_price")
    ]
    joined = selected.merge(source_prices, left_on=["source_year", "vehicle_class", "vehicle_detail"],
                            right_on=["year", "vehicle_class", "vehicle_detail"], validate="one_to_one")
    assert len(joined) == len(selected)
    assert joined.vehicle_price_usd_2022_per_vehicle.equals(joined.value)
    assert selected["source_id"].eq("nlr_atb_transportation").all()


def test_rejects_unreviewed_markup_and_duplicate_price_details() -> None:
    sample = pd.DataFrame(
        [{
            "year": 2025, "vehicle_weight_category": "Light Duty",
            "vehicle_class": "Compact", "vehicle_powertrain": "Gasoline",
            "vehicle_detail": "Gasoline ICE Vehicle (spark ignition)",
            "metric_key": "modeled_vehicle_price", "value": 30000.0,
            "unit": "2022 USD/vehicle", "trajectory": "Mid",
            "source_id": "nlr_atb_transportation",
        }]
    )
    rules = {
        "ldv_archetypes": {"gasoline": {"detail": sample.vehicle_detail.iloc[0]}},
        "mhdv_archetypes": {},
        "atb": {"weight_categories": {"ldv": "Light Duty", "mhdv": "Medium/Heavy Duty"}},
    }
    with pytest.raises(ValueError, match="markup"):
        select_nlr_purchase_prices(
            sample, trajectory="Mid", efficiency_rules=rules, rpe_markups={"ldv": 1, "mhdv": 1.2},
            source_currency="USD", source_dollar_year=2022,
        )
    with pytest.raises(ValueError, match="ambiguous"):
        select_nlr_purchase_prices(
            pd.concat([sample, sample]), trajectory="Mid", efficiency_rules=rules,
            rpe_markups={"ldv": 1.5, "mhdv": 1.2}, source_currency="USD", source_dollar_year=2022,
        )


@pytest.mark.parametrize("markups", [
    1.5, {"ldv": 1.5}, {"ldv": 1.5, "mhdv": 1.2, "other": 1.3},
    {"ldv": 1.5, "mhdv": 0}, {"ldv": 1.5, "mhdv": float("nan")},
    {"ldv": float("inf"), "mhdv": 1.2}, {"ldv": True, "mhdv": 1.2},
    {"ldv": 1.5, "mhdv": "1.2"},
])
def test_rejects_missing_or_invalid_family_markup_before_selecting_evidence(markups):
    with pytest.raises(ValueError, match="RPE markups"):
        select_nlr_purchase_prices(
            pd.DataFrame(), trajectory="Mid", efficiency_rules={}, rpe_markups=markups,
            source_currency="USD", source_dollar_year=2022,
        )
