"""NLR road purchase price selection and RPE basis checks."""

import pandas as pd
import pytest

from parameterization.road_capex_opex import select_nlr_purchase_prices
from utils import load_config_bundle, load_harmonization_rules


def test_selected_atb_prices_keep_msrp_and_recover_manufacturing_cost() -> None:
    bundle = load_config_bundle("config/scenarios/legacy_reproduction.yaml")
    efficiency_rules = load_harmonization_rules(bundle, "efficiencies")
    cost_rules = load_harmonization_rules(bundle, "costs")
    vehicles = pd.read_csv(
        "inputs/1_interim/fetched_nlr_atb_autonomie/atb_vehicles.csv"
    )
    price_source = bundle.sources.sources["nlr_atb_transportation_2024"].component(
        "vehicles"
    )
    selected = select_nlr_purchase_prices(
        vehicles,
        trajectory=bundle.scenario.sources.selections[
            "nlr_atb_transportation_2024"
        ].trajectory,
        efficiency_rules=efficiency_rules,
        rpe_markup=cost_rules["nlr_road_retail_price_equivalent_markup"],
        source_currency=price_source.adapter["native_cost_currency"],
        source_dollar_year=price_source.adapter["native_cost_dollar_year"],
    )
    assert not selected.empty
    assert set(selected.source_currency) == {"USD"}
    assert set(selected.source_dollar_year) == {2022}
    assert not selected.loc[selected.family.eq("mhdv"), "powertrain"].isin(
        ["gasoline", "cng"]
    ).any()
    assert selected.manufacturing_cost_usd_2022_per_vehicle.equals(
        selected.vehicle_price_usd_2022_per_vehicle / 1.5
    )
    assert selected["source_id"].eq("nlr_atb_transportation_2024").all()


def test_rejects_unreviewed_markup_and_duplicate_price_details() -> None:
    sample = pd.DataFrame(
        [{
            "year": 2025, "vehicle_weight_category": "Light Duty",
            "vehicle_class": "Compact", "vehicle_powertrain": "Gasoline",
            "vehicle_detail": "Gasoline ICE Vehicle (spark ignition)",
            "metric_key": "modeled_vehicle_price", "value": 30000.0,
            "unit": "2022 USD/vehicle", "trajectory": "Mid",
            "source_id": "nlr_atb_transportation_2024",
        }]
    )
    rules = {
        "ldv_archetypes": {"gasoline": {"detail": sample.vehicle_detail.iloc[0]}},
        "mhdv_archetypes": {},
        "atb": {"weight_categories": {"ldv": "Light Duty", "mhdv": "Medium/Heavy Duty"}},
    }
    with pytest.raises(ValueError, match="markup"):
        select_nlr_purchase_prices(
            sample, trajectory="Mid", efficiency_rules=rules, rpe_markup=1,
            source_currency="USD", source_dollar_year=2022,
        )
    with pytest.raises(ValueError, match="ambiguous"):
        select_nlr_purchase_prices(
            pd.concat([sample, sample]), trajectory="Mid", efficiency_rules=rules,
            rpe_markup=1.5, source_currency="USD", source_dollar_year=2022,
        )
