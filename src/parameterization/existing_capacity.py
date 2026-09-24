"""Compile road and off-road existing capacity into validated CANOE v4 rows."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from typing import Any

import pandas as pd
from canoe_schema.v4_0 import ExistingCapacity

from parameterization.efficiencies import prepare_bus_annual_efficiency_evidence
from parameterization.offroad_lifetimes import prepare_statcan_bus_lifetimes
from parameterization.offroad_stocks_and_demands import build_offroad_existing_capacity
from parameterization.road_stocks_and_demands import (
    accepted_curve_ages,
    build_existing_stock_age_artifacts,
    distribute_existing_bus_capacity,
    distribute_existing_road_capacity,
    fixed_existing_lifetimes,
)
from utils import (
    ConfigBundle,
    active_source_keys,
    load_config_bundle,
    load_harmonization_rules,
    file_sha256,
    resolve_artifact_path,
    resolve_input_path,
    resolve_parameter_path,
    write_dataframe_atomic,
)
from validation.insertion import (
    cleanup_transport_parameter_batches,
    validate_parameter_rows,
)
from validation.provenance import (
    ResolvedProvenance,
    resolve_composite_provenance,
    resolve_provenance,
    source_id_mapping,
)


LOGGER = logging.getLogger(__name__)


def _rules_digest(rules: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(rules, sort_keys=True).encode()).hexdigest()[:16]


def _component(
    bundle: ConfigBundle, source: str, component: str | int
) -> ResolvedProvenance:
    return resolve_provenance(
        bundle.sources,
        source_key=source,
        component_key=component,
        transformation="existing_capacity_input",
        transformation_version="1",
    )


def _context(
    bundle: ConfigBundle,
    *,
    name: str,
    components: list[tuple[str, str | int]],
    variant: dict[str, Any],
) -> ResolvedProvenance:
    inputs = [_component(bundle, source, component) for source, component in components]
    governing_id = source_id_mapping(bundle.sources)["nrcan_ceud_transport_provincial"]
    governing_quality = next(
        item.data_quality for item in inputs if item.source_id == governing_id
    )
    return resolve_composite_provenance(
        inputs=inputs,
        dataset_key=f"existing_capacity.{name}",
        transformation="CEUD stock or energy with reviewed age, fuel, and lifetime evidence",
        transformation_version="1",
        governing_source_id=governing_id,
        data_quality=governing_quality,
        value_variant=variant,
    )


def validate_capacity_inputs(
    bundle: ConfigBundle, provincial: pd.DataFrame, national: pd.DataFrame
) -> None:
    """Reject incomplete CEUD editions and scenario region mismatches."""
    base_year = bundle.scenario.periods.base_year
    regions = set(bundle.scenario.geography.regions)
    if set(provincial["region"]) != regions:
        raise ValueError(
            f"CEUD regional coverage differs from scenario: {set(provincial['region']) ^ regions}"
        )
    if (
        int(provincial["year"].max()) != base_year
        or int(national["year"].max()) != base_year
    ):
        raise ValueError("CEUD interim tables do not end in the scenario base year")
    if provincial.duplicated(["region", "table_id", "raw_series", "year"]).any():
        raise ValueError("Duplicate provincial CEUD series/year rows")
    if national.duplicated(["table_id", "raw_series", "year"]).any():
        raise ValueError("Duplicate national CEUD series/year rows")


def validate_capacity_outputs(
    *,
    bundle: ConfigBundle,
    road_cohorts: pd.DataFrame,
    road_capacity: pd.DataFrame,
    bus_cohorts: pd.DataFrame,
    bus_capacity: pd.DataFrame,
    offroad_annual: pd.DataFrame,
    offroad_capacity: pd.DataFrame,
    rows: list[ExistingCapacity],
    cleanup_epsilon: float,
) -> dict[str, Any]:
    """Check conservation, ownership, units, and key coverage before publication."""
    regions = set(bundle.scenario.geography.regions)
    output_regions = {
        "NLLAB" if x == "NL" else "PEI" if x == "PE" else x for x in regions
    }
    template = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    technologies = set(template["tech"])
    if not rows or {row.region for row in rows} != output_regions:
        raise ValueError("Existing capacity lacks one or more native output regions")
    if any(row.tech not in technologies for row in rows):
        raise ValueError("Existing capacity references a missing template technology")
    if any(row.vintage not in bundle.scenario.periods.existing for row in rows):
        raise ValueError("Existing capacity references a non-existing vintage period")
    if any(row.capacity is None or row.capacity < 0 for row in rows):
        raise ValueError("Existing capacity has missing or negative capacity")
    if any(
        row.units not in {"k vehicles", "bn passenger-km", "bn tonne-km"}
        for row in rows
    ):
        raise ValueError("Existing capacity has an unsupported unit")
    road_stock = road_cohorts.groupby(["ceud_region", "road_class"])[
        "cohort_k_vehicles"
    ].sum()
    road_source = road_cohorts.groupby(["ceud_region", "road_class"])[
        "stock_k_vehicles"
    ].first()
    if not (road_stock - road_source).abs().le(1e-7).all():
        raise ValueError("Road age cohorts do not conserve CEUD base-year stock")
    if (
        abs(
            float(road_capacity["capacity"].sum())
            - float(road_cohorts["cohort_k_vehicles"].sum())
        )
        > 1e-6
    ):
        raise ValueError("Road fuel distribution does not conserve disaggregated stock")
    bus_stock = bus_cohorts.groupby(["ceud_region", "road_class"])[
        "capacity"
    ].sum()
    bus_source = bus_cohorts.groupby(["ceud_region", "road_class"])[
        "stock_k_vehicles"
    ].first()
    if not (bus_stock - bus_source).abs().le(1e-7).all():
        raise ValueError("Bus cohorts do not conserve CEUD base-year stock")
    if (
        abs(float(bus_capacity["capacity"].sum()) - float(bus_stock.sum())) > 1e-7
    ):
        raise ValueError("Bus vintage aggregation does not conserve cohort capacity")
    if len(offroad_annual) != len(output_regions) * 6 * (
        bundle.scenario.periods.base_year - min(bundle.scenario.periods.existing) + 1
    ):
        raise ValueError(
            "Off-road annual capacity lacks region/mode/technology/year coverage"
        )
    if set(offroad_capacity["region"]) != output_regions:
        raise ValueError("Off-road capacity lacks native regions")
    if len({(row.region, row.tech, row.vintage) for row in rows}) != len(rows):
        raise ValueError(
            "Existing capacity has duplicate native region/tech/vintage keys"
        )
    expected_keys = {
        (str(record.region), str(record.tech), int(record.vintage))
        for record in pd.concat([road_capacity, bus_capacity, offroad_capacity]).itertuples(
            index=False
        )
        if float(record.capacity) > 0 and float(record.capacity) >= cleanup_epsilon
    }
    actual_keys = {(row.region, row.tech, row.vintage) for row in rows}
    if actual_keys != expected_keys:
        raise ValueError(
            f"Existing capacity technology/vintage coverage differs: missing={len(expected_keys - actual_keys)}, extra={len(actual_keys - expected_keys)}"
        )
    return {
        "regions": sorted(output_regions),
        "road_classes": sorted(road_cohorts["road_class"].unique()),
        "bus_classes": sorted(bus_cohorts["road_class"].unique()),
        "bus_stock_k_vehicles": float(bus_capacity["capacity"].sum()),
        "offroad_modes": sorted(offroad_annual["mode"].unique()),
        "road_stock_k_vehicles": float(road_capacity["capacity"].sum()),
        "offroad_capacity_by_unit": offroad_capacity.groupby("units")["capacity"]
        .sum()
        .to_dict(),
        "parameter_rows": len(rows),
        "pending_classes": [],
        "excluded_family": "EV charger capacity",
    }


def build_existing_capacity_artifacts(
    bundle: ConfigBundle,
) -> tuple[list[ExistingCapacity], list[ResolvedProvenance], dict[str, Any]]:
    """Rebuild normalized audit products and parameter-ready v4 rows from cached inputs."""
    scenario = bundle.scenario
    if scenario.existing_capacity is None:
        raise ValueError("Scenario existing_capacity source selection is required")
    selected_source = scenario.existing_capacity.other_region_vehicle_population_source
    available_sources = active_source_keys(bundle)
    if (
        selected_source not in available_sources
        or selected_source != "ontario_ministry_transport_vehicle_population"
    ):
        raise ValueError(
            "Selected non-Ontario vehicle population source is unavailable"
        )
    lifetime_sources = {
        "nhtsa_cafe_2024_ldv_survival",
        "eia_nems_hd_truck_scrappage",
        "epa_moves4_population_activity_2023",
        "canada_energy_policy_simulator_3_4_7",
        "emrg_sfu_cims_model",
    }
    inactive_lifetimes = sorted(lifetime_sources - available_sources)
    if inactive_lifetimes:
        raise ValueError(
            f"Existing-capacity lifetime sources are inactive: {inactive_lifetimes}"
        )
    base_year = scenario.periods.base_year
    regions = list(scenario.geography.regions)
    road_family = load_harmonization_rules(bundle, "road_stocks_and_demands")
    road_rules = road_family["existing_capacity"]
    bus_rules = road_family["bus_existing_capacity"]
    offroad_rules = load_harmonization_rules(bundle, "offroad_stocks_and_demands")[
        "existing_capacity"
    ]
    ceud_rules = load_harmonization_rules(bundle, "nrcan_ceud")
    statcan_rules = load_harmonization_rules(bundle, "statcan_tables")
    dashboard_rules = load_harmonization_rules(bundle, "assorted_sources")[
        "tc_ev_dashboard"
    ]
    mto_rules = load_harmonization_rules(bundle, "ontario_vehicle_population")
    ceud_dir = resolve_input_path(bundle, "interim", ceud_rules["interim_subdir"])
    ceud_files = [
        ceud_dir / ceud_rules["region_output_template"].format(region=region.lower())
        for region in regions
    ]
    national_file = ceud_dir / ceud_rules["region_output_template"].format(
        region="national"
    )
    provincial = pd.concat(
        [pd.read_csv(path) for path in ceud_files],
        ignore_index=True,
    )
    national = pd.read_csv(national_file)
    validate_capacity_inputs(bundle, provincial, national)
    statcan_dir = resolve_input_path(bundle, "interim", statcan_rules["interim_subdir"])
    build_existing_stock_age_artifacts(bundle)
    road_age_file = (
        resolve_artifact_path(bundle, "road_stocks_and_demands")
        / load_harmonization_rules(bundle, "road_stocks_and_demands")[
            "ontario_report_a"
        ]["age_distribution_file"]
    )
    report5_file = resolve_artifact_path(
        bundle, "ontario_vehicle_population"
    ) / mto_rules["reports"][5]["distribution_output_template"].format(
        year=road_rules["age_evidence_year"]
    )
    ldv_file = statcan_dir / statcan_rules["ldv_history"]["output_file"]
    truck_file = statcan_dir / statcan_rules["tables"]["23-10-0308-01"]["output_file"]
    dashboard_file = (
        resolve_artifact_path(bundle, "tc_ev_dashboard_interim")
        / dashboard_rules["output_file"]
    )
    lifetime_file = resolve_input_path(bundle, "manual", "lifetime_process.csv")
    lifetime_rules = load_harmonization_rules(bundle, "road_lifetimes_survival")
    lifetime_dir = resolve_artifact_path(bundle, "road_lifetimes_survival")
    median_file = lifetime_dir / lifetime_rules["median_lifetimes_file"]
    curve_file = lifetime_dir / lifetime_rules["transformed_curves_file"]
    mapping_file = resolve_parameter_path(
        bundle,
        load_harmonization_rules(bundle, "road_aggregation")[
            "vehicle_size_class_map_file"
        ],
    )
    bus_efficiency, bus_evidence_files = prepare_bus_annual_efficiency_evidence(
        bundle, provincial
    )
    bus_lifetime_rows, _, _ = prepare_statcan_bus_lifetimes(bundle)
    bus_lifetimes = {
        (row.region, row.tech): float(row.lifetime) for row in bus_lifetime_rows
    }
    bus_lifetime_rules = load_harmonization_rules(bundle, "road_lifetimes_survival")[
        "bus_lifetimes"
    ]
    bus_lifetime_file = statcan_dir / statcan_rules["tables"][str(
        bus_lifetime_rules["statcan_table_id"]
    )]["output_file"]
    input_digest = hashlib.sha256()
    for path in sorted(
        [
            *ceud_files,
            national_file,
            road_age_file,
            report5_file,
            ldv_file,
            truck_file,
            dashboard_file,
            lifetime_file,
            median_file,
            curve_file,
            mapping_file,
            bus_lifetime_file,
            *bus_evidence_files,
        ],
        key=lambda item: item.name,
    ):
        input_digest.update(path.name.encode("utf-8"))
        input_digest.update(file_sha256(path).encode("ascii"))
    manual_lifetimes = pd.read_csv(lifetime_file)
    for category, source_key, component_key in (
        (
            "heavy_trucks",
            "epa_moves4_population_activity_2023",
            "heavy_duty_truck_lifetimes",
        ),
        ("motorcycles", "canada_energy_policy_simulator_3_4_7", "motorcycle_lifetimes"),
    ):
        expected_source = (
            bundle.sources.sources[source_key]
            .component(component_key)
            .adapter["source_selector"]
        )
        selected = manual_lifetimes.loc[manual_lifetimes["category"].eq(category)]
        if (
            len(selected) != 1
            or selected.iloc[0]["source -> data_source"] != expected_source
        ):
            raise ValueError(f"Manual road lifetime source mismatch for {category}")
    medians = pd.read_csv(median_file)
    road_cohorts, road_shares, road_exclusions, road_capacity = (
        distribute_existing_road_capacity(
            provincial=provincial,
            ldv_age=pd.read_csv(road_age_file),
            report5_age=pd.read_csv(report5_file),
            ldv_registrations=pd.read_csv(ldv_file),
            truck_registrations=pd.read_csv(truck_file),
            dashboard=pd.read_csv(dashboard_file),
            regions=regions,
            base_year=base_year,
            first_model_period=min(scenario.periods.model),
            survival_curves=scenario.switches.survival_curves,
            survival_curve_max_age=scenario.switches.survival_curve_max_age,
            fixed_lifetimes_by_class=fixed_existing_lifetimes(
                medians, manual_lifetimes, road_rules
            ),
            curve_ages_by_class=accepted_curve_ages(pd.read_csv(curve_file), road_rules)
            if scenario.switches.survival_curves
            else {},
            rules=road_rules,
        )
    )
    bus_cohorts, bus_shares, bus_exclusions, bus_transfers, bus_capacity = (
        distribute_existing_bus_capacity(
            provincial=provincial,
            report5_age=pd.read_csv(report5_file),
            annual_efficiency=bus_efficiency,
            lifetimes=bus_lifetimes,
            regions=regions,
            base_year=base_year,
            first_model_period=min(scenario.periods.model),
            road_rules=road_rules,
            rules=bus_rules,
        )
    )
    cims_component = bundle.sources.sources["emrg_sfu_cims_model"].component(
        "transport_process_lifetimes"
    )
    offroad_annual, offroad_cohorts, offroad_capacity, air_exclusions = (
        build_offroad_existing_capacity(
            provincial=provincial,
            national=national,
            manual_lifetimes=manual_lifetimes,
            source_selector=cims_component.adapter["source_selector"],
            regions=regions,
            base_year=base_year,
            first_model_period=min(scenario.periods.model),
            rules=offroad_rules,
        )
    )
    interim_output = resolve_artifact_path(bundle, "existing_capacity_interim")
    processed_output = resolve_artifact_path(bundle, "existing_capacity_processed")
    validation_output = resolve_artifact_path(bundle, "existing_capacity_validation")
    variants = {
        "base_year": base_year,
        "periods": scenario.periods.existing,
        "first_model_period": min(scenario.periods.model),
        "survival_curves": scenario.switches.survival_curves,
        "cleanup_epsilon": scenario.existing_capacity.cleanup_epsilon,
        "road_rules_sha256": _rules_digest(road_rules),
        "bus_rules_sha256": _rules_digest(bus_rules),
        "offroad_rules_sha256": _rules_digest(offroad_rules),
        "vehicle_population_source": selected_source,
        "input_content_digest": input_digest.hexdigest(),
    }
    rows: list[ExistingCapacity] = []
    contexts: list[ResolvedProvenance] = []
    for (road_class, vintage), group in road_capacity.groupby(
        ["road_class", "vintage"], sort=True
    ):
        ceud_component = {"cars": 21, "motorcycles": 32}.get(road_class, 37)
        mto_component = (
            "A" if road_class in road_rules["statcan_ldv_source_types"] else 5
        )
        components: list[tuple[str, str | int]] = [
            ("nrcan_ceud_transport_provincial", ceud_component),
            (selected_source, mto_component),
        ]
        if road_class in road_rules["statcan_ldv_source_types"]:
            mapping_sources = (
                "nrcan_fuel_consumption_ratings",
                "fueleconomy_gov_vehicle_data",
                "reviewed_mto_make_model_evidence",
            )
            inactive = sorted(set(mapping_sources) - available_sources)
            if inactive:
                raise ValueError(
                    f"LDV mapping evidence sources are inactive: {inactive}"
                )
            components.extend(
                (source_key, component_key)
                for source_key in mapping_sources
                for component_key in bundle.sources.sources[source_key].components
            )
            components.append(("nhtsa_cafe_2024_ldv_survival", "ldv_survival_rates"))
        elif road_class == "medium_trucks":
            components.append(("eia_nems_hd_truck_scrappage", "truck_scrappage_rates"))
        elif road_class == "heavy_trucks":
            components.append(
                ("epa_moves4_population_activity_2023", "heavy_duty_truck_lifetimes")
            )
        elif road_class == "motorcycles":
            components.append(
                ("canada_energy_policy_simulator_3_4_7", "motorcycle_lifetimes")
            )
        shares = road_shares.loc[
            road_shares["road_class"].eq(road_class)
            & road_shares["vintage"].eq(vintage)
        ]
        table_ids = sorted(
            {
                table_id
                for packed in shares["source_table_ids"].dropna()
                for table_id in str(packed).split("|")
                if table_id
            }
        )
        components.extend(
            ("statcan_transport_tables", table_id) for table_id in table_ids
        )
        if road_class == "medium_trucks" and vintage == base_year:
            components.append(
                ("transport_canada_ev_dashboard", "medium_heavy_ev_market_share")
            )
        context = _context(
            bundle,
            name=f"road.{road_class}.{vintage}",
            components=components,
            variant=variants,
        )
        contexts.append(context)
        rows.extend(
            validate_parameter_rows(
                ExistingCapacity,
                [
                    {
                        "region": record.region,
                        "tech": record.tech,
                        "vintage": int(record.vintage),
                        "capacity": float(record.capacity),
                        "units": record.units,
                        "notes": f"CEUD {base_year} stock; MTO {road_rules['age_evidence_year']} age; {road_class}",
                    }
                    for record in group.itertuples(index=False)
                ],
                context,
            )
        )
    for road_class, group in bus_capacity.groupby("road_class", sort=True):
        bus_spec = bus_rules["classes"][road_class]
        components: list[tuple[str, str | int]] = [
            ("nrcan_ceud_transport_provincial", bus_spec["stock"]["table_id"]),
            ("nrcan_ceud_transport_provincial", bus_spec["energy_table"]),
            (selected_source, 5),
            ("statcan_transport_tables", str(bus_lifetime_rules["statcan_table_id"])),
            ("nlr_atb_transportation_2024", "vehicles"),
            ("nlr_atb_transportation_2024", "phev_vehicle_inputs"),
        ]
        if road_class == "inter_city_buses":
            components.append(
                ("epri_us_regen_2025_transportation", "intercity_bus_charts")
            )
        context = _context(
            bundle, name=f"bus.{road_class}", components=components, variant=variants
        )
        contexts.append(context)
        rows.extend(
            validate_parameter_rows(
                ExistingCapacity,
                [
                    {
                        "region": record.region,
                        "tech": record.tech,
                        "vintage": int(record.vintage),
                        "capacity": float(record.capacity),
                        "units": record.units,
                        "notes": (
                            f"CEUD {base_year} bus stock; annual fuel activity shares; "
                            f"MTO {road_rules['age_evidence_year']} BUS ages; {road_class}"
                        ),
                    }
                    for record in group.itertuples(index=False)
                ],
                context,
            )
        )
    for mode, group in offroad_capacity.groupby("mode", sort=True):
        selectors = [
            value
            for key, value in offroad_rules["capacity_series"].items()
            if value.get("mode", key) == mode
        ]
        components = [
            ("nrcan_ceud_transport_provincial", selector["provincial_table"])
            for selector in selectors
        ]
        components += [
            ("nrcan_ceud_transport_national", selector["national_table"])
            for selector in selectors
        ]
        components.append(("emrg_sfu_cims_model", "transport_process_lifetimes"))
        context = _context(
            bundle, name=f"offroad.{mode}", components=components, variant=variants
        )
        contexts.append(context)
        rows.extend(
            validate_parameter_rows(
                ExistingCapacity,
                [
                    {
                        "region": record.region,
                        "tech": record.tech,
                        "vintage": int(record.vintage),
                        "capacity": float(record.capacity),
                        "units": record.units,
                        "notes": f"CEUD same-year energy/intensity; linear cohort retirement; {mode}",
                    }
                    for record in group.itertuples(index=False)
                ],
                context,
            )
        )
    cleaned, removed = cleanup_transport_parameter_batches(
        {"existing_capacity": rows},
        existing_periods=scenario.periods.existing,
        first_model_period=min(scenario.periods.model),
        epsilon=scenario.existing_capacity.cleanup_epsilon,
    )
    rows = cleaned["existing_capacity"]
    removed_capacity = pd.DataFrame(
        [item for item in removed if item["table"] == "existing_capacity"],
        columns=["units", "value"],
    )
    audit = validate_capacity_outputs(
        bundle=bundle,
        road_cohorts=road_cohorts,
        road_capacity=road_capacity,
        bus_cohorts=bus_cohorts,
        bus_capacity=bus_capacity,
        offroad_annual=offroad_annual,
        offroad_capacity=offroad_capacity,
        rows=rows,
        cleanup_epsilon=scenario.existing_capacity.cleanup_epsilon,
    )
    audit["cleanup"] = {
        "epsilon": scenario.existing_capacity.cleanup_epsilon,
        "removed_rows": removed,
        "removed_count": len(removed),
        "removed_capacity_by_unit": {
            str(units): float(value)
            for units, value in removed_capacity.groupby("units")["value"].sum().items()
        },
    }
    audit["excluded_road_fuel_rows"] = len(road_exclusions)
    audit["excluded_bus_fuel_rows"] = len(bus_exclusions)
    audit["bus_technology_transfers"] = len(bus_transfers)
    audit["bus_transferred_stock_k_vehicles"] = (
        float(bus_transfers["capacity_k_vehicles"].sum())
        if not bus_transfers.empty
        else 0.0
    )
    audit["excluded_bus_fuel_pj"] = (
        {
            str(member): float(value)
            for member, value in bus_exclusions.groupby("fuel_member")[
                "excluded_energy_pj"
            ].sum().items()
        }
        if not bus_exclusions.empty
        else {}
    )
    audit["bus_ineligible_raw_stock_k_vehicles"] = float(
        bus_cohorts.loc[
            ~bus_cohorts["eligible_first_model_period"], "raw_cohort_k_vehicles"
        ].sum()
    )
    audit["excluded_road_fuel_rows_by_reason"] = (
        {
            str(reason): int(count)
            for reason, count in road_exclusions.groupby("exclusion_reason")
            .size()
            .items()
        }
        if not road_exclusions.empty
        else {}
    )
    audit["road_ineligible_raw_stock_k_vehicles"] = float(
        road_cohorts.loc[
            ~road_cohorts["eligible_first_model_period"],
            "raw_cohort_k_vehicles",
        ].sum()
    )
    audit["offroad_ineligible_raw_capacity_by_unit"] = {
        str(units): float(value)
        for units, value in offroad_cohorts.loc[
            ~offroad_cohorts["eligible_first_model_period"]
        ]
        .groupby("units")["raw_capacity_at_base_year"]
        .sum()
        .items()
    }
    audit["excluded_air_gasoline_rows"] = len(air_exclusions)
    audit["input_content_digest"] = input_digest.hexdigest()
    audit["max_excluded_fuel_source_share"] = (
        [
            {
                "road_class": road_class,
                "fuel_type": fuel_type,
                "max_share": float(share),
            }
            for (road_class, fuel_type), share in road_exclusions.groupby(
                ["road_class", "fuel_type"]
            )["excluded_source_share"]
            .max()
            .items()
        ]
        if not road_exclusions.empty
        else []
    )
    audit["excluded_air_gasoline_pj"] = {
        str(mode): float(value)
        for mode, value in air_exclusions.groupby("mode")["excluded_energy_pj"]
        .sum()
        .items()
    }
    observed_2023 = (
        offroad_annual.loc[offroad_annual["year"].eq(base_year)]
        .groupby("mode")["capacity"]
        .sum()
    )
    surviving_2023 = offroad_cohorts.groupby("mode")["capacity_at_base_year"].sum()
    audit["offroad_surviving_minus_observed_base_year"] = {
        str(mode): float(surviving_2023[mode] - observed_2023[mode])
        for mode in sorted(observed_2023.index)
    }
    audit["offroad_early_retired_capacity_by_mode"] = {
        str(mode): float(value)
        for mode, value in offroad_cohorts.groupby("mode")["early_retired_capacity"]
        .sum()
        .items()
    }
    retained = {(row.region, row.tech, row.vintage) for row in rows}
    road_parameter_ready = road_capacity.loc[
        road_capacity.apply(
            lambda record: (record.region, record.tech, record.vintage) in retained,
            axis=1,
        )
    ]
    bus_parameter_ready = bus_capacity.loc[
        bus_capacity.apply(
            lambda record: (record.region, record.tech, record.vintage) in retained,
            axis=1,
        )
    ]
    offroad_parameter_ready = offroad_capacity.loc[
        offroad_capacity.apply(
            lambda record: (record.region, record.tech, record.vintage) in retained,
            axis=1,
        )
    ]
    for frame, path in (
        (road_cohorts, interim_output / road_rules["age_cohorts_file"]),
        (road_shares, interim_output / road_rules["fuel_shares_file"]),
        (road_exclusions, validation_output / road_rules["exclusions_file"]),
        (bus_cohorts, interim_output / bus_rules["age_cohorts_file"]),
        (bus_shares, interim_output / bus_rules["activity_shares_file"]),
        (bus_exclusions, validation_output / bus_rules["exclusions_file"]),
        (bus_transfers, validation_output / bus_rules["transfers_file"]),
        (offroad_annual, interim_output / offroad_rules["annual_capacity_file"]),
        (offroad_cohorts, interim_output / offroad_rules["cohort_additions_file"]),
        (air_exclusions, validation_output / "excluded_air_gasoline_energy.csv"),
        (road_parameter_ready, processed_output / road_rules["capacity_file"]),
        (bus_parameter_ready, processed_output / bus_rules["capacity_file"]),
        (offroad_parameter_ready, processed_output / offroad_rules["capacity_file"]),
    ):
        write_dataframe_atomic(frame, path)
    write_dataframe_atomic(
        pd.DataFrame([row.model_dump(mode="python") for row in rows]),
        processed_output / "existing_capacity.csv",
    )
    validation_output.mkdir(parents=True, exist_ok=True)
    (validation_output / "integrity.json").write_text(
        json.dumps(audit, indent=2) + "\n", encoding="utf-8"
    )
    LOGGER.info(
        "Prepared %s existing-capacity rows across %s regions; removed %s below epsilon=%s; excluded %s road fuel shares",
        len(rows),
        len(audit["regions"]),
        len(removed),
        scenario.existing_capacity.cleanup_epsilon,
        len(road_exclusions),
    )
    return rows, contexts, audit


def prepare_existing_capacity_rows(
    bundle: ConfigBundle,
) -> tuple[list[ExistingCapacity], list[ResolvedProvenance], dict[str, Any]]:
    """Reusable caller-owned preparation seam for standalone and CANOE-main."""
    return build_existing_capacity_artifacts(bundle)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/legacy_reproduction.yaml"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    build_existing_capacity_artifacts(load_config_bundle(args.scenario))


if __name__ == "__main__":
    main()
