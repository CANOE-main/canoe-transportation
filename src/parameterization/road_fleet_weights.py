"""Shared regional fleet evidence and class weights for road parameter adapters."""

from __future__ import annotations

import re
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from typing import Any

import pandas as pd

from utils import ConfigBundle, load_harmonization_rules, resolve_artifact_path, resolve_input_path
from validation.config_models import AggregationRole


MTO = "ontario_ministry_transport_vehicle_population"
WARDS = "wards_intelligence_2022_sales_shares"
STATCAN = "statcan_transport_tables"


def aggregation_component(
    bundle: ConfigBundle, role: AggregationRole, region: str,
) -> tuple[str, str | int]:
    """Resolve actual regional input lineage, independently of parameter family."""
    source = bundle.scenario.aggregation_sources.source_for(role, region)
    components: dict[tuple[str, str], str | int] = {
        ("ldv", MTO): "A",
        ("medium_trucks", MTO): 4,
        ("medium_trucks", WARDS): "vehicle_class_market_shares",
        ("heavy_truck_haul", STATCAN): "23-10-0142-01",
    }
    try:
        return source, components[role, source]
    except KeyError as error:
        raise ValueError(f"No {role} aggregation adapter for {region}: {source}") from error


def report4_gvwr_shares(report4: pd.DataFrame, *, rules: dict[str, Any]) -> dict[int, float]:
    """Map native Report 4 Class 2b to canonical Class 2, retaining its stock weight."""
    mapping = rules["report4_gvwr"]
    selected = report4.loc[report4.EPA_GVWR.isin(mapping)].copy()
    if selected.EPA_GVWR.duplicated().any() or set(selected.EPA_GVWR) != set(mapping):
        raise ValueError("Incomplete MTO medium GVWR counts")
    counts = {
        int(mapping[item.EPA_GVWR]): float(item.NATIVE_COUNT)
        for item in selected.itertuples(index=False)
    }
    if len(counts) != len(mapping) or any(not isfinite(v) or v <= 0 for v in counts.values()):
        raise ValueError("Invalid MTO medium GVWR counts")
    return {gvwr: count / sum(counts.values()) for gvwr, count in sorted(counts.items())}


def wards_gvwr_shares(wards: pd.DataFrame, *, rules: dict[str, Any]) -> dict[int, float]:
    """Use the configured national Wards Classes 3-7 shares, without Class 2/2b."""
    selected = wards.loc[
        wards.vehicle_scope.eq("mhdv")
        & wards.nrcan_ceud_class.eq("Medium Trucks")
        & wards.year.eq(int(rules["wards_year"]))
    ].copy()
    pattern = re.compile(rules["wards_class_pattern"])
    selected["gvwr"] = selected.wards_size_class.map(
        lambda label: int(match[1]) if (match := pattern.fullmatch(str(label))) else None
    )
    if (selected.empty or selected.gvwr.isna().any() or selected.gvwr.duplicated().any()
            or set(selected.gvwr) != set(rules["wards_classes"])):
        raise ValueError("National medium-truck GVWR shares are incomplete or invalid")
    shares = selected.set_index("gvwr").market_share.astype(float).to_dict()
    if (any(not isfinite(v) or v <= 0 for v in shares.values())
            or abs(sum(shares.values()) - 1) > 1e-8):
        raise ValueError("National medium-truck GVWR shares are incomplete or invalid")
    return {int(gvwr): float(share) for gvwr, share in sorted(shares.items())}


def medium_vocation_weights(
    shares: dict[int, float], classes: list[str], *, rules: dict[str, Any],
) -> dict[str, float]:
    """Allocate each GVWR share equally to all available freight vocations.

    Identical numerical schedules remain separate vocations and retain equal weight.
    This is the common treatment for consumption, prices and annual VMT schedules.
    """
    if rules["within_gvwr_aggregation"] != "equal_available_freight_vocations":
        raise ValueError("Unsupported medium-truck within-GVWR treatment")
    if (not shares or any(not isfinite(v) or v <= 0 for v in shares.values())
            or abs(sum(shares.values()) - 1) > 1e-8):
        raise ValueError("Invalid medium-truck GVWR shares")
    pattern = re.compile(rules["atb_class_pattern"])
    result: dict[str, float] = {}
    for gvwr, share in sorted(shares.items()):
        vocations = sorted({
            label for label in classes
            if (match := pattern.match(label)) and int(match[1]) == gvwr
            and not any(excluded in label for excluded in rules["excluded_vocations"])
        })
        if not vocations:
            raise ValueError(f"No ATB freight vocation coverage for Class {gvwr}")
        result.update({label: share / len(vocations) for label in vocations})
    return result


@dataclass(frozen=True)
class FleetAggregationEvidence:
    """Validated source-native evidence reused by the parameter-specific adapters."""

    bundle: ConfigBundle
    rules: dict[str, Any]
    ldv: pd.DataFrame
    medium_shares: dict[str, dict[int, float]]
    freight: pd.DataFrame
    paths: tuple[Path, ...]

    def medium_weights(self, region: str, classes: list[str]) -> dict[str, float]:
        source, _ = aggregation_component(self.bundle, "medium_trucks", region)
        return medium_vocation_weights(
            self.medium_shares[source], classes, rules=self.rules["medium_trucks"],
        )

    def heavy_weights(self, region: str) -> dict[str, float]:
        aggregation_component(self.bundle, "heavy_truck_haul", region)
        rules = self.rules["heavy_trucks"]
        source_region = rules["freight_source_region_map"].get(region, region)
        selected = self.freight.loc[self.freight.scenario_region.eq(source_region)]
        values = pd.to_numeric(selected.tonne_kilometres, errors="raise")
        vocations = rules["vocations"]
        totals = selected.groupby("haul_class").tonne_kilometres.sum()
        if (selected.empty or values.map(lambda v: not isfinite(v) or v < 0).any()
                or set(totals.index) != set(vocations) or totals.sum() <= 0):
            raise ValueError(f"Incomplete freight haul evidence for {source_region}")
        return {
            label: float(totals[haul] / totals.sum()) / len(labels)
            for haul, labels in vocations.items() for label in labels
        }


def load_ldv_aggregation_weights(
    bundle: ConfigBundle, *, rules: dict[str, Any],
) -> tuple[pd.DataFrame, Path]:
    """Validate the single selected Report A edition and class-weight basis."""
    for region in bundle.scenario.geography.regions:
        aggregation_component(bundle, "ldv", region)
    path = resolve_artifact_path(bundle, "road_aggregation", rules["nlr_weights_file"])
    ldv = pd.read_csv(path)
    year = bundle.scenario.existing_capacity.vehicle_population_year
    if set(ldv.report_year) != {year}:
        raise ValueError("LDV aggregation evidence differs from the selected population year")
    ldv = ldv.loc[ldv.weight_basis.eq(rules["ldv_weight_basis"])].copy()
    if (ldv.empty or set(ldv.nrcan_ceud_class) != {"Car", "Light Truck"}
            or ldv.duplicated(["nrcan_ceud_class", "nlr_atb_class"]).any()
            or ldv.aggregation_weight.map(lambda v: not isfinite(v) or v <= 0).any()
            or ldv.groupby("nrcan_ceud_class").aggregation_weight.sum().sub(1).abs().gt(1e-8).any()):
        raise ValueError("Incomplete or invalid Report A LDV weights")
    return ldv, path


def load_fleet_aggregation_evidence(bundle: ConfigBundle) -> FleetAggregationEvidence:
    """Read only the sources selected by the shared regional policy; never download."""
    rules = load_harmonization_rules(bundle, "road_aggregation")
    paths: list[Path] = []

    def read(path: Path) -> pd.DataFrame:
        paths.append(path)
        frame = pd.read_csv(path)
        if frame.empty:
            raise ValueError(f"Empty fleet aggregation evidence: {path}")
        return frame

    ldv, ldv_path = load_ldv_aggregation_weights(bundle, rules=rules)
    paths.append(ldv_path)
    year = bundle.scenario.existing_capacity.vehicle_population_year
    medium_shares = {}
    selected_sources = {
        aggregation_component(bundle, "medium_trucks", region)[0]
        for region in bundle.scenario.geography.regions
    }
    for source in sorted(selected_sources):
        if source == MTO:
            ontario = load_harmonization_rules(bundle, "ontario_vehicle_population")
            report = read(resolve_input_path(
                bundle, "interim", ontario["interim_subdir"],
                ontario["reports"][4]["distribution_output_template"].format(year=year),
            ))
            if set(report.year) != {year} or set(report.source_id) != {MTO}:
                raise ValueError("Report 4 year/source differs from the selected MTO evidence")
            medium_shares[source] = report4_gvwr_shares(report, rules=rules["medium_trucks"])
        elif source == WARDS:
            from parameterization.manual_parameters import validate_manual_registry

            manual_rules = load_harmonization_rules(bundle, "manual_parameters")
            wards_path = resolve_input_path(
                bundle, "manual", bundle.sources.sources[WARDS].adapter["manual_parameter_path"],
            )
            validate_manual_registry(
                bundle, source_column=manual_rules["source_column"],
                notes_column=manual_rules["notes_column"], selected_files={wards_path.name},
            )
            medium_shares[source] = wards_gvwr_shares(
                read(wards_path),
                rules=rules["medium_trucks"],
            )
    statcan = load_harmonization_rules(bundle, "statcan_tables")
    freight = read(resolve_input_path(
        bundle, "interim", statcan["interim_subdir"], statcan["freight"]["output_file"],
    ))
    if set(freight.table_id) != {"23-10-0142-01"}:
        raise ValueError("Freight evidence differs from the registered haul table")
    return FleetAggregationEvidence(bundle, rules, ldv, medium_shares, freight, tuple(paths))
