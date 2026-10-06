"""OMEGA sales-weighted LDV EV range evidence and representation preparation."""

import argparse
from collections import defaultdict
from dataclasses import dataclass
import hashlib
import json
import logging
from math import isfinite
from typing import Literal

from canoe_schema import CanoeBaseModel
from canoe_schema.v4_0 import (
    Commodity,
    CommodityLabel,
    DataSet,
    Efficiency,
    LimitTechInputSplit,
    LimitNewCapacityShare,
    TechGroup,
    TechGroupMember,
    TechnologyLabel,
)
import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from fetching.epa_omega_baseline import (
    SOURCE as EPA_SOURCE,
    COMPONENT as EPA_COMPONENT,
    build_request as epa_request,
    fetch_baseline,
)
from utils import (
    ConfigBundle,
    load_config_bundle,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_input_path,
    write_dataframe_atomic,
    write_text_atomic,
)
from validation.insertion import validate_parameter_rows
from validation.provenance import (
    ResolvedProvenance,
    make_data_id,
    resolve_composite_provenance,
    resolve_provenance,
)
from validation.schema_contract import TransportationTechnology

LOGGER = logging.getLogger(__name__)


class RangeRepresentationBlocked(ValueError):
    """An unresolved source or modelling contract prevents representation writes."""


class RangeBucket(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    sub_category: str = Field(min_length=1)
    upper: float | None = Field(ge=0, allow_inf_nan=False)
    upper_inclusive: bool


class RepresentativeFamily(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    BEV: str = Field(min_length=1)
    PHEV: str = Field(min_length=1)
    phev_blend: str = Field(min_length=1)
    phev_commodity: str = Field(min_length=1)


class RepresentativeRules(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    families: dict[str, RepresentativeFamily]
    efficiency: Literal["sales_weighted_consumption"]
    costs: Literal["sales_weighted_same_capacity_and_service_units"]
    capacity_to_activity: Literal["require_identical"]
    utilization: Literal["require_identical"]
    fixed_lifetime: Literal["require_identical"]
    survival: Literal["sales_weighted_surviving_fraction"]
    survival_composition: Literal["require_identical_range_curves"]
    embodied: Literal["sales_weighted_lifetime_mass"]
    phev_split: Literal["mean_vintage_consumption_weighted_energy_share"]
    equality_tolerance: float = Field(gt=0, allow_inf_nan=False)
    fuel_commodity: str = Field(min_length=1)
    electricity_commodity: str = Field(min_length=1)
    parameter_file: str = Field(min_length=1)
    audit_file: str = Field(min_length=1)


class RangeRules(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    range_units: Literal["miles"]
    share_units: Literal["fraction"]
    buckets: dict[str, list[RangeBucket]]
    category_to_market: dict[str, str]
    new_suffix: str
    constraint_operator: str
    group_prefix: str
    share_sum_tolerance: float = Field(ge=0, allow_inf_nan=False)
    files: dict[str, str]
    representative: RepresentativeRules

    @model_validator(mode="after")
    def validate_buckets(self):
        if set(self.buckets) != {"BEV", "PHEV"} or self.constraint_operator != "ge":
            raise ValueError(
                "Range rules require BEV/PHEV and the legacy minimum-share operator ge"
            )
        if set(self.category_to_market) != {
            "cars",
            "passenger_light_trucks",
            "freight_light_trucks",
        } or set(self.category_to_market.values()) != {"car", "light_truck"}:
            raise ValueError(
                "Range representation supports the three LDV categories and two source market classes only"
            )
        if set(self.representative.families) != set(self.category_to_market):
            raise ValueError(
                "Representative labels must cover exactly the LDV categories"
            )
        labels = [
            value
            for f in self.representative.families.values()
            for value in f.model_dump().values()
        ]
        if len(labels) != len(set(labels)):
            raise ValueError("Representative structural labels must be unique")
        for buckets in self.buckets.values():
            finite = [b.upper for b in buckets[:-1]]
            if (
                not buckets
                or buckets[-1].upper is not None
                or any(x is None for x in finite)
            ):
                raise ValueError(
                    "Range bucket coverage must end in one unbounded bucket"
                )
            if finite != sorted(set(finite)) or len(
                {b.sub_category for b in buckets}
            ) != len(buckets):
                raise ValueError(
                    "Range bucket boundaries/categories must be unique and increasing"
                )
        return self


def module_rules(bundle: ConfigBundle) -> RangeRules:
    return RangeRules.model_validate(load_harmonization_rules(bundle, "ldv_ev_ranges"))


def range_bucket(powertrain: str, miles: float, rules: RangeRules) -> str:
    if not isfinite(miles) or miles <= 0:
        raise ValueError("Charge-depleting range must be finite and positive")
    for bucket in rules.buckets[powertrain]:
        if (
            bucket.upper is None
            or miles < bucket.upper
            or (bucket.upper_inclusive and miles == bucket.upper)
        ):
            return bucket.sub_category
    raise ValueError("Uncovered range bucket")


@dataclass(frozen=True)
class RangeEvidence:
    records: pd.DataFrame
    bucket_totals: pd.DataFrame
    provenance_contexts: list[ResolvedProvenance]
    audit: dict


@dataclass(frozen=True)
class RangePreparation:
    share_rows: list[LimitNewCapacityShare]
    group_rows: list[TechGroup]
    member_rows: list[TechGroupMember]
    structural_dataset: DataSet | None
    provenance_contexts: list[ResolvedProvenance]
    audit: dict
    regions: tuple[str, ...]
    expected_shares: frozenset[tuple] = frozenset()
    expected_members: frozenset[tuple[str, str]] = frozenset()
    evidence: RangeEvidence | None = None
    representative: "RepresentativePreparation | None" = None


@dataclass(frozen=True)
class RepresentativePreparation:
    """Pure parameter/structure batches; no acquisition or SQLite transaction ownership."""

    batches: dict[str, list[CanoeBaseModel]]
    labels: dict[str, list[CanoeBaseModel]]
    structural_dataset: DataSet
    provenance_contexts: list[ResolvedProvenance]
    audit: dict
    fingerprint: str


def validate_range_preparation(result: RangePreparation) -> None:
    """Validate logical group references missing from v4 FK declarations as well as values."""
    actual = {
        (r.region, r.sub_group, r.super_group, r.vintage, r.share)
        for r in result.share_rows
    }
    members = {(r.group_name, r.tech) for r in result.member_rows}
    if actual != set(result.expected_shares) or len(actual) != len(result.share_rows):
        raise ValueError(
            "Range share coverage/values changed after validated preparation"
        )
    if members != set(result.expected_members) or len(members) != len(
        result.member_rows
    ):
        raise ValueError("Range group membership changed after validated preparation")
    groups = {r.group_name for r in result.group_rows}
    if groups != {group for group, _ in members} or len(groups) != len(
        result.group_rows
    ):
        raise ValueError(
            "Range group structure contains missing/duplicate logical references"
        )
    contexts = {c.data_id: c.parameter_fields() for c in result.provenance_contexts}
    for row in result.share_rows:
        LimitNewCapacityShare.model_validate(row.model_dump())
        if (
            row.operator.value != "ge"
            or row.share is None
            or not isfinite(row.share)
            or not 0 <= row.share <= 1
        ):
            raise ValueError("Range shares must retain finite minimum-share semantics")
        if row.sub_group not in groups or row.super_group not in groups:
            raise ValueError("Range share has dangling group references")
        numerator = {tech for group, tech in members if group == row.sub_group}
        denominator = {tech for group, tech in members if group == row.super_group}
        if len(numerator) != 1 or not numerator <= denominator:
            raise ValueError(
                "Range numerator must be one variant inside its new-build family"
            )
        fields = contexts.get(row.data_id)
        if fields is None or any(
            getattr(row, key) != value for key, value in fields.items()
        ):
            raise ValueError("Range rows disagree with source/DQ lineage")
    for row in [*result.group_rows, *result.member_rows]:
        if (
            result.structural_dataset is None
            or row.data_id != result.structural_dataset.data_id
        ):
            raise ValueError("Range group lacks its internal structural dataset")


def bucket_range_evidence(records: pd.DataFrame, *, rules: RangeRules) -> RangeEvidence:
    """Assign legacy buckets without a source join or an additional range-share weight."""
    records = records.copy()
    records["bucket"] = [
        range_bucket(p, float(x), rules) if status == "valid" else None
        for p, x, status in zip(
            records.powertrain, records.cd_range_miles, records.range_status
        )
    ]
    records["status"] = np.where(
        records.bucket.notna(), "resolved_bucket", "invalid_cd_range"
    )
    totals = sales_bucket_totals(records, rules)
    unresolved = records.loc[records.status.ne("resolved_bucket")]
    summaries = []
    for (market, powertrain), group in totals.groupby(
        ["market_class", "powertrain"], sort=True
    ):
        summaries.append(
            {
                "market_class": market,
                "powertrain": powertrain,
                "sales": int(group.total_sales.iloc[0]),
                "unresolved_sales": int(group.unresolved_sales.iloc[0]),
                "sum_bucket_shares": float(group.share_of_all_sales.sum()),
            }
        )
    audit = {
        "source": EPA_SOURCE,
        "model_years": sorted(map(int, records.model_year.unique())),
        "records": len(records),
        "sales": int(records.sales.sum()),
        "unresolved_records": len(unresolved),
        "unresolved_sales": int(unresolved.sales.sum()),
        "groups": summaries,
        "accepted_for_parameters": not unresolved.sales.sum()
        and all(g["sales"] > 0 for g in summaries),
        "weight_basis": "OMEGA baseline sales; not final MY2024 actual production or catalogue counts",
        "market_proxy": "US car/light-truck baseline sales mapped to Canadian LDV categories",
        "period_applicability": "constant across configured model vintages",
        "range_metric": "onroad_charge_depleting_range_mi; the exact legacy notebook field",
        "renormalized_unresolved_sales": False,
    }
    return RangeEvidence(records, totals, [], audit)


def build_range_evidence(bundle: ConfigBundle) -> RangeEvidence:
    """Prepare auditable OMEGA bucket weights independently of scenario representation."""
    rules = module_rules(bundle)
    evidence = bucket_range_evidence(fetch_baseline(bundle), rules=rules)
    request = epa_request(bundle)
    context = resolve_provenance(
        bundle.sources,
        source_key=EPA_SOURCE,
        component_key=EPA_COMPONENT,
        transformation="OMEGA baseline sales-weighted legacy CD-range buckets",
        transformation_version="2",
        value_variant={
            "sha256": request.expected_sha256,
            "parent_sha256": request.parent_sha256,
            "legacy_notebook_sha256": request.notebook_sha256,
            "source_rules": load_harmonization_rules(bundle, EPA_SOURCE),
            "range_rules": rules.model_dump(),
        },
    )
    evidence.provenance_contexts.append(context)
    directory = resolve_artifact_path(bundle, "ldv_range_validation")
    for frame, key in (
        (evidence.records, "records"),
        (
            evidence.records.loc[evidence.records.status.ne("resolved_bucket")],
            "unresolved",
        ),
        (evidence.bucket_totals, "buckets"),
    ):
        write_dataframe_atomic(frame, directory / rules.files[key])
    write_text_atomic(
        json.dumps(
            {**evidence.audit, "provenance": context.model_dump(mode="json")},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        directory / rules.files["integrity"],
    )
    write_representative_decisions(bundle, evidence=evidence)
    LOGGER.info(
        "OMEGA range evidence: %s sales, unresolved=%s, accepted=%s",
        evidence.audit["sales"],
        evidence.audit["unresolved_sales"],
        evidence.audit["accepted_for_parameters"],
    )
    if not evidence.audit["accepted_for_parameters"]:
        LOGGER.warning(
            "Range modes blocked by unresolved sales; inspect %s",
            directory / rules.files["unresolved"],
        )
    return evidence


def validate_share_evidence(evidence: RangeEvidence, *, rules: RangeRules) -> None:
    """Recompute full-denominator shares; an audit flag cannot authorize incomplete evidence."""
    if not evidence.provenance_contexts:
        raise RangeRepresentationBlocked(
            "Range evidence lacks registered source/DQ lineage"
        )
    records = evidence.records
    expected = sales_bucket_totals(records, rules)
    actual = evidence.bucket_totals.sort_values(
        ["market_class", "powertrain", "bucket"]
    ).reset_index(drop=True)
    if not actual.equals(expected):
        raise ValueError(
            "Range bucket totals do not reconcile with baseline sales records"
        )
    unknown = int(records.loc[records.status.ne("resolved_bucket"), "sales"].sum())
    if unknown:
        raise RangeRepresentationBlocked(
            f"Range representation blocked: {unknown} baseline sales have invalid/unresolved CD ranges; inspect unresolved_sales.csv; no renormalization is authorized"
        )
    for _, group in expected.groupby(["market_class", "powertrain"]):
        if group.total_sales.iloc[0] <= 0 or not np.isclose(
            group.share_of_all_sales.sum(),
            1,
            atol=rules.share_sum_tolerance,
            rtol=0,
        ):
            raise RangeRepresentationBlocked(
                "Range representation lacks complete positive within-group sales shares"
            )


def sales_bucket_totals(records: pd.DataFrame, rules: RangeRules) -> pd.DataFrame:
    if (
        records.source_row_id.duplicated().any()
        or records.source_row_id.isna().any()
        or not np.isfinite(records.sales).all()
        or (records.sales < 0).any()
        or (records.sales % 1 != 0).any()
        or not records.market_class.isin(rules.category_to_market.values()).all()
        or not records.powertrain.isin(rules.buckets).all()
    ):
        raise ValueError("OMEGA sales records are duplicated or invalid")
    for row in records.loc[records.status.eq("resolved_bucket")].itertuples(
        index=False
    ):
        if row.bucket != range_bucket(row.powertrain, float(row.cd_range_miles), rules):
            raise ValueError("Range record bucket disagrees with its native CD range")
    totals = []
    for market in sorted(set(rules.category_to_market.values())):
        for powertrain, buckets in rules.buckets.items():
            group = records.loc[
                records.market_class.eq(market) & records.powertrain.eq(powertrain)
            ]
            total = int(group.sales.sum())
            unresolved = int(
                group.loc[group.status.ne("resolved_bucket"), "sales"].sum()
            )
            for bucket in buckets:
                count = int(
                    group.loc[
                        group.status.eq("resolved_bucket")
                        & group.bucket.eq(bucket.sub_category),
                        "sales",
                    ].sum()
                )
                totals.append(
                    {
                        "market_class": market,
                        "powertrain": powertrain,
                        "bucket": bucket.sub_category,
                        "sales": count,
                        "total_sales": total,
                        "unresolved_sales": unresolved,
                        "share_of_all_sales": count / total if total else None,
                    }
                )
    return (
        pd.DataFrame(totals)
        .sort_values(["market_class", "powertrain", "bucket"])
        .reset_index(drop=True)
    )


def write_representative_decisions(
    bundle: ConfigBundle, *, evidence: RangeEvidence | None = None
) -> dict:
    """Record accepted rules separately from the per-run conservation checks."""
    decisions = {
        "aggregation_contract_accepted": True,
        "run_validated": False,
        "share_evidence": evidence.audit if evidence is not None else None,
        "cost_invest": "Sales-weighted cost per identical capacity unit/currency/year",
        "cost_variable": "Sales-weighted service cost after proving equal C2A/utilization",
        "efficiency": "Reciprocal of sales-weighted consumption per service, separately for each vehicle vintage",
        "phev_pathways": "One category-specific supporting blend; mean across model vintages of consumption-weighted electricity fractions, fixed across periods, as explicitly accepted by the user. Total energy is conserved; per-vintage component fuel differences are audited.",
        "capacity_to_activity_and_utilization": "Require identical values and units within each range family",
        "lifetimes_survival": "Fixed lifetimes and range survival curves must be identical; sales-weighted surviving fractions conserve cohort survivors without shifting the service/energy mix with age",
        "emission_embodied": "Sales-weighted lifetime gases per identical new-vehicle capacity unit",
        "historical_stock": "Keep EX technologies/stock and historical source lineage exactly; no new-sales redistribution",
        "charging_profiles": "Frozen fleet shape already embeds ranges; retain independently selected proxy; no range multiplication",
        "structure": "New category-specific representative labels/blends must replace investable variants without dropping shared historical or other-class paths",
        "blocking_decisions": [],
    }
    rules = module_rules(bundle)
    write_text_atomic(
        json.dumps(decisions, indent=2, sort_keys=True) + "\n",
        resolve_artifact_path(bundle, "ldv_range_validation", rules.files["decisions"]),
    )
    return decisions


def prepare_range_rows(
    bundle: ConfigBundle,
    *,
    evidence: RangeEvidence | None = None,
    technologies: pd.DataFrame | None = None,
    publish: bool = True,
) -> RangePreparation:
    selection = bundle.scenario.BEV_PHEV_range_representation
    rules = module_rules(bundle)
    region_map = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ]["region_output_map"]
    regions = tuple(
        sorted(region_map.get(r, r) for r in bundle.scenario.geography.regions)
    )
    scope_super_groups = [
        f"{rules.group_prefix}:{category}:{powertrain.lower()}"
        for category in rules.category_to_market
        for powertrain in rules.buckets
    ]
    scope_groups = [
        *scope_super_groups,
        *[
            f"{rules.group_prefix}:{category}:{powertrain.lower()}:{bucket.sub_category}"
            for category in rules.category_to_market
            for powertrain, buckets in rules.buckets.items()
            for bucket in buckets
        ],
    ]
    if technologies is None:
        technologies = pd.read_csv(
            resolve_input_path(bundle, "template", "technology.csv")
        )
    variant_categories = {
        b.sub_category for buckets in rules.buckets.values() for b in buckets
    }
    scope = {
        "scope_variant_technologies": sorted(
            technologies.loc[
                technologies.category.isin(rules.category_to_market)
                & technologies.sub_category.isin(variant_categories)
                & technologies.tech.str.endswith(rules.new_suffix),
                "tech",
            ]
        ),
        "scope_representative_technologies": sorted(
            value
            for f in rules.representative.families.values()
            for key, value in f.model_dump().items()
            if key != "phev_commodity"
        ),
        "scope_representative_commodities": sorted(
            f.phev_commodity for f in rules.representative.families.values()
        ),
    }
    if selection.mode == "none":
        LOGGER.info(
            "Range representation none: no share acquisition, constraints or technology changes"
        )
        result = RangePreparation(
            [],
            [],
            [],
            None,
            [],
            {
                "mode": "none",
                "share_rows": 0,
                "scope_super_groups": scope_super_groups,
                "scope_group_names": scope_groups,
                **scope,
            },
            regions,
        )
    else:
        evidence = build_range_evidence(bundle) if evidence is None else evidence
        validate_share_evidence(evidence, rules=rules)
        if any(
            c.source_key != EPA_SOURCE or c.component_key != EPA_COMPONENT
            for c in evidence.provenance_contexts
        ):
            raise RangeRepresentationBlocked(
                "Range representation requires the registered OMEGA baseline sales/range component"
            )
        if selection.mode == "representative_archetype":
            result = RangePreparation(
                [],
                [],
                [],
                None,
                evidence.provenance_contexts,
                {
                    "mode": selection.mode,
                    "share_rows": 0,
                    "evidence": evidence.audit,
                    "scope_super_groups": scope_super_groups,
                    "scope_group_names": scope_groups,
                    **scope,
                },
                regions,
                evidence=evidence,
            )
            if publish:
                _publish_range_rows(bundle, result)
                write_representative_decisions(bundle, evidence=evidence)
            return result
        if technologies is None:
            technologies = pd.read_csv(
                resolve_input_path(bundle, "template", "technology.csv")
            )
        context = resolve_composite_provenance(
            inputs=evidence.provenance_contexts,
            dataset_key="ldv_range_minimum_new_capacity_shares",
            transformation="Within-class/powertrain minimum new capacity shares",
            transformation_version="1",
            governing_source_id=evidence.provenance_contexts[0].governing_source_id,
            value_variant={
                "selection": selection.model_dump(),
                "rules": rules.model_dump(),
            },
        )
        data_id = make_data_id(
            dataset_key="canoe_transport_range_groups",
            source_version="internal",
            transformation="Modern LDV category/subcategory new-build groups",
            transformation_version="1",
            value_variant={
                "mapping": rules.category_to_market,
                "buckets": {
                    k: [b.sub_category for b in v] for k, v in rules.buckets.items()
                },
            },
        )
        dataset = DataSet(
            data_id=data_id,
            label="Internal transport range group structure",
            version="1",
            description="Backend-owned class/powertrain/range membership; no external DQ or source identity",
        )
        groups = []
        members = []
        records = []
        for category, market in rules.category_to_market.items():
            for powertrain, buckets in rules.buckets.items():
                names = {b.sub_category for b in buckets}
                targets = technologies.loc[
                    technologies.category.eq(category)
                    & technologies.sub_category.isin(names)
                    & technologies.tech.str.endswith(rules.new_suffix)
                ]
                if (
                    targets.tech.duplicated().any()
                    or set(targets.sub_category) != names
                    or len(targets) != len(names)
                ):
                    raise ValueError(
                        f"Incomplete/ambiguous investable range family: {category}/{powertrain}"
                    )
                super_group = f"{rules.group_prefix}:{category}:{powertrain.lower()}"
                groups.append(
                    TechGroup(
                        group_name=super_group,
                        data_id=data_id,
                        notes="Denominator: new-build class/powertrain capacity only",
                    )
                )
                for row in targets.itertuples(index=False):
                    sub_group = f"{super_group}:{row.sub_category}"
                    groups.append(
                        TechGroup(
                            group_name=sub_group,
                            data_id=data_id,
                            notes="Numerator: one investable range variant",
                        )
                    )
                    members.extend(
                        [
                            TechGroupMember(
                                group_name=g, tech=row.tech, data_id=data_id
                            )
                            for g in [super_group, sub_group]
                        ]
                    )
                    selected = evidence.bucket_totals.loc[
                        evidence.bucket_totals.market_class.eq(market)
                        & evidence.bucket_totals.powertrain.eq(powertrain)
                        & evidence.bucket_totals.bucket.eq(row.sub_category)
                    ]
                    if len(selected) != 1:
                        raise ValueError("Range share bucket ownership is ambiguous")
                    share = float(selected.iloc[0].share_of_all_sales)
                    records.extend(
                        {
                            "region": region,
                            "sub_group": sub_group,
                            "super_group": super_group,
                            "vintage": vintage,
                            "operator": rules.constraint_operator,
                            "share": share,
                            "notes": "Minimum within-class/powertrain share of new capacity; Legacy OMEGA US MY2022 baseline proxy held across model vintages; not adoption/stock share",
                        }
                        for region in regions
                        for vintage in bundle.scenario.periods.model
                    )
        rows = validate_parameter_rows(LimitNewCapacityShare, records, context)
        result = RangePreparation(
            rows,
            groups,
            members,
            dataset,
            [context],
            {
                "mode": selection.mode,
                "share_rows": len(rows),
                "operator": "ge",
                "denominator": "new capacity in class/powertrain family",
                "units": "dimensionless fraction",
                "evidence": evidence.audit,
                "scope_super_groups": scope_super_groups,
                "scope_group_names": scope_groups,
                **scope,
            },
            regions,
            frozenset(
                (r.region, r.sub_group, r.super_group, r.vintage, r.share) for r in rows
            ),
            frozenset((r.group_name, r.tech) for r in members),
        )
    validate_range_preparation(result)
    if publish:
        _publish_range_rows(bundle, result)
    return result


def _publish_range_rows(bundle: ConfigBundle, result: RangePreparation) -> None:
    rules = module_rules(bundle)
    directory = resolve_artifact_path(bundle, "ldv_range_processed")
    for rows, key, model in (
        (result.share_rows, "constraints", LimitNewCapacityShare),
        (result.group_rows, "groups", TechGroup),
        (result.member_rows, "members", TechGroupMember),
    ):
        write_dataframe_atomic(
            pd.DataFrame(
                [r.model_dump(mode="json") for r in rows],
                columns=list(model.model_fields),
            ),
            directory / rules.files[key],
        )
    # Reset the representation handoff when switching modes; it is never an input authority.
    write_text_atomic(
        json.dumps({"mode": result.audit["mode"], "tables": {}}, indent=2) + "\n",
        directory / rules.representative.parameter_file,
    )
    write_text_atomic(
        json.dumps(
            {
                "mode": result.audit["mode"],
                "run_validated": False,
                "parameter_stage": "requires supplied parameter batches"
                if result.audit["mode"] == "representative_archetype"
                else "inactive",
            },
            indent=2,
        )
        + "\n",
        resolve_artifact_path(
            bundle, "ldv_range_validation", rules.representative.audit_file
        ),
    )


def _batch_fingerprint(batches: dict[str, list[CanoeBaseModel]]) -> str:
    payload = {
        table: sorted(
            (r.model_dump(mode="json") for r in rows),
            key=lambda r: json.dumps(r, sort_keys=True),
        )
        for table, rows in sorted(batches.items())
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def validate_representative_preparation(result: RepresentativePreparation) -> None:
    if _batch_fingerprint(result.batches) != result.fingerprint:
        raise ValueError("Representative batches changed after conservation validation")
    if {r.tech for r in result.labels["technology_label"]} != {
        r.tech for r in result.batches["technology"]
    } or {r.commodity for r in result.labels["commodity_label"]} != {
        r.name for r in result.batches["commodity"]
    }:
        raise ValueError("Representative labels do not cover the validated structure")
    for rows in result.batches.values():
        for row in rows:
            type(row).model_validate(row.model_dump())
    contexts = {c.data_id: c.parameter_fields() for c in result.provenance_contexts}
    for table, rows in result.batches.items():
        if table in {"technology", "commodity"}:
            continue
        for row in rows:
            fields = contexts.get(row.data_id)
            if fields and any(getattr(row, k) != v for k, v in fields.items()):
                raise ValueError(
                    "Representative parameter source/DQ fields disagree with lineage"
                )


def prepare_representative_parameters(
    bundle: ConfigBundle,
    *,
    range_preparation: RangePreparation,
    batches: dict[str, list[CanoeBaseModel]],
    provenance_contexts: list[ResolvedProvenance],
    internal_datasets: tuple[DataSet, ...] = (),
    publish: bool = True,
) -> RepresentativePreparation:
    """Aggregate supplied prerequisites once, independently of acquisition and SQLite.

    Sales weights are capacity weights. Equal C2A and utilization are prerequisites
    for using them as service weights. Unit/structure/coverage differences fail;
    they are not silently averaged. EX rows and all non-target rows are retained.
    """
    rules = module_rules(bundle)
    spec = rules.representative
    evidence = range_preparation.evidence
    if (
        range_preparation.audit["mode"] != "representative_archetype"
        or evidence is None
    ):
        raise ValueError(
            "Representative preparation requires validated representative share evidence"
        )
    validate_share_evidence(evidence, rules=rules)
    if any(
        c.source_key != EPA_SOURCE or c.component_key != EPA_COMPONENT
        for c in evidence.provenance_contexts
    ):
        raise RangeRepresentationBlocked(
            "Representative parameters require registered OMEGA evidence"
        )
    contexts = {
        c.data_id: c for c in [*provenance_contexts, *evidence.provenance_contexts]
    }
    internal_ids = {d.data_id for d in internal_datasets}
    dataset = DataSet(
        data_id=make_data_id(
            dataset_key="canoe_transport_ev_representatives",
            source_version="internal",
            transformation="Scenario-local LDV representative structure",
            transformation_version="1",
            value_variant=spec.model_dump(),
        ),
        label="Internal transport LDV representative structure",
        version="1",
        description="Scenario-local investable LDV labels and unlimited unit-energy PHEV blends; original EX stock and shared charger structures retained",
    )
    output = {table: list(rows) for table, rows in batches.items()}
    technology = {r.tech: r for r in batches.get("technology", [])}
    commodity = {r.name: r for r in batches.get("commodity", [])}
    generated_contexts: dict[str, ResolvedProvenance] = {}
    generated: dict[str, list[CanoeBaseModel]] = defaultdict(list)
    conservation = []
    fuel_audit = []
    scope_variants: set[str] = set()
    regions = set(range_preparation.regions)
    periods = bundle.scenario.periods.model
    provenance_fields = {
        "data_id",
        "data_source",
        "dq_cred",
        "dq_geog",
        "dq_struc",
        "dq_tech",
        "dq_time",
        "notes",
    }

    def context_for(rows: list[CanoeBaseModel], table: str) -> ResolvedProvenance:
        native = {r.data_id: contexts[r.data_id] for r in rows if r.data_id in contexts}
        if any(
            r.data_id not in contexts and r.data_id not in internal_ids for r in rows
        ):
            raise ValueError(f"Missing prerequisite lineage for representative {table}")
        inputs = {**native, **{c.data_id: c for c in evidence.provenance_contexts}}
        governing = (
            next(iter(native.values())).governing_source_id
            if native
            else evidence.provenance_contexts[0].governing_source_id
        )
        context = resolve_composite_provenance(
            inputs=list(inputs.values()),
            dataset_key=f"ldv_ev_representative_{table}",
            transformation=f"LDV representative {table} aggregation",
            transformation_version="1",
            governing_source_id=governing,
            value_variant={
                "aggregation": spec.model_dump(),
                "internal_inputs": sorted(
                    {r.data_id for r in rows if r.data_id in internal_ids}
                ),
            },
        )
        generated_contexts[context.data_id] = context
        return context

    values = {
        "capacity_to_activity": ("c2a", "identity"),
        "limit_annual_capacity_factor": ("factor", "identity"),
        "lifetime_tech": ("lifetime", "identity"),
        "lifetime_survival_curve": ("fraction", "weighted"),
        "efficiency": ("efficiency", "harmonic"),
        "cost_invest": ("cost", "weighted"),
        "cost_fixed": ("cost", "weighted"),
        "cost_variable": ("cost", "weighted"),
        "emission_embodied": ("value", "weighted"),
    }
    for category, market in rules.category_to_market.items():
        family_spec = spec.families[category]
        for powertrain, buckets in rules.buckets.items():
            targets = {
                r.sub_category: r
                for r in technology.values()
                if r.category == category
                and r.sub_category in {b.sub_category for b in buckets}
                and r.tech.endswith(rules.new_suffix)
            }
            if set(targets) != {b.sub_category for b in buckets} or len(targets) != len(
                buckets
            ):
                raise ValueError(
                    f"Incomplete representative technology family {category}/{powertrain}"
                )
            family = {r.tech for r in targets.values()}
            scope_variants.update(family)
            if any(r.tech in family for r in batches.get("existing_capacity", [])):
                raise RangeRepresentationBlocked(
                    "Representative new-sales weights cannot redistribute stock on range variants"
                )
            weights = {
                targets[r.bucket].tech: float(r.share_of_all_sales)
                for r in evidence.bucket_totals.loc[
                    evidence.bucket_totals.market_class.eq(market)
                    & evidence.bucket_totals.powertrain.eq(powertrain)
                ].itertuples()
            }
            representative = getattr(family_spec, powertrain)
            prototype = next(iter(targets.values()))
            metadata = [
                r.model_dump(
                    exclude={"tech", "sub_category", "description", "notes", "data_id"}
                )
                for r in targets.values()
            ]
            if any(m != metadata[0] for m in metadata):
                raise RangeRepresentationBlocked(
                    f"Representative structural flags differ: {category}/{powertrain}"
                )
            new_tech = TransportationTechnology.model_validate(
                {
                    **prototype.model_dump(),
                    "tech": representative,
                    "sub_category": powertrain.lower(),
                    "description": f"Sales-weighted representative new {category} {powertrain}",
                    "notes": "OMEGA MY2022 range mix; historical stock retained on original EX technologies",
                    "data_id": dataset.data_id,
                }
            )
            generated["technology"].append(new_tech)
            phev_efficiencies = []
            for table, (field, method) in values.items():
                tech_field = (
                    "tech_or_group"
                    if table == "limit_annual_capacity_factor"
                    else "tech"
                )
                selected = [
                    r
                    for r in batches.get(table, [])
                    if getattr(r, tech_field) in family and r.region in regions
                ]
                required = table in {
                    "capacity_to_activity",
                    "limit_annual_capacity_factor",
                    "efficiency",
                    "cost_invest",
                    "cost_variable",
                }
                if not selected:
                    if required:
                        raise RangeRepresentationBlocked(
                            f"Representative mode requires supplied {table} prerequisites for {category}/{powertrain}"
                        )
                    continue
                expected_region_vintages = {
                    (region, vintage) for region in regions for vintage in periods
                }
                if table in {
                    "efficiency",
                    "cost_invest",
                    "cost_variable",
                    "limit_annual_capacity_factor",
                    "lifetime_survival_curve",
                    "emission_embodied",
                }:
                    if {
                        (r.region, r.vintage) for r in selected
                    } != expected_region_vintages:
                        raise RangeRepresentationBlocked(
                            f"Representative {table} region/vintage prerequisites are incomplete"
                        )
                elif {r.region for r in selected} != regions:
                    raise RangeRepresentationBlocked(
                        f"Representative {table} region prerequisites are incomplete"
                    )
                grouped: dict[tuple, list[CanoeBaseModel]] = defaultdict(list)
                for row in selected:
                    type(row).model_validate(row.model_dump())
                    context = contexts.get(row.data_id)
                    if context is not None:
                        if any(
                            getattr(row, k) != v
                            for k, v in context.parameter_fields().items()
                        ):
                            raise ValueError(
                                "Representative prerequisite source/DQ fields disagree with lineage"
                            )
                    elif (
                        row.data_id not in internal_ids
                        or row.data_source is not None
                        or any(
                            getattr(row, k) is not None
                            for k in (
                                "dq_cred",
                                "dq_geog",
                                "dq_struc",
                                "dq_tech",
                                "dq_time",
                            )
                        )
                    ):
                        raise ValueError(
                            "Representative prerequisite lacks registered external or internal lineage"
                        )
                    if hasattr(row, "vintage") and row.vintage not in periods:
                        raise ValueError(
                            "New-sales range aggregation cannot redistribute historical vintages"
                        )
                    key_fields = [
                        k
                        for k in row.__primary_key__
                        if k not in {tech_field, "data_id"}
                        and not (
                            table == "efficiency"
                            and powertrain == "PHEV"
                            and k == "input_comm"
                        )
                    ]
                    grouped[tuple(getattr(row, k) for k in key_fields)].append(row)
                for key, group in sorted(
                    grouped.items(), key=lambda item: repr(item[0])
                ):
                    if {getattr(r, tech_field) for r in group} != family or len(
                        group
                    ) != len(family):
                        raise RangeRepresentationBlocked(
                            f"Representative {table} family coverage differs: {category}/{powertrain}/{key}"
                        )
                    excluded = provenance_fields | {field, tech_field}
                    if table == "efficiency" and powertrain == "PHEV":
                        excluded.add("input_comm")
                    attributes = [r.model_dump(exclude=excluded) for r in group]
                    if any(a != attributes[0] for a in attributes):
                        raise RangeRepresentationBlocked(
                            f"Representative {table} units/relationships differ: {category}/{powertrain}/{key}"
                        )
                    native_values = [float(getattr(r, field)) for r in group]
                    if not all(isfinite(x) for x in native_values) or (
                        method == "harmonic" and min(native_values) <= 0
                    ):
                        raise ValueError(f"Invalid representative {table} values")
                    if table == "lifetime_survival_curve" and not np.allclose(
                        native_values,
                        native_values[0],
                        rtol=0,
                        atol=spec.equality_tolerance,
                    ):
                        raise RangeRepresentationBlocked(
                            "Different range survival curves would change the fuel/service mix with age; representative vintage-only efficiency cannot conserve that composition"
                        )
                    weighted = sum(
                        weights[getattr(r, tech_field)]
                        * (
                            1 / float(getattr(r, field))
                            if method == "harmonic"
                            else float(getattr(r, field))
                        )
                        for r in group
                    )
                    if method == "identity":
                        if not np.allclose(
                            native_values,
                            native_values[0],
                            rtol=0,
                            atol=spec.equality_tolerance,
                        ):
                            raise RangeRepresentationBlocked(
                                f"Representative {table} requires identical range values: {category}/{powertrain}/{key}"
                            )
                        value = native_values[0]
                    else:
                        value = 1 / weighted if method == "harmonic" else weighted
                    payload = {
                        **group[0].model_dump(),
                        tech_field: representative,
                        field: value,
                    }
                    if table == "capacity_to_activity" or table == "lifetime_tech":
                        if len({r.data_id for r in group}) != 1:
                            raise RangeRepresentationBlocked(
                                f"Identical {table} prerequisites have conflicting lineage"
                            )
                    else:
                        payload.update(context_for(group, table).parameter_fields())
                    payload["notes"] = (
                        f"{spec.efficiency if method == 'harmonic' else method}; OMEGA MY2022 {category}/{powertrain}; original data IDs in conservation audit"
                    )
                    if table == "efficiency" and powertrain == "PHEV":
                        payload["input_comm"] = family_spec.phev_commodity
                        phev_efficiencies.append((group, weights, value))
                    new_row = type(group[0]).model_validate(payload)
                    generated[table].append(new_row)
                    actual = 1 / value if method == "harmonic" else value
                    conservation.append(
                        {
                            "table": table,
                            "category": category,
                            "powertrain": powertrain,
                            "key": list(key),
                            "expected": weighted,
                            "actual": actual,
                            "absolute_error": abs(actual - weighted),
                            "units": payload.get("units"),
                            "input_data_ids": sorted({r.data_id for r in group}),
                            "output_data_id": new_row.data_id,
                        }
                    )
            if not any(
                r.tech in family for r in batches.get("lifetime_tech", [])
            ) and not any(
                r.tech in family for r in batches.get("lifetime_survival_curve", [])
            ):
                raise RangeRepresentationBlocked(
                    f"Representative mode requires supplied lifetime/survival prerequisites for {category}/{powertrain}"
                )
            if powertrain == "PHEV":
                _prepare_representative_blend(
                    family_spec,
                    phev_efficiencies,
                    batches,
                    technology,
                    commodity,
                    spec,
                    dataset,
                    context_for,
                    generated,
                    fuel_audit,
                    periods,
                    regions,
                    category,
                )
    # No historical stock or non-target row is edited, including old shared PHEV blends.
    for table, rows in output.items():
        tech_field = (
            "tech_or_group" if table == "limit_annual_capacity_factor" else "tech"
        )
        if table == "technology":
            output[table] = [r for r in rows if r.tech not in scope_variants]
        elif (
            table not in {"commodity", "existing_capacity"}
            and rows
            and hasattr(rows[0], tech_field)
        ):
            output[table] = [
                r
                for r in rows
                if getattr(r, tech_field) not in scope_variants
                or r.region not in regions
            ]
        output[table].extend(generated.get(table, []))
    labels = {
        "technology_label": [
            TechnologyLabel(tech=r.tech) for r in output["technology"]
        ],
        "commodity_label": [
            CommodityLabel(commodity=r.name) for r in output["commodity"]
        ],
    }
    audit = {
        "mode": "representative_archetype",
        "run_validated": True,
        "rules": spec.model_dump(),
        "scope_variants": sorted(scope_variants),
        "representative_technologies": sorted(r.tech for r in generated["technology"]),
        "representative_commodities": sorted(r.name for r in generated["commodity"]),
        "generated_rows": {t: len(r) for t, r in generated.items()},
        "conservation": conservation,
        "max_conservation_error": max(
            (r["absolute_error"] for r in conservation), default=0
        ),
        "phev_component_energy": fuel_audit,
        "historical_stock_reweighted": False,
        "charging_range_shares_applied_again": False,
        "max_phev_component_relative_difference": max(
            (abs(r["electricity_relative_difference"]) for r in fuel_audit), default=0
        ),
    }
    if audit["max_conservation_error"] > spec.equality_tolerance:
        raise ValueError("Representative parameter conservation failed")
    result = RepresentativePreparation(
        output,
        labels,
        dataset,
        list(generated_contexts.values()),
        audit,
        _batch_fingerprint(output),
    )
    validate_representative_preparation(result)
    if publish:
        write_text_atomic(
            json.dumps(
                {
                    "mode": audit["mode"],
                    "tables": {
                        t: [r.model_dump(mode="json") for r in rows]
                        for t, rows in generated.items()
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            resolve_artifact_path(bundle, "ldv_range_processed", spec.parameter_file),
        )
        write_text_atomic(
            json.dumps(audit, indent=2, sort_keys=True) + "\n",
            resolve_artifact_path(bundle, "ldv_range_validation", spec.audit_file),
        )
        decisions = write_representative_decisions(bundle, evidence=evidence)
        decisions["run_validated"] = True
        write_text_atomic(
            json.dumps(decisions, indent=2, sort_keys=True) + "\n",
            resolve_artifact_path(
                bundle, "ldv_range_validation", rules.files["decisions"]
            ),
        )
    LOGGER.info(
        "Representative LDV parameters: %s; conservation error=%s; accepted PHEV component approximation max relative difference=%s",
        audit["generated_rows"],
        audit["max_conservation_error"],
        audit["max_phev_component_relative_difference"],
    )
    return result


def _prepare_representative_blend(
    family,
    efficiency_groups,
    batches,
    technology,
    commodity,
    spec,
    dataset,
    context_for,
    generated,
    audit,
    periods,
    regions,
    category,
):
    inputs = {spec.electricity_commodity, spec.fuel_commodity}
    fractions = defaultdict(list)
    original_splits = []
    vehicle_rows = []
    blend_prototype = None
    commodity_prototype = None
    for group, weights, efficiency in efficiency_groups:
        region, vintage = group[0].region, group[0].vintage
        total = 1 / efficiency
        electricity = 0.0
        for row in group:
            vehicle_rows.append(row)
            pathways = [
                r
                for r in batches["efficiency"]
                if r.region == region
                and r.vintage == vintage
                and r.output_comm == row.input_comm
            ]
            blend_names = {r.tech for r in pathways}
            if (
                len(blend_names) != 1
                or {r.input_comm for r in pathways} != inputs
                or len(pathways) != 2
                or any(r.efficiency != 1 or r.units != "PJ/PJ" for r in pathways)
            ):
                raise RangeRepresentationBlocked(
                    "Representative PHEV supporting pathways must be one unit-energy gasoline/electricity blend"
                )
            blend = next(iter(blend_names))
            blend_prototype = technology[blend]
            commodity_prototype = commodity[row.input_comm]
            if (
                not blend_prototype.unlim_cap
                or not blend_prototype.annual
                or commodity_prototype.units != "PJ"
                or commodity_prototype.flag.value != "p"
            ):
                raise RangeRepresentationBlocked(
                    "Representative PHEV source structure must be an unlimited annual PJ blend"
                )
            split = [
                r
                for r in batches["limit_tech_input_split"]
                if r.region == region and r.tech == blend
            ]
            original_splits.extend(split)
            if {r.period for r in split} != set(periods):
                raise ValueError("PHEV source split period coverage is incomplete")
            for period in periods:
                pair = [r for r in split if r.period == period]
                if (
                    len(pair) != 2
                    or {r.input_comm for r in pair} != inputs
                    or any(r.operator.value != "e" for r in pair)
                    or any(
                        not isfinite(r.proportion) or not 0 <= r.proportion <= 1
                        for r in pair
                    )
                    or not np.isclose(
                        sum(r.proportion for r in pair),
                        1,
                        rtol=0,
                        atol=spec.equality_tolerance,
                    )
                ):
                    raise RangeRepresentationBlocked(
                        "PHEV source split must be a complementary equality energy split"
                    )
            elc = {
                r.proportion
                for r in split
                if r.input_comm == spec.electricity_commodity
            }
            if len(elc) != 1:
                raise RangeRepresentationBlocked(
                    "Representative PHEV averaging requires the existing constant-horizon source splits"
                )
            electricity += weights[row.tech] * next(iter(elc)) / row.efficiency
        fractions[region].append((vintage, electricity / total, total))
    context = context_for([*original_splits, *vehicle_rows], "limit_tech_input_split")
    generated["technology"].append(
        TransportationTechnology.model_validate(
            {
                **blend_prototype.model_dump(),
                "tech": family.phev_blend,
                "description": f"Representative gasoline/electricity blend for {category}",
                "data_id": dataset.data_id,
                "notes": "Period-indexed horizon mean energy split; all vehicle vintages share this blend",
            }
        )
    )
    generated["commodity"].append(
        Commodity.model_validate(
            {
                **commodity_prototype.model_dump(),
                "name": family.phev_commodity,
                "data_id": dataset.data_id,
                "description": f"Representative {category} PHEV energy mixture",
            }
        )
    )
    for region in sorted(regions):
        values = fractions[region]
        if len(values) != len(periods) or {v for v, _, _ in values} != set(periods):
            raise ValueError(
                "Representative PHEV requires one consumption-weighted fraction per model vintage"
            )
        mean = float(np.mean([f for _, f, _ in values]))
        for period in periods:
            for input_comm, proportion in (
                (spec.electricity_commodity, mean),
                (spec.fuel_commodity, 1 - mean),
            ):
                generated["limit_tech_input_split"].append(
                    LimitTechInputSplit(
                        region=region,
                        period=period,
                        tech=family.phev_blend,
                        input_comm=input_comm,
                        operator="e",
                        proportion=proportion,
                        notes="Mean of vintage consumption-weighted OMEGA range-mixture energy shares; fixed horizon; component approximation audited",
                        **context.parameter_fields(),
                    )
                )
        for vintage, fraction, total in values:
            for input_comm in sorted(inputs):
                generated["efficiency"].append(
                    Efficiency(
                        region=region,
                        vintage=vintage,
                        tech=family.phev_blend,
                        input_comm=input_comm,
                        output_comm=family.phev_commodity,
                        efficiency=1,
                        units="PJ/PJ",
                        data_id=dataset.data_id,
                        notes="Unlimited unit-energy supporting blend; no vehicle stock or investment capacity",
                    )
                )
            audit.append(
                {
                    "category": category,
                    "region": region,
                    "vintage": vintage,
                    "total_energy_per_service": total,
                    "vintage_electricity_fraction": fraction,
                    "horizon_mean_electricity_fraction": mean,
                    "expected_electricity": total * fraction,
                    "representative_electricity": total * mean,
                    "expected_gasoline": total * (1 - fraction),
                    "representative_gasoline": total * (1 - mean),
                    "electricity_relative_difference": mean / fraction - 1
                    if fraction
                    else 0.0,
                    "gasoline_relative_difference": (1 - mean) / (1 - fraction) - 1
                    if fraction != 1
                    else 0.0,
                    "accepted_approximation": "arithmetic mean across configured vehicle vintages, matching the existing NLR endpoint averaging approach",
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/legacy_reproduction.yaml"
    )
    parser.add_argument("--evidence-only", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    bundle = load_config_bundle(args.scenario)
    if (
        not args.evidence_only
        and bundle.scenario.BEV_PHEV_range_representation.mode
        == "representative_archetype"
    ):
        raise SystemExit(
            "Representative mode requires supplied parameter batches through prepare_representative_parameters, or the shared src/build_transport.py entrypoint; this CLI prepares range evidence/share rows only"
        )
    result = (
        build_range_evidence(bundle)
        if args.evidence_only
        else prepare_range_rows(bundle)
    )
    print(json.dumps(result.audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
