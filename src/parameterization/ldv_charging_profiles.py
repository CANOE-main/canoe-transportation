"""Inspectable charging factors with explicit projection onto CANOE time structures."""

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
import hashlib
import json
import logging

from canoe_schema.v4_0 import CapacityFactorTech, TimeOfDay, TimeSeason
import numpy as np
import pandas as pd

from fetching.legacy_charging_profiles import (
    ProfileEvidence,
    SOURCE,
    build_request,
    normalize_profile,
)
from utils import (
    ConfigBundle,
    load_config_bundle,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_input_path,
    resolve_repo_path,
    write_dataframe_atomic,
    write_text_atomic,
)
from validation.insertion import validate_parameter_rows
from validation.provenance import ResolvedProvenance, resolve_provenance

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChargingPreparation:
    rows: list[CapacityFactorTech]
    seasons: list[TimeSeason]
    times_of_day: list[TimeOfDay]
    provenance_contexts: list[ResolvedProvenance]
    hourly: pd.DataFrame
    mapping: pd.DataFrame
    audit: dict
    regions: tuple[str, ...]
    expected_keys: frozenset[tuple[str, str, str, str]] = frozenset()


def project_hours(
    evidence: ProfileEvidence,
    *,
    seasons: Sequence[TimeSeason],
    times_of_day: Sequence[TimeOfDay],
    mapping: pd.DataFrame | None,
    tolerance: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Average only through an explicit, complete mapping; preserve its year integral."""
    seasons = [TimeSeason.model_validate(s.model_dump()) for s in seasons]
    times_of_day = [TimeOfDay.model_validate(t.model_dump()) for t in times_of_day]
    if any(
        s.segment_fraction is None
        or not np.isfinite(s.segment_fraction)
        or s.segment_fraction <= 0
        for s in seasons
    ) or any(
        t.hours is None or not np.isfinite(t.hours) or t.hours <= 0
        for t in times_of_day
    ):
        raise ValueError(
            "Charging time structures require positive finite fractions/hours"
        )
    hourly = evidence.hourly
    mapping = (
        hourly[["hour_index", "season", "tod"]].copy()
        if mapping is None
        else mapping.copy()
    )
    if list(mapping.columns) != ["hour_index", "season", "tod"]:
        raise ValueError("Charging time mapping requires hour_index,season,tod columns")
    if mapping.hour_index.duplicated().any() or set(mapping.hour_index) != set(
        hourly.hour_index
    ):
        raise ValueError(
            "Charging time mapping must assign every physical hour exactly once"
        )
    axis = {(s.season, t.tod) for s in seasons for t in times_of_day}
    actual = set(zip(mapping.season, mapping.tod))
    if len({s.season for s in seasons}) != len(seasons) or len(
        {t.tod for t in times_of_day}
    ) != len(times_of_day):
        raise ValueError("Duplicate inherited charging time labels")
    if not axis or actual != axis:
        raise ValueError(
            "Charging label coverage differs from inherited CANOE axes; supply an explicit time mapping"
        )
    if not np.isclose(
        sum(s.segment_fraction for s in seasons), 1, atol=tolerance, rtol=0
    ):
        raise ValueError("CANOE season fractions must sum to one")
    duration = sum(t.hours for t in times_of_day)
    joined = mapping.merge(
        hourly[["hour_index", "factor"]], on="hour_index", validate="one_to_one"
    )
    projected = (
        joined.groupby(["season", "tod"], sort=True)
        .agg(
            factor=("factor", "mean"),
            physical_hours=("factor", "size"),
        )
        .reset_index()
    )
    weights = {
        (s.season, t.tod): len(hourly) * s.segment_fraction * t.hours / duration
        for s in seasons
        for t in times_of_day
    }
    expected = np.array(
        [weights[s, t] for s, t in zip(projected.season, projected.tod)]
    )
    if not np.allclose(expected, projected.physical_hours, atol=tolerance, rtol=0):
        raise ValueError(
            "Charging mapping durations disagree with inherited season fractions/TOD hours"
        )
    if not np.isclose(
        np.dot(expected, projected.factor), hourly.factor.sum(), atol=tolerance, rtol=0
    ):
        raise ValueError(
            "Charging temporal projection changed the normalized annual integral"
        )
    return projected, mapping.sort_values("hour_index").reset_index(drop=True)


def validate_charging_rows(
    rows: Sequence[CapacityFactorTech], *, expected_keys: set
) -> dict:
    keys = {(r.region, r.tech, r.season, r.tod) for r in rows}
    if len(keys) != len(rows) or keys != expected_keys:
        raise ValueError("Charging factor key coverage mismatch")
    for row in rows:
        CapacityFactorTech.model_validate(row.model_dump())
        if (
            row.factor is None
            or not np.isfinite(row.factor)
            or not 0 <= row.factor <= 1
        ):
            raise ValueError("Invalid charging capacity factor")
        if any(
            getattr(row, name) is None
            for name in (
                "data_id",
                "data_source",
                "dq_cred",
                "dq_geog",
                "dq_struc",
                "dq_tech",
                "dq_time",
            )
        ):
            raise ValueError("Charging factor provenance/DQ is incomplete")
    return {"ok": True, "rows": len(rows), "keys": len(keys)}


def validate_charging_preparation(
    result: ChargingPreparation,
    *,
    seasons: Sequence[TimeSeason],
    times_of_day: Sequence[TimeOfDay],
) -> None:
    """Reconcile mutable final rows with the prepared hourly values before insertion."""
    validate_charging_rows(result.rows, expected_keys=set(result.expected_keys))
    if not result.audit["enabled"]:
        if result.rows:
            raise ValueError("Disabled charging preparation contains rows")
        return
    projected, _ = project_hours(
        ProfileEvidence(result.hourly, result.audit, ""),
        seasons=seasons,
        times_of_day=times_of_day,
        mapping=result.mapping,
        tolerance=result.audit["conservation_tolerance"],
    )
    expected = {(r.season, r.tod): r.factor for r in projected.itertuples(index=False)}
    if any(
        not np.isclose(r.factor, expected[r.season, r.tod], atol=1e-12, rtol=0)
        for r in result.rows
    ):
        raise ValueError(
            "Charging rows disagree with the preserved hourly profile/projection"
        )
    contexts = {c.data_id: c.parameter_fields() for c in result.provenance_contexts}
    for row in result.rows:
        fields = contexts.get(row.data_id)
        if fields is None or any(
            getattr(row, key) != value for key, value in fields.items()
        ):
            raise ValueError("Charging rows disagree with registered provenance/DQ")


def prepare_charging_profile_rows(
    bundle: ConfigBundle,
    *,
    technologies: pd.DataFrame | None = None,
    seasons: Sequence[TimeSeason] | None = None,
    times_of_day: Sequence[TimeOfDay] | None = None,
    evidence: ProfileEvidence | None = None,
    time_mapping: pd.DataFrame | None = None,
    publish: bool = True,
) -> ChargingPreparation:
    """Use supplied evidence/structures without regenerating prerequisites or writing SQLite."""
    selection = bundle.scenario.charging_profiles
    rules = load_harmonization_rules(bundle, "ldv_charging_profiles")
    if rules["factor_units"] != "fraction":
        raise ValueError("Charging factors require dimensionless fraction units")
    region_map = load_harmonization_rules(bundle, "road_stocks_and_demands")[
        "existing_capacity"
    ]["region_output_map"]
    scope = tuple(
        sorted(region_map.get(r, r) for r in bundle.scenario.geography.regions)
    )
    if selection.travel_behavior_source == "none":
        LOGGER.info("Legacy charging profiles disabled; no source evidence required")
        result = ChargingPreparation(
            [],
            [],
            [],
            [],
            pd.DataFrame(),
            pd.DataFrame(),
            {
                "enabled": False,
                "rows": 0,
                "target_technologies": rules["target_technologies"],
            },
            scope,
        )
        if publish:
            publish_charging(bundle, result)
        return result
    if len(scope) != len(set(scope)):
        raise ValueError("Charging region mapping merges multiple selected regions")
    evidence = (
        normalize_profile(bundle, selection.travel_behavior_source)
        if evidence is None
        else evidence
    )
    request = build_request(bundle, selection.travel_behavior_source)
    if (
        evidence.source_digest != request.expected_sha256
        or evidence.audit.get("profile") != selection.travel_behavior_source
    ):
        raise ValueError(
            "Supplied charging evidence must match the selected registered source digest/profile"
        )
    if technologies is None:
        technologies = pd.read_csv(
            resolve_input_path(bundle, "template", "technology.csv")
        )
    targets = technologies.loc[technologies.tech.isin(rules["target_technologies"])]
    if (
        set(targets.tech) != set(rules["target_technologies"])
        or targets.tech.duplicated().any()
        or not targets.category.eq(rules["technology_category"]).all()
        or not targets.sub_category.eq(rules["technology_sub_category"]).all()
        or "annual" not in targets
        or not (targets.annual.isna() | targets.annual.isin([0, False])).all()
    ):
        raise ValueError(
            "Charging target technologies lack the configured non-annual LDV charger mapping"
        )
    created_seasons = []
    created_times = []
    if not seasons and not times_of_day:
        days = len(evidence.hourly) // 24
        created_seasons = [
            TimeSeason(
                season=f"D{i + 1:03d}",
                sequence=i + 1,
                segment_fraction=1 / days,
                notes="Elapsed 24 physical hours from Toronto-year start",
            )
            for i in range(days)
        ]
        created_times = [
            TimeOfDay(tod=f"H{i + 1:02d}", sequence=i + 1, hours=1) for i in range(24)
        ]
        seasons, times_of_day = created_seasons, created_times
    if not seasons or not times_of_day:
        raise ValueError(
            "Both inherited season and time-of-day structures are required"
        )
    if time_mapping is None and selection.time_mapping is not None:
        time_mapping = pd.read_csv(
            resolve_repo_path(bundle.repo_root, selection.time_mapping)
        )
    projected, mapping = project_hours(
        evidence,
        seasons=seasons,
        times_of_day=times_of_day,
        mapping=time_mapping,
        tolerance=rules["conservation_tolerance"],
    )
    digest = hashlib.sha256(mapping.to_csv(index=False).encode()).hexdigest()
    regions = scope
    context = resolve_provenance(
        bundle.sources,
        source_key=SOURCE,
        component_key=selection.travel_behavior_source,
        transformation="Legacy hourly charging mean/peak and explicit temporal projection",
        transformation_version="1",
        value_variant={
            "source_sha256": evidence.source_digest,
            "mapping_sha256": digest,
            "hourly_sha256": hashlib.sha256(
                evidence.hourly.to_csv(index=False).encode()
            ).hexdigest(),
            "rules": rules,
            "selection": selection.model_dump(),
        },
    )
    records = [
        {
            "region": region,
            "tech": tech,
            "season": r.season,
            "tod": r.tod,
            "factor": r.factor,
            "notes": f"{selection.travel_behavior_source}; legacy BEV shape proxy for shared LDV chargers; Ontario shape; inherited range composition"
            if r.tod == "H01"
            else None,
        }
        for region in regions
        for tech in targets.tech
        for r in projected.itertuples(index=False)
    ]
    rows = validate_parameter_rows(CapacityFactorTech, records, context)
    expected = {
        (region, tech, r.season, r.tod)
        for region in regions
        for tech in targets.tech
        for r in projected.itertuples(index=False)
    }
    audit = {
        **evidence.audit,
        **validate_charging_rows(rows, expected_keys=expected),
        "enabled": True,
        "regions": list(regions),
        "target_technologies": sorted(targets.tech),
        "travel_behavior_source": selection.travel_behavior_source,
        "applicability": rules["applicability"],
        "projection_sha256": digest,
        "conservation_tolerance": rules["conservation_tolerance"],
        "new_time_structures": bool(created_seasons),
        "inherited_time_structures_preserved": not created_seasons,
    }
    LOGGER.warning(
        "Using legacy Ontario BEV charging shape for shared LDV chargers in all selected regions: %s",
        regions,
    )
    LOGGER.info("Prepared %s charging factors; no composition reweighting", len(rows))
    result = ChargingPreparation(
        rows,
        created_seasons,
        created_times,
        [context],
        evidence.hourly,
        mapping,
        audit,
        scope,
        frozenset(expected),
    )
    if publish:
        publish_charging(bundle, result)
    return result


def publish_charging(bundle: ConfigBundle, result: ChargingPreparation) -> None:
    rules = load_harmonization_rules(bundle, "ldv_charging_profiles")
    directory = resolve_artifact_path(bundle, "charging_profiles_processed")
    for rows, key, columns in (
        (result.rows, "factor_file", list(CapacityFactorTech.model_fields)),
        (result.seasons, "time_season_file", list(TimeSeason.model_fields)),
        (result.times_of_day, "time_of_day_file", list(TimeOfDay.model_fields)),
    ):
        write_dataframe_atomic(
            pd.DataFrame([r.model_dump(mode="json") for r in rows], columns=columns),
            directory / rules[key],
        )
    write_dataframe_atomic(
        result.mapping.reindex(columns=["hour_index", "season", "tod"]),
        directory / rules["mapping_file"],
    )
    write_text_atomic(
        json.dumps(result.audit, indent=2, sort_keys=True) + "\n",
        resolve_artifact_path(
            bundle, "charging_profiles_validation", rules["validation_file"]
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="config/scenarios/legacy_reproduction.yaml"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    result = prepare_charging_profile_rows(load_config_bundle(args.scenario))
    print(json.dumps(result.audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
