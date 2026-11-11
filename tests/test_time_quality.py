"""Temporal annotations assess evidence years without replacing reviewed overrides."""

from pathlib import Path

import pytest

from utils import load_config_bundle
from validation.config_models import SourcesConfig


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def sources():
    return load_config_bundle("config/scenarios/default.yaml", repo_root=ROOT).sources


def test_production_components_have_explicit_temporal_assessments(sources):
    """Known production evidence must not silently inherit the worst fallback."""
    unresolved = []
    for source_key, source in sources.sources.items():
        if source.status != "active":
            continue
        for component_key, component in source.components.items():
            if not component.parameter_modules or component.adapter.get("development_only"):
                continue
            component_time = (
                component.data_quality.dq_time
                if component.data_quality is not None else None
            )
            if component_time is None and source.data_quality.dq_time is None:
                unresolved.append(f"{source_key}.{component_key}")
    assert unresolved == [], f"Unassessed production dq_time: {unresolved}"


def test_truck_survival_time_quality_follows_underlying_cohort_evidence(sources):
    """A recent publication does not erase substantially older observations."""
    nems = sources.resolved_data_quality(
        "eia_nems_hd_truck_scrappage", "truck_scrappage_rates",
    )
    moves = sources.resolved_data_quality(
        "epa_moves_heavy_truck_survival", "heavy_truck_survival_rates",
    )
    assert nems.dq_time < moves.dq_time


def test_native_dollar_year_is_not_the_time_quality_of_a_projected_value(sources):
    source_key = "jgcri_gcam_motorcycle_inputs"
    component_key = "canada_motorcycle_inputs"
    component = sources.sources[source_key].component(component_key)

    assert component.adapter["native_cost_dollar_year"] == 2005
    # The registered value-year trajectory covers 2005-2100, including model years.
    assert sources.resolved_data_quality(source_key, component_key).dq_time == 1


@pytest.mark.parametrize(
    ("source_key", "component_key", "reviewed_time"),
    [
        ("emrg_sfu_cims_model", "transport_process_lifetimes", 1),
        ("open_energy_outlook", "transport_process_lifetimes", 1),
        ("argonne_hdsam", "hydrogen_dispenser_lifetimes", 1),
        ("canada_energy_policy_simulator", "motorcycle_lifetimes", 4),
    ],
)
def test_reviewed_lifetime_time_quality_remains_a_component_override(
    sources, source_key, component_key, reviewed_time,
):
    payload = sources.model_dump(mode="python")
    payload["sources"][source_key]["data_quality"]["dq_time"] = 5
    changed = SourcesConfig.model_validate(payload)

    assert changed.resolved_data_quality(source_key, component_key).dq_time == reviewed_time
    assert sources.resolved_data_quality(source_key, component_key).dq_time == reviewed_time
