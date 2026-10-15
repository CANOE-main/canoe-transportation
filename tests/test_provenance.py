from __future__ import annotations

from pathlib import Path

import pytest

from utils import load_config_bundle
from validation.config_models import DataQuality, SourcesConfig
from validation.provenance import (
    ProvenanceError,
    make_data_id,
    registry_rows,
    resolve_composite_provenance,
    resolve_provenance,
    source_id_mapping,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/default.yaml"


@pytest.fixture
def bundle():
    return load_config_bundle(SCENARIO, repo_root=REPO_ROOT)


def test_explicit_source_ids_preserve_retired_gaps(bundle) -> None:
    assert source_id_mapping(bundle.sources) == {
        "nrcan_ceud_transport_provincial": "T01",
        "nrcan_ceud_transport_national": "T02",
        "ontario_ministry_transport_vehicle_population": "T03",
        "statcan_transport_tables": "T04",
        "cer_canadas_energy_future": "T05",
        "nlr_atb_transportation": "T06",
        "anl_autonomie_bean": "T07",
        "nrcan_fuel_consumption_ratings": "T08",
        "nhtsa_cafe_ldv_survival": "T09",
        "eia_nems_hd_truck_scrappage": "T10",
        "jgcri_gcam_motorcycle_inputs": "T11",
        "epri_us_regen_transportation": "T12",
        "faa_economic_values": "T13",
        "wards_intelligence_sales_shares": "T14",
        "emrg_sfu_cims_model": "T15",
        "open_energy_outlook": "T16",
        "argonne_rd_greet": "T17",
        "canada_energy_policy_simulator": "T19",
        "argonne_hdsam": "T20",
        "fueleconomy_gov_vehicle_data": "T21",
        "nhtsa_vpic_vehicle_models": "T23",
        "dunsky_ev_charging_infrastructure": "T24",
        "transport_canada_ev_dashboard": "T25",
        "pollution_probe_ev_charging_survey": "T26",
        "nlr_alternative_fueling_infrastructure": "T27",
        "icct_ev_charge": "T28",
        "icct_hdv_charge": "T29",
        "iea_future_of_hydrogen": "T30",
        "legacy_charging_profiles": "T31",
        "epa_omega_baseline": "T33",
        "hatch_original_growth_evidence": "T34",
        "hatch_extended_growth_evidence": "T35",
        "eafo_norway_growth_evidence": "T36",
        "epa_moves_heavy_truck_survival": "T39",
    }


def test_data_id_is_stable_and_changes_only_for_value_variants() -> None:
    base = make_data_id(
        dataset_key="source.component",
        source_version="2021",
        transformation="normalize",
        transformation_version="1",
    )
    repeated = make_data_id(
        dataset_key="source.component",
        source_version="2021",
        transformation="normalize",
        transformation_version="1",
    )
    variant = make_data_id(
        dataset_key="source.component",
        source_version="2021",
        transformation="normalize",
        transformation_version="1",
        value_variant={"region": "ON"},
    )

    assert repeated == base
    assert variant != base
    with pytest.raises(ProvenanceError, match="Runtime-only"):
        make_data_id(
            dataset_key="source.component",
            source_version="2021",
            transformation="normalize",
            transformation_version="1",
            value_variant={"cache_path": "anywhere"},
        )


def test_single_source_inherits_component_or_family_dq(bundle) -> None:
    resolved = resolve_provenance(
        bundle.sources,
        source_key="nrcan_ceud_transport_provincial",
        component_key=20,
        transformation="ceud_normalization",
        transformation_version="1",
    )

    assert resolved.source_id == "T01"
    assert resolved.parameter_fields() == {
        "data_source": "T01",
        "data_id": resolved.data_id,
        **bundle.sources.resolved_data_quality(
            "nrcan_ceud_transport_provincial", 20,
        ).row_fields(),
    }
    labels, datasets, sources = registry_rows([resolved])
    assert labels[0].source_id == "T01"
    assert datasets[0].data_id == resolved.data_id
    assert sources[0].source == bundle.sources.sources[
        "nrcan_ceud_transport_provincial"
    ].citation
    assert sources[0].notes == resolved.parameter_notes()
    assert bundle.sources.sources["nrcan_ceud_transport_provincial"].database_note in sources[0].notes
    assert "  - 20: " in sources[0].notes


def test_partial_annotations_resolve_per_indicator_and_notes_are_inherited(bundle) -> None:
    payload = bundle.sources.model_dump()
    source = payload["sources"]["nrcan_ceud_transport_provincial"]
    source["data_quality"] = {"dq_cred": 2, "dq_struc": 3}
    source["database_note"] = "Reviewed family note"
    source["components"][20]["data_quality"] = {"dq_geog": 1, "dq_struc": 4}
    source["components"][20]["database_note"] = None
    registry = SourcesConfig.model_validate(payload)
    context = resolve_provenance(
        registry, source_key="nrcan_ceud_transport_provincial", component_key=20,
        transformation="fixture", transformation_version="1",
    )
    assert context.data_quality.row_fields() == {
        "dq_cred": 2, "dq_geog": 1, "dq_struc": 4, "dq_tech": 5, "dq_time": 5,
    }
    assert registry_rows([context])[2][0].notes == (
        "nrcan_ceud_transport_provincial: Reviewed family note"
    )
    source["components"][20]["database_note"] = "Reviewed component note"
    overridden = resolve_provenance(
        SourcesConfig.model_validate(payload), source_key="nrcan_ceud_transport_provincial",
        component_key=20, transformation="fixture", transformation_version="1",
    )
    assert overridden.parameter_notes() == (
        "nrcan_ceud_transport_provincial: Reviewed family note\n"
        "  - 20: Reviewed component note"
    )
    assert overridden.data_id == context.data_id
    source["components"][20]["database_note"] = ""
    cleared = resolve_provenance(
        SourcesConfig.model_validate(payload), source_key="nrcan_ceud_transport_provincial",
        component_key=20, transformation="fixture", transformation_version="1",
    )
    assert registry_rows([cleared])[2][0].notes == context.parameter_notes()


def test_composite_preserves_distinct_component_database_notes(bundle) -> None:
    payload = bundle.sources.model_dump()
    source = payload["sources"]["nrcan_ceud_transport_provincial"]
    source["components"][20]["database_note"] = "Car evidence"
    source["components"][21]["database_note"] = "Light truck evidence"
    registry = SourcesConfig.model_validate(payload)
    contexts = [
        resolve_provenance(
            registry, source_key="nrcan_ceud_transport_provincial", component_key=key,
            transformation="fixture", transformation_version="1",
        )
        for key in (20, 21)
    ]
    combined = resolve_composite_provenance(
        inputs=contexts, dataset_key="fixture", transformation="fixture",
        transformation_version="1", governing_source_id="T01",
    )
    notes = combined.parameter_notes()
    assert notes.count(source["database_note"]) == 1
    assert "  - 20: Car evidence\n  - 21: Light truck evidence" in notes
    nested = resolve_composite_provenance(
        inputs=[combined, contexts[1], combined], dataset_key="nested_fixture",
        transformation="nested", transformation_version="1",
        governing_source_id="T01",
    )
    assert registry_rows([nested])[2][0].notes == notes
    assert nested.data_quality == combined.data_quality
    assert nested.parameter_notes() == notes


def test_composite_requires_explicit_dq_when_contributors_disagree(bundle) -> None:
    first = resolve_provenance(
        bundle.sources,
        source_key="nrcan_ceud_transport_provincial",
        component_key=20,
        transformation="normalize",
        transformation_version="1",
    )
    second = resolve_provenance(
        bundle.sources,
        source_key="statcan_transport_tables",
        component_key="20-10-0021-01",
        transformation="normalize",
        transformation_version="1",
    )
    composite = resolve_composite_provenance(
        inputs=[second, first],
        dataset_key="combined.ldv",
        transformation="combine",
        transformation_version="1",
        governing_source_id="T01",
        data_quality=first.data_quality,
    )
    reversed_order = resolve_composite_provenance(
        inputs=[first, second],
        dataset_key="combined.ldv",
        transformation="combine",
        transformation_version="1",
        governing_source_id="T01",
        data_quality=first.data_quality,
    )
    assert composite.data_id == reversed_order.data_id
    assert [item.source_id for item in composite.contributors] == ["T01", "T04"]

    different = second.model_copy(
        update={
            "data_quality": DataQuality(
                dq_cred=2,
                dq_geog=5,
                dq_struc=2,
                dq_tech=2,
                dq_time=5,
            )
        }
    )
    with pytest.raises(ProvenanceError, match="different DQ"):
        resolve_composite_provenance(
            inputs=[first, different],
            dataset_key="combined.ldv",
            transformation="combine",
            transformation_version="1",
            governing_source_id="T01",
        )


def test_conflicting_registry_definition_is_rejected(bundle) -> None:
    resolved = resolve_provenance(
        bundle.sources,
        source_key="nrcan_ceud_transport_provincial",
        component_key=20,
        transformation="template_bootstrap",
        transformation_version="1",
    )
    contributor = resolved.contributors[0]
    conflicting = resolved.model_copy(
        update={
            "contributors": (
                contributor.model_copy(update={"title": "conflicting title"}),
            )
        }
    )

    with pytest.raises(ProvenanceError, match="Conflicting data_source_label"):
        registry_rows([resolved, conflicting])


def test_composite_preserves_multiple_component_citations_from_one_source(bundle) -> None:
    ceud = resolve_provenance(
        bundle.sources,
        source_key="nrcan_ceud_transport_provincial",
        component_key=21,
        transformation="stock",
        transformation_version="1",
    )
    archived = resolve_provenance(
        bundle.sources,
        source_key="statcan_transport_tables",
        component_key="20-10-0021-01",
        transformation="fuel_mix",
        transformation_version="1",
    )
    current = resolve_provenance(
        bundle.sources,
        source_key="statcan_transport_tables",
        component_key="20-10-0025-01",
        transformation="fuel_mix",
        transformation_version="1",
    )

    combined = resolve_composite_provenance(
        inputs=[ceud, archived, current],
        dataset_key="existing_capacity.cars",
        transformation="disaggregate",
        transformation_version="1",
        governing_source_id="T01",
        data_quality=ceud.data_quality,
    )

    statcan = next(item for item in combined.contributors if item.source_id == "T04")
    assert "2010002101-eng" in statcan.citation
    assert "2010002501-eng" in statcan.citation
    assert [item.source_id for item in combined.contributors] == ["T01", "T04"]
