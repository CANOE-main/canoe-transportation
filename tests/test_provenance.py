from __future__ import annotations

from pathlib import Path

import pytest

from utils import load_config_bundle
from validation.config_models import DataQuality
from validation.provenance import (
    ProvenanceError,
    make_data_id,
    registry_rows,
    resolve_composite_provenance,
    resolve_provenance,
    source_id_mapping,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"


@pytest.fixture
def bundle():
    return load_config_bundle(SCENARIO, repo_root=REPO_ROOT)


def test_yaml_order_pins_current_source_mapping(bundle) -> None:
    assert source_id_mapping(bundle.sources) == {
        "nrcan_ceud_transport_provincial": "T01",
        "nrcan_ceud_transport_national": "T02",
        "ontario_ministry_transport_vehicle_population": "T03",
        "statcan_transport_tables": "T04",
        "cer_canadas_energy_future": "T05",
        "nlr_atb_transportation_2024": "T06",
        "anl_autonomie_bean_2022": "T07",
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
        "dq_cred": 5,
        "dq_geog": 5,
        "dq_struc": 5,
        "dq_tech": 5,
        "dq_time": 5,
    }
    labels, datasets, sources = registry_rows([resolved])
    assert labels[0].source_id == "T01"
    assert datasets[0].data_id == resolved.data_id
    assert sources[0].source == bundle.sources.sources[
        "nrcan_ceud_transport_provincial"
    ].citation


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
    )
    reversed_order = resolve_composite_provenance(
        inputs=[first, second],
        dataset_key="combined.ldv",
        transformation="combine",
        transformation_version="1",
        governing_source_id="T01",
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
