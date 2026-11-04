"""Reviewed vehicle maps are immutable inputs; calculations stay downstream."""

from dataclasses import replace
from hashlib import sha256

import pandas as pd
import pytest
import yaml

from parameterization.vehicle_reference import (
    CANONICAL_COLUMNS, SOURCE_LINK_COLUMNS, VehicleReference,
    attach_catalogue_identities, catalogue_family, load_vehicle_reference,
    load_vehicle_reference_frames, reference_mto_mapping, resolve_source_labels,
    validate_vehicle_reference, vehicle_id,
)


def canonical(**changes):
    row = {"vehicle_id": "FIXTURE/FAMILY", "canonical_make": "Fixture", "canonical_model": "Family",
           "family_boundary": False,
           "model_year_from": 2023, "model_year_to": 2023, "vehicle_scope": "ldv",
           "nrcan_vehicle_class": "Compact", "powertrain": "gasoline",
           "evidence_status": "catalogue_supported", "evidence_source": "NRCan fixture", "notes": ""}
    row.update(changes)
    return row


def link(**changes):
    row = {"source": "nrcan_fuel_consumption_ratings", "source_make": "Fixture", "source_model": "Family Base",
           "model_year_from": 2023, "model_year_to": 2023, "observed_powertrain": "",
           "source_vehicle_class": "Compact", "vehicle_id": "FIXTURE/FAMILY", "vehicle_scope": "",
           "link_status": "catalogue_label", "evidence_source": "NRCan fixture",
           "source_notes": "", "wards_notes": "", "canonical_description": "Family Base"}
    row.update(changes)
    return row


def reference(facts=None, links=None):
    return load_vehicle_reference_frames(
        pd.DataFrame([canonical()] if facts is None else facts, columns=CANONICAL_COLUMNS),
        pd.DataFrame([link()] if links is None else links, columns=SOURCE_LINK_COLUMNS),
    )


def native(**changes):
    row = {"Make": "Fixture", "Model": "Family Base", "year": 2023, "powertrain": "gasoline",
           "rating_class": "Compact", "count": 5}
    row.update(changes)
    return row


def test_identity_helpers_remain_single_implementation_and_labels_preserve_model_numbers():
    from parameterization.rating_composition import catalogue_family as imported
    from parameterization.road_aggregation import normalize_vehicle_text as road_import
    from utils.vehicle_labels import normalize_vehicle_text

    assert imported is catalogue_family and road_import is normalize_vehicle_text
    assert normalize_vehicle_text("Citroën C-Zéro") == "CITROENCZERO"
    assert vehicle_id("Fixture", "Family Sport",
        {"family_matching": {"trim_suffix_patterns": [r"\s+Sport$"]}}) == "FIXTURE/FAMILY"


def test_two_authoritative_tables_have_no_calculated_range_count_or_bucket_fields():
    ref = reference()
    assert list(ref.canonical) == CANONICAL_COLUMNS and list(ref.links) == SOURCE_LINK_COLUMNS
    assert not {"median_range", "eligible_configurations", "source_rows", "count"} & set(ref.canonical)
    assert not {"count", "allocated_count", "median_range"} & set(ref.links)
    assert set(ref.canonical.powertrain) == {"gasoline"}


def test_native_labels_are_registered_before_calculations_without_changing_counts_or_models():
    rows = pd.DataFrame([native(), native(count=9)])
    before = rows.copy(deep=True)
    tagged = attach_catalogue_identities(rows, reference())
    assert tagged.vehicle_id.tolist() == ["FIXTURE/FAMILY"] * 2
    assert tagged.family_model.eq("Family").all()
    assert tagged.canonical_description.eq("Family Base").all()
    assert tagged["count"].sum() == 14
    pd.testing.assert_frame_equal(tagged[before.columns], before)
    pd.testing.assert_frame_equal(rows, before)


def test_family_identity_does_not_collapse_distinct_native_trims():
    from parameterization.rating_composition import distinct_catalogue_variants

    ref = reference(links=[link(), link(source_model="Family Sport", canonical_description="Family Sport")])
    rows = pd.DataFrame([native(Model=model, included=True, component="ratings", source_row=index + 2,
        native_consumption=value, consumption_mj_per_vkm=value * .4, native_unit="L/100 km", range_km=None)
        for index, (model, value) in enumerate([("Family Base", 8.), ("Family Sport", 12.), ("Family Base", 8.)])])
    variants = distinct_catalogue_variants(attach_catalogue_identities(rows, ref), {"variant_identity_columns": ["Model"]})
    assert variants.family_key.nunique() == 1
    assert variants.rating_variant_id.nunique() == 2
    assert variants.drop_duplicates("rating_variant_id").native_consumption.median() == 10.
    assert variants.Model.tolist() == rows.Model.tolist()


def test_historical_proxies_resolve_original_model_year_links_and_preserve_native_records():
    from parameterization.rating_composition import _historical_observations

    ref = reference(facts=[canonical(powertrain="bev")], links=[link(source="transport_canada_izev",
        source_model="Short EV", observed_powertrain="bev", source_vehicle_class="", canonical_description="Family")])
    records = pd.DataFrame([{"make": "Fixture", "model": "Short EV", "model_year": 2023,
        "request_year": 2024, "province": "ON", "powertrain": "bev", "count": 3,
        "source_row": index, "Purchase or Lease": "Purchase"} for index in [2, 3]])
    baseline = pd.DataFrame([{"year": year, "powertrain": "bev_150_miles"} for year in [2010, 2011]])
    policy = {"izev_model_year_min": 2020, "izev_model_year_max": 2024,
        "izev_transactions": {"column": "Purchase or Lease", "accepted_values": ["Purchase", "Lease"]}}
    historical = _historical_observations(records, baseline, policy, ref)
    assert historical.vehicle_id.eq("FIXTURE/FAMILY").all()
    assert historical.groupby("year").source_count.sum().to_dict() == {2010: 6, 2011: 6}
    assert historical.source_rows.eq(2).all()
    assert historical.source_model_years.eq("2023").all()
    assert len(records) == 2 and records["count"].sum() == 6


def test_missing_native_link_requires_review_instead_of_automatic_identity_expansion():
    with pytest.raises(ValueError, match="No applicable reviewed"):
        attach_catalogue_identities(pd.DataFrame([native(Model="Family Touring")]), reference())


def test_resolved_source_descriptions_preserve_trim_constraints():
    ref = reference(links=[link(source="transport_canada_izev", source_model="Short EV",
        source_vehicle_class="", observed_powertrain="bev", canonical_description="Family Long Range AWD")])
    rows = pd.DataFrame([{"make": "Fixture", "model": "Short EV", "year": 2023, "powertrain": "bev", "count": 7}])
    resolved = resolve_source_labels(rows, ref, source="transport_canada_izev",
        make_column="make", model_column="model", year_column="year", powertrain_column="powertrain")
    assert resolved.family_model.item() == "Family"
    assert resolved.canonical_description.item() == "Family Long Range AWD"
    assert resolved["count"].item() == 7


def test_supported_multi_class_powertrain_and_unknown_scopes_remain_separate():
    ref = reference(facts=[
        canonical(),
        canonical(powertrain="diesel"),
        canonical(powertrain="hev"),
        canonical(powertrain="bev", nrcan_vehicle_class="Mid-size"),
        canonical(powertrain="unknown", evidence_status="propulsion_unresolved"),
    ])
    assert len(ref.canonical) == 5
    assert set(ref.canonical.powertrain) == {"gasoline", "diesel", "hev", "bev", "unknown"}


def test_canonical_availability_validates_propulsion_without_classifying_a_trim():
    rows = pd.DataFrame([native(propulsion_resolved=False)])
    ref = reference(facts=[canonical(powertrain="unknown", evidence_status="propulsion_unresolved")])
    tagged = attach_catalogue_identities(rows, ref)
    assert tagged.powertrain.item() == "gasoline"  # Native audit value remains untouched.
    assert not tagged.propulsion_resolved.item()
    with pytest.raises(ValueError, match="Classified propulsion conflicts"):
        attach_catalogue_identities(rows.assign(propulsion_resolved=True), ref)
    with pytest.raises(ValueError, match="Classified propulsion conflicts"):
        attach_catalogue_identities(pd.DataFrame([native(powertrain="hev")]), reference())


def test_untracked_fuel_modes_do_not_require_invented_canonical_powertrain_facts():
    rows = pd.DataFrame([native(powertrain="ethanol", included=False)])
    tagged = attach_catalogue_identities(rows, reference())
    assert tagged.powertrain.item() == "ethanol" and not tagged.included.item()


def test_catalogue_absence_and_model_nonexistence_are_distinct():
    ref = reference(facts=[
        canonical(model_year_from=2000, model_year_to=2020, powertrain="unknown",
                  evidence_status="model_not_produced", nrcan_vehicle_class="", notes="OEM first MY2021"),
        canonical(model_year_from=2022, model_year_to=2022, powertrain="unknown", evidence_status="reviewed_identity"),
        canonical(),
    ], links=[link(source="ontario_ministry_transport_vehicle_population", source_make="FIX", source_model="F",
                   model_year_from=2022, model_year_to=2022, vehicle_scope="ldv", canonical_description="Family")])
    observed = pd.DataFrame([{"MAKE": "FIX", "MODEL": "F", "MY": 2022}])
    assert resolve_source_labels(observed, ref, source="ontario_ministry_transport_vehicle_population",
        make_column="MAKE", model_column="MODEL", year_column="MY").vehicle_id.item() == "FIXTURE/FAMILY"
    assert ref.canonical.evidence_status.eq("model_not_produced").sum() == 1
    with pytest.raises(ValueError, match="nonexistence conflicts"):
        reference(facts=[canonical(), canonical(powertrain="unknown", evidence_status="model_not_produced")])


def test_link_cannot_imply_out_of_scope_canonical_model_year():
    with pytest.raises(ValueError, match="year/class applicability"):
        reference(links=[link(model_year_from=2020, model_year_to=2020)])


def test_link_cannot_bridge_a_gap_in_canonical_year_applicability():
    with pytest.raises(ValueError, match="year/class applicability"):
        reference(facts=[canonical(model_year_from=2020, model_year_to=2020), canonical()],
            links=[link(model_year_from=2020, model_year_to=2023)])


def test_wildcard_and_conditional_links_cannot_disagree_about_identity():
    with pytest.raises(ValueError, match="conflicting wildcard"):
        reference(facts=[canonical(), canonical(vehicle_id="FIXTURE/SECOND", canonical_model="Second")], links=[
            link(), link(observed_powertrain="bev", vehicle_id="FIXTURE/SECOND")])


def test_unresolved_source_links_remain_nullable_and_retain_explanation():
    ref = reference(links=[link(source="ontario_ministry_transport_vehicle_population", vehicle_id=None,
        vehicle_scope="identity_unresolved", link_status="unresolved_identity", source_notes="Unsupported modern identity")])
    rows = pd.DataFrame([native()])
    resolved = resolve_source_labels(rows, ref, source="ontario_ministry_transport_vehicle_population",
        make_column="Make", model_column="Model", year_column="year")
    assert resolved.vehicle_id.isna().all()
    assert resolved.identity_exclusion_reason.item() == "Unsupported modern identity"
    assert resolved["count"].sum() == 5


def test_missing_observation_interval_is_explained_without_inventing_a_link():
    resolved = resolve_source_labels(pd.DataFrame([native(year=2024)]), reference(),
        source="nrcan_fuel_consumption_ratings", make_column="Make", model_column="Model", year_column="year")
    assert resolved.vehicle_id.isna().all()
    assert resolved.reference_link_status.item() == "no_applicable_source_link"


def test_propulsion_and_class_conditions_choose_one_source_identity_without_fanout():
    ref = reference(facts=[canonical(powertrain="bev"), canonical(vehicle_id="FIXTURE/SECOND",
        canonical_model="Second", powertrain="phev")], links=[
        link(source="transport_canada_izev", source_model="Shared", observed_powertrain="bev", source_vehicle_class=""),
        link(source="transport_canada_izev", source_model="Shared", observed_powertrain="phev",
             source_vehicle_class="", vehicle_id="FIXTURE/SECOND"),
    ])
    rows = pd.DataFrame([native(Model="Shared", powertrain="bev"), native(Model="Shared", powertrain="phev")])
    resolved = resolve_source_labels(rows, ref, source="transport_canada_izev",
        make_column="Make", model_column="Model", year_column="year", powertrain_column="powertrain")
    assert resolved.vehicle_id.tolist() == ["FIXTURE/FAMILY", "FIXTURE/SECOND"]
    assert len(resolved) == len(rows) and resolved["count"].sum() == rows["count"].sum()


def test_mto_link_does_not_establish_observed_powertrain():
    with pytest.raises(ValueError, match="cannot establish observed propulsion"):
        reference(links=[link(source="ontario_ministry_transport_vehicle_population", observed_powertrain="hev")])


def test_foreign_keys_conflicting_names_and_overlapping_links_are_rejected():
    with pytest.raises(ValueError, match="foreign key"):
        reference(links=[link(vehicle_id="UNKNOWN/FAMILY")])
    with pytest.raises(ValueError, match="conflicting make/model"):
        reference(facts=[canonical(), canonical(canonical_model="Other")])
    with pytest.raises(ValueError, match="overlapping"):
        reference(links=[link(), link()])
    with pytest.raises(ValueError, match="overlapping"):
        reference(facts=[canonical(), canonical()])
    with pytest.raises(ValueError, match="conflicting reviewed family boundaries"):
        reference(facts=[canonical(), canonical(model_year_from=2024, model_year_to=2024, family_boundary=True)])


def test_generic_provincial_sources_can_reuse_identity_without_copying_canonical_facts():
    ref = reference(links=[link(source=source, source_model="Local code", source_vehicle_class="")
                          for source in ["icbc_vehicle_population", "saaq_vehicle_population"]])
    for source in ["icbc_vehicle_population", "saaq_vehicle_population"]:
        resolved = resolve_source_labels(pd.DataFrame([native(Model="Local code")]), ref, source=source,
            make_column="Make", model_column="Model", year_column="year")
        assert resolved.vehicle_id.item() == "FIXTURE/FAMILY"
    assert len(ref.canonical) == 1


def test_mto_adapter_preserves_scope_class_notes_and_supplies_no_population_counts():
    rules = yaml.safe_load(open("config/parameters/rules.yaml", encoding="utf-8"))
    road = rules["parameterization"]["road_aggregation"]
    classes = rules["fetching"]["nrcan_fuel_consumption_ratings"]["vehicle_class_harmonization"]
    ref = reference(links=[link(source="ontario_ministry_transport_vehicle_population", source_make="FIX", source_model="F",
        vehicle_scope="ldv", canonical_description="Family", source_notes="Accepted identity",
        wards_notes="Wards CY2014 family corroboration")])
    mapped = reference_mto_mapping(ref, road, classes)
    assert list(mapped) == road["mapping_columns"]
    assert mapped.canonical_model.item() == "Family"
    assert mapped.nrcan_vehicle_class.item() == "Compact"
    assert mapped.nrcan_ceud_class.item() == "Car"
    assert "Wards CY2014" in mapped.review_notes.item()
    assert "count" not in mapped


def test_ambiguous_mto_class_is_not_inferred_from_known_canonical_class():
    rules = yaml.safe_load(open("config/parameters/rules.yaml", encoding="utf-8"))
    ref = reference(links=[link(source="ontario_ministry_transport_vehicle_population", source_make="FIX", source_model="F",
        vehicle_scope="ldv_class_unresolved", source_vehicle_class="", canonical_description="Family")])
    frame = reference_mto_mapping(ref, rules["parameterization"]["road_aggregation"],
        rules["fetching"]["nrcan_fuel_consumption_ratings"]["vehicle_class_harmonization"])
    assert frame.vehicle_scope.item() == "ldv_class_unresolved"
    assert frame[["nrcan_vehicle_class", "nlr_atb_class", "nrcan_ceud_class"]].eq("").all().all()


def test_loader_reads_fixed_tables_without_rewriting_them(tmp_path, monkeypatch):
    import utils

    ref = reference()
    canonical_path, links_path = tmp_path / "vehicle_reference.csv", tmp_path / "vehicle_source_links.csv"
    ref.canonical.to_csv(canonical_path, index=False)
    ref.links.to_csv(links_path, index=False)
    original = {path: sha256(path.read_bytes()).hexdigest() for path in (canonical_path, links_path)}
    monkeypatch.setattr(utils, "load_harmonization_rules",
        lambda bundle, name: {"canonical_file": canonical_path.name, "source_links_file": links_path.name})
    monkeypatch.setattr(utils, "resolve_parameter_path", lambda bundle, name: tmp_path / name)
    loaded = load_vehicle_reference(object())
    assert loaded.paths == (canonical_path, links_path)
    assert {path: sha256(path.read_bytes()).hexdigest() for path in original} == original
    validate_vehicle_reference(loaded)
    with pytest.raises(ValueError, match="authoritative schema"):
        load_vehicle_reference_frames(ref.canonical.assign(median_range=100), ref.links)


def test_revalidation_rejects_mutated_configuration():
    ref = reference()
    invalid = ref.links.copy()
    invalid.loc[0, "vehicle_id"] = "UNKNOWN/FAMILY"
    with pytest.raises(ValueError, match="foreign key"):
        validate_vehicle_reference(replace(ref, links=invalid))
    assert isinstance(ref, VehicleReference)
