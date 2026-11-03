"""Presentation slices preserve captured cohort support, units and stock weights."""

from copy import deepcopy
from pathlib import Path
import runpy
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from validation import ldv_efficiency_diagnostic as diagnostic


def trajectory_helper(recorded):
    import altair as alt

    notebook = Path(__file__).resolve().parents[1] / "docs/insights/ldv_efficiency_diagnosis.py"
    cell = runpy.run_path(str(notebook))["trajectory_helpers"]
    _, definitions = cell.run(alt=alt, pd=pd, diagnostic=recorded,
        nrcan_anchor_endpoints=diagnostic.nrcan_anchor_endpoints,
        composition_view={"bridge_years": [2010, 2040], "class_order": {"Cars": ["Compact"]}})
    return definitions["trajectory_chart"]


@pytest.mark.parametrize("metric", ["consumption", "service_efficiency"])
def test_bridge_reference_remains_visible_without_duplicate_anchor_annotations(metric):
    frame = pd.DataFrame([
        {"vehicle_group": "Cars", "atb_class": diagnostic.COHORT, "powertrain": powertrain,
         "series": series, "stage": "Indexed bridge", "metric": metric,
         "year": year, "value": value, "units": "MJ/vkm" if metric == "consumption" else "bn passenger-km/PJ",
         "sample_kind": "base_year_anchor" if year == 2025 else "period",
         "marker_source": "NRCan-cohort projection anchor" if series == "Production method" else "Catalogue median"}
        for powertrain in ["bev_150_miles", "bev_300_miles"]
        for series, value in [("Production method", .65), ("Catalogue median", .75)]
        for year in [2025, 2030]
    ])
    original = frame.copy(deep=True)
    recorded = SimpleNamespace(summary={"base_year": 2023, "atb_reference_year": 2025})
    chart = trajectory_helper(recorded)(frame, "Production periods", width=900).to_dict(validate=True)
    rows = pd.DataFrame(chart["data"]["values"])
    labelled = rows.loc[rows.base_label.ne("")]

    assert chart["width"] == 900
    assert len(rows.loc[rows.series.eq("Catalogue median")]) == 4
    assert len(labelled) == 2
    assert set(labelled.series) == {"Production method"}
    assert set(labelled.year) == {2025}
    pd.testing.assert_frame_equal(frame, original)


def test_bridge_nrcan_label_is_compact_and_preserves_both_values():
    frame = pd.DataFrame([
        {"vehicle_group": "Cars", "atb_class": diagnostic.COHORT, "powertrain": "bev_300_miles",
         "series": series, "stage": "Harmonised before indexing", "year": year, "value": value, "units": "MJ/vkm"}
        for series, year, value in [("NRCan size-class aggregate", 2023, .706), ("ATB absolute total energy", 2025, .543)]
    ])
    anchors = pd.DataFrame([{"vehicle_group": "Cars", "atb_class": diagnostic.COHORT,
        "anchor_powertrain": "bev_300_miles", "source_treatments": "NRCan_observed_class_weighted",
        "source_year_start": 2021, "source_year_end": 2023, "source_years": "2021 | 2022 | 2023",
        "anchor_consumption_mj_per_vkm": .682}])
    recorded = SimpleNamespace(summary={"base_year": 2023, "atb_reference_year": 2025}, frames={"projection_anchors": anchors})
    chart = trajectory_helper(recorded)(frame, "Harmonised source energy", width=900).to_dict(validate=True)
    rows = pd.DataFrame(chart["data"]["values"])

    assert rows.loc[rows.series.eq("NRCan size-class aggregate"), "base_label"].item() == "BEV 300 · 0.706\nCohort anchor: 0.682"
    assert rows.loc[rows.series.eq("ATB absolute total energy"), "base_label"].item() == "1. BEV 300 · 0.543"


def test_display_years_trim_only_unsupported_ends_and_keep_interior_gaps():
    frame = pd.DataFrame({"year": ["2015", 2023, None, "not a year", np.inf, -np.inf]})
    original = frame.copy(deep=True)
    years = [2010, 2015, 2016, 2020, 2023, 2025]

    assert diagnostic.supported_display_years(frame, years) == [2015, 2016, 2020, 2023]
    pd.testing.assert_frame_equal(frame, original)
    assert years == [2010, 2015, 2016, 2020, 2023, 2025]


@pytest.mark.parametrize("observed_years", [[], [None, "unknown", np.inf, -np.inf]])
def test_display_years_without_finite_support_are_empty(observed_years):
    frame = pd.DataFrame({"year": observed_years})

    assert diagnostic.supported_display_years(frame, [2015, 2020, 2025]) == []


def test_display_years_preserve_requested_years_between_non_displayed_endpoints():
    frame = pd.DataFrame({"year": [2014, 2026]})

    assert diagnostic.supported_display_years(frame, [2015, 2020, 2025]) == [2015, 2020, 2025]


@pytest.fixture
def class_weights():
    rows = [
        (2025, "Car", "Large", .45),
        (2025, "Car", "Compact", .30),
        (2025, "Car", "Mid-size", .20),
        (2025, "Car", "Subcompact", .05),
        (2024, "Car", "Subcompact", .99),
        (2027, "Light Truck", "Pickup truck: Standard", .55),
        (2027, "Light Truck", "Sport utility vehicle: Small", .35),
        (2027, "Light Truck", "Minivan", .10),
        (2023, "Light Truck", "Minivan", 1.),
    ]
    frame = pd.DataFrame([
        {"report_year": year, "weight_basis": "all_vintages", "nrcan_ceud_class": group,
         "nrcan_vehicle_class": size, "aggregation_weight": weight, "vehicle_count": int(weight * 1000)}
        for year, group, size, weight in rows
    ])
    annual = frame.loc[frame.nrcan_ceud_class.eq("Car")].iloc[[0]].assign(
        report_year=2030, weight_basis="model_year", nrcan_vehicle_class="Annual-only class", aggregation_weight=1.)
    return pd.concat([frame, annual], ignore_index=True)


@pytest.mark.parametrize(
    ("vehicle_group", "top_count", "expected", "expected_year", "expected_weights"),
    [
        ("Cars", 3, ["Compact", "Mid-size", "Large"], 2025, [.30, .20, .45]),
        ("Light trucks", 2, ["Sport utility vehicle: Small", "Pickup truck: Standard"], 2027, [.35, .55]),
    ],
)
def test_class_columns_select_latest_pooled_stock_before_nominal_display_order(
    class_weights, vehicle_group, top_count, expected, expected_year, expected_weights
):
    original = class_weights.copy(deep=True)
    recorded = SimpleNamespace(inputs={"rating_class_weights": class_weights})
    view = {"top_size_classes": top_count, "class_order": {
        "Cars": ["Subcompact", "Compact", "Mid-size", "Large"],
        "Light trucks": ["Minivan", "Sport utility vehicle: Small", "Pickup truck: Standard"],
    }}
    original_view = deepcopy(view)

    result = diagnostic.composition_class_columns(recorded, vehicle_group, view)

    assert result.nrcan_vehicle_class.tolist() == expected
    assert result.aggregation_weight.tolist() == expected_weights
    assert set(result.report_year) == {expected_year}
    assert set(result.weight_basis) == {"all_vintages"}
    assert "vehicle_count" in result
    result.loc[:, "aggregation_weight"] = -1.
    pd.testing.assert_frame_equal(class_weights, original)
    assert view == original_view


def test_class_columns_reject_duplicate_latest_snapshot_identities(class_weights):
    duplicate = class_weights.loc[class_weights.report_year.eq(2025)].iloc[[0]]
    recorded = SimpleNamespace(inputs={"rating_class_weights": pd.concat([class_weights, duplicate], ignore_index=True)})
    view = {"top_size_classes": 2, "class_order": {"Cars": ["Compact", "Large"]}}

    with pytest.raises(ValueError, match="Duplicate captured MTO class shares"):
        diagnostic.composition_class_columns(recorded, "Cars", view)


def test_class_columns_ignore_duplicates_outside_the_chosen_snapshot(class_weights):
    previous = class_weights.loc[class_weights.report_year.eq(2024)]
    recorded = SimpleNamespace(inputs={"rating_class_weights": pd.concat([class_weights, previous], ignore_index=True)})
    view = {"top_size_classes": 1, "class_order": {"Cars": ["Large"]}}

    result = diagnostic.composition_class_columns(recorded, "Cars", view)

    assert result.nrcan_vehicle_class.tolist() == ["Large"]
    assert result.aggregation_weight.item() == .45


def test_all_class_columns_retain_historical_body_without_inventing_stock_weight(class_weights):
    recorded = SimpleNamespace(inputs={"rating_class_weights": class_weights}, frames={"rating_distribution": pd.DataFrame({
        "vehicle_group": ["Cars", "Cars", "Light trucks"],
        "rating_class": ["Compact", "Historical body", "Truck-only body"]})})
    view = {"top_size_classes": None, "class_order": {"Cars": ["Subcompact", "Compact", "Mid-size", "Large"]}}

    result = diagnostic.composition_class_columns(recorded, "Cars", view)

    assert result.nrcan_vehicle_class.tolist() == ["Subcompact", "Compact", "Mid-size", "Large", "Historical body"]
    assert result.aggregation_weight.sum() == 1.
    assert pd.isna(result.loc[result.nrcan_vehicle_class.eq("Historical body"), "aggregation_weight"]).all()


@pytest.mark.parametrize("values", [[4.1, 8.8], [0.578, .882], [43., 256.], [2.], [2.3, np.nan, np.inf]])
def test_practical_axis_covers_support_with_four_regular_readable_ticks(values):
    domain, ticks = diagnostic.practical_axis(pd.Series(values))
    finite = [value for value in values if np.isfinite(value)]

    assert len(ticks) == 4
    assert np.allclose(np.diff(ticks), ticks[1] - ticks[0])
    assert domain[0] <= min(finite) <= max(finite) <= domain[-1]
    assert ticks[0] <= min(finite) <= max(finite) <= ticks[-1]
    assert domain[0] >= 0


@pytest.mark.parametrize("values, expected", [([16.2, 47.3], [12., 24., 36., 48.]), ([16.7, 44.6], [15., 25., 35., 45.])])
def test_practical_axis_avoids_unused_zero_origin_for_bev_native_consumption(values, expected):
    domain, ticks = diagnostic.practical_axis(pd.Series(values))

    assert ticks == expected
    assert domain[0] > 0


def test_nrcan_annotation_distinguishes_source_endpoint_from_cohort_anchor_and_excludes_cd_and_analogue():
    frame = pd.DataFrame([
        {"vehicle_group": "Cars", "atb_class": diagnostic.COHORT, "powertrain": powertrain,
         "series": series, "year": year, "value": value}
        for powertrain, series, year, value in [
            ("phev_35_miles", "NRCan size-class aggregate", 2021, 1.2),
            ("phev_35_miles", "NRCan size-class aggregate", 2023, 1.4),
            ("phev_35_miles", "NRCan size-class aggregate", 2024, 1.5),
            ("phev_35_miles", "NRCan CD component-energy aggregate", 2023, .5),
            ("phev_50_miles", "NRCan size-class aggregate", 2023, 1.1),
            ("cng", "NRCan size-class aggregate", 2023, 2.2),
        ]])
    anchors = pd.DataFrame([
        {"vehicle_group": "Cars", "atb_class": diagnostic.COHORT, "anchor_powertrain": powertrain,
         "source_year_start": 2021, "source_year_end": 2023, "source_years": "2021 | 2023",
         "anchor_consumption_mj_per_vkm": 1.3, "source_treatments": treatment}
        for powertrain, treatment in [("phev_35_miles", "NRCan_observed_class_weighted"),
                                      ("cng", "NRCan_missing_year_approved_analogue_index")]])
    original = frame.copy(deep=True)

    result = diagnostic.nrcan_anchor_endpoints(SimpleNamespace(frames={"projection_anchors": anchors}), frame)

    selected = result.loc[result.nrcan_anchor_window.ne("")]
    assert len(selected) == 1
    assert selected.year.item() == 2023
    assert selected.value.item() == 1.4
    assert selected.nrcan_anchor_consumption.item() == 1.3
    assert selected.nrcan_anchor_window.item() == "2021–2023"
    pd.testing.assert_frame_equal(frame, original)


@pytest.fixture
def comparison():
    rows = [
        ("Cars", "Compact", 2020, "phev_35_miles", "phev", 2., 1.5),
        ("Cars", "Compact", 2020, "phev_50_miles", "phev", 4., 5.),
        ("Cars", "Mid-size", 2021, "phev_35_miles", "phev", 3., 3.),
        ("Cars", "Compact", 2022, "phev_35_miles", "phev", np.nan, 1.2),
        ("Cars", "Compact", 2023, "phev_35_miles", "phev", 2., np.nan),
        ("Cars", "Compact", 2019, "phev_35_miles", "phev", 2., 1.),
        ("Cars", "Compact", 2024, "phev_35_miles", "phev", 2., 1.),
        ("Cars", "Unmapped class", 2021, "phev_35_miles", "phev", 2., 1.),
        ("Cars", "Compact", 2020, "bev_300_miles", "bev", 5., 1.),
        ("Light trucks", "Compact", 2020, "phev_35_miles", "phev", 100., 1.),
    ]
    return pd.DataFrame([
        {"vehicle_group": group, "rating_class": size, "year": year, "powertrain": powertrain,
         "powertrain_family": family, "catalogue_mj_per_vkm": catalogue, "production_mj_per_vkm": production,
         # Native CD rates deliberately imply the opposite sign to total PHEV energy.
         "catalogue_native_consumption": 1., "production_native_consumption": 5., "native_unit": "Le/100 km (CD)"}
        for group, size, year, powertrain, family, catalogue, production in rows
    ])


def recorded_comparison(frame):
    mapping = pd.DataFrame([
        {"rating_class": "Compact", "atb_class": "Compact"},
        {"rating_class": "Compact", "atb_class": "Compact"},
        {"rating_class": "Mid-size", "atb_class": "Midsize"},
    ])
    return SimpleNamespace(frames={"composition_comparison": frame, "rating_summary": mapping})


def test_heatmap_filters_group_family_class_and_inclusive_years_without_pooling_ranges(comparison):
    recorded = recorded_comparison(comparison)
    original = {name: frame.copy(deep=True) for name, frame in recorded.frames.items()}

    result = diagnostic.composition_heatmap_data(recorded, "Cars", "phev", atb_class="Compact", years=(2020, 2023))

    assert set(zip(result.year, result.powertrain, strict=True)) == {
        (2020, "phev_35_miles"), (2020, "phev_50_miles"), (2022, "phev_35_miles"), (2023, "phev_35_miles")}
    assert set(result.vehicle_group) == {"Cars"}
    assert set(result.rating_class) == {"Compact"}
    assert set(result.powertrain_family) == {"phev"}
    result.loc[:, "production_mj_per_vkm"] = -1.
    for name, frame in original.items():
        pd.testing.assert_frame_equal(recorded.frames[name], frame)


def test_heatmap_default_scope_retains_fine_classes_without_requiring_atb_mapping(comparison):
    recorded = SimpleNamespace(frames={"composition_comparison": comparison})

    result = diagnostic.composition_heatmap_data(recorded, "Cars", "phev")

    assert set(result.rating_class) == {"Compact", "Mid-size", "Unmapped class"}
    assert set(result.year) == {2019, 2020, 2021, 2022, 2023, 2024}
    assert set(result.powertrain) == {"phev_35_miles", "phev_50_miles"}


def test_heatmap_difference_is_catalogue_minus_production_on_total_mj_basis(comparison):
    result = diagnostic.composition_heatmap_data(recorded_comparison(comparison), "Cars", "phev", years=(2020, 2021))
    compact = result.loc[result.rating_class.eq("Compact")].set_index("powertrain")

    assert compact.loc["phev_35_miles", "delta_mj_per_vkm"] == .5
    assert compact.loc["phev_35_miles", "delta_fraction"] == .25
    assert compact.loc["phev_50_miles", "delta_mj_per_vkm"] == -1.
    assert compact.loc["phev_50_miles", "delta_fraction"] == -.25
    equal = result.loc[result.rating_class.eq("Mid-size")]
    assert equal.delta_mj_per_vkm.item() == 0.
    assert equal.delta_fraction.item() == 0.
    assert compact.loc["phev_35_miles", "native_unit"] == "Le/100 km (CD)"


@pytest.mark.parametrize(
    ("catalogue", "production", "expected_delta", "expected_fraction"),
    [(np.nan, 0., np.nan, np.nan), (0., 0., 0., np.nan), (0., 1., -1., np.nan),
     (2., np.nan, np.nan, np.nan), (2., 2., 0., 0.), (2., 0., 2., 1.)],
)
def test_heatmap_keeps_missing_values_distinct_from_zero(comparison, catalogue, production, expected_delta, expected_fraction):
    frame = comparison.iloc[[0]].assign(catalogue_mj_per_vkm=catalogue, production_mj_per_vkm=production)

    result = diagnostic.composition_heatmap_data(recorded_comparison(frame), "Cars", "phev").iloc[0]

    if np.isnan(expected_delta):
        assert pd.isna(result.delta_mj_per_vkm)
    else:
        assert result.delta_mj_per_vkm == expected_delta
    if np.isnan(expected_fraction):
        assert pd.isna(result.delta_fraction)
    else:
        assert result.delta_fraction == expected_fraction


def test_heatmap_rejects_duplicate_cohort_identities_in_selected_scope(comparison):
    frame = pd.concat([comparison, comparison.iloc[[0]].assign(production_mj_per_vkm=1.7)], ignore_index=True)

    with pytest.raises(ValueError, match="Duplicate class/year/powertrain consumption comparison"):
        diagnostic.composition_heatmap_data(recorded_comparison(frame), "Cars", "phev")


@pytest.mark.parametrize("excluded_scope", ["group", "family", "year", "class"])
def test_heatmap_duplicate_check_is_limited_to_displayed_scope(comparison, excluded_scope):
    duplicate = comparison.iloc[[0]].copy()
    if excluded_scope == "group":
        duplicate["vehicle_group"] = "Light trucks"
    elif excluded_scope == "family":
        duplicate["powertrain_family"] = "bev"
    elif excluded_scope == "year":
        duplicate["year"] = 2019
    else:
        duplicate["rating_class"] = "Mid-size"
    frame = pd.concat([comparison, duplicate, duplicate], ignore_index=True)

    result = diagnostic.composition_heatmap_data(recorded_comparison(frame), "Cars", "phev", atb_class="Compact", years=(2020, 2023))

    assert len(result) == 4
    assert not result.duplicated(["year", "rating_class", "powertrain"]).any()
