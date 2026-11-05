"""CEUD observations, explicit tail assumptions, and isolated parameter evidence."""

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from parameterization import ceud_inputs
from parameterization.ceud_inputs import extend_ceud_to_base_year
from utils import file_sha256


def source_frame(*, years=(2021, 2022, 2023), values=(10.0, 11.0, 12.0)):
    return pd.DataFrame({
        "source_id": "nrcan_ceud_transport_provincial", "region": "ON",
        "table_id": 21, "raw_series": "Stock (thousands)|Cars",
        "unit": "thousands", "year": years, "value": values,
        "cached_file": "registered_2023.xls",
    })


def test_constant_tail_preserves_native_rows_and_source_identity():
    source = source_frame()
    untouched = source.copy(deep=True)
    result, audit = extend_ceud_to_base_year(source, base_year=2025, policy="constant")
    pd.testing.assert_frame_equal(source, untouched)
    pd.testing.assert_frame_equal(result.loc[:2, source.columns], source)
    assert result.year.tolist() == [2021, 2022, 2023, 2024, 2025]
    assert result.value.tolist() == [10.0, 11.0, 12.0, 12.0, 12.0]
    assert result.ceud_value_status.tolist() == ["observed"] * 3 + ["constant"] * 2
    assert result.ceud_observed_year.tolist() == [2021, 2022, 2023, 2023, 2023]
    assert set(result.ceud_source_endpoint_year) == {2023}
    assert set(result.cached_file) == {"registered_2023.xls"}
    assert audit.iloc[0].extended_rows == 2
    assert audit.iloc[0].source_endpoint_year == 2023


@pytest.mark.parametrize("year", [2021, 2022, 2023])
def test_in_range_keeps_full_source_history_and_no_extension(year):
    source = source_frame()
    result, audit = extend_ceud_to_base_year(source, base_year=year)
    pd.testing.assert_frame_equal(result[source.columns], source)
    assert result.year.max() == 2023
    assert audit.extended_rows.sum() == 0


def test_error_policy_requires_explicit_constant_for_first_unobserved_year():
    with pytest.raises(ValueError, match="2024 exceeds observed endpoint 2023"):
        extend_ceud_to_base_year(source_frame(), base_year=2024)


def test_table_specific_endpoints_are_not_source_edition_or_global_max():
    early = source_frame(years=(2020, 2021), values=(8.0, 9.0))
    late = source_frame().assign(table_id=22, raw_series="Other stock")
    result, audit = extend_ceud_to_base_year(
        pd.concat([early, late], ignore_index=True), base_year=2025, policy="constant",
    )
    assert result.loc[result.table_id.eq(21), "year"].tolist() == list(range(2020, 2026))
    assert result.loc[result.table_id.eq(22), "year"].tolist() == list(range(2021, 2026))
    assert audit.set_index("table_id").extended_rows.to_dict() == {21: 4, 22: 2}


def test_old_fuel_series_and_all_missing_series_are_explicitly_skipped():
    old = source_frame(years=(2021,), values=(3.0,)).assign(raw_series="Old fuel label")
    absent = source_frame(values=(np.nan, np.nan, np.nan)).assign(raw_series="Not applicable")
    source = pd.concat([source_frame(), old, absent], ignore_index=True)
    result, audit = extend_ceud_to_base_year(source, base_year=2025, policy="constant")
    assert len(result.loc[result.raw_series.eq("Old fuel label")]) == 1
    assert len(result.loc[result.raw_series.eq("Not applicable")]) == 3
    assert result.loc[result.raw_series.eq("Not applicable"), "value"].isna().all()
    assert audit.set_index("raw_series").status.to_dict() == {
        "Stock (thousands)|Cars": "extended",
        "Old fuel label": "skipped_not_observed_at_source_endpoint",
        "Not applicable": "skipped_no_source_values",
    }
    _, in_range = extend_ceud_to_base_year(source, base_year=2023)
    assert in_range.extended_rows.sum() == 0


def test_missing_interior_values_and_years_are_never_repaired():
    source = source_frame(years=(2020, 2021, 2023), values=(8.0, np.nan, 12.0))
    result, _ = extend_ceud_to_base_year(source, base_year=2025, policy="constant")
    assert 2022 not in set(result.year)
    assert result.loc[result.year.eq(2021), "value"].isna().all()
    assert result.loc[result.year.eq(2021), "ceud_value_status"].iloc[0] == "source_missing"
    with pytest.raises(ValueError, match="Missing required CEUD observations"):
        extend_ceud_to_base_year(
            source, base_year=2025, policy="constant",
            required_series=[(21, "Stock (thousands)|Cars")],
        )


def test_required_selector_missing_or_historically_discontinued_fails():
    with pytest.raises(ValueError, match="Missing required CEUD series"):
        extend_ceud_to_base_year(
            source_frame(), base_year=2023, required_series=[(21, "Sales")],
        )
    old = source_frame(years=(2021,), values=(3.0,)).assign(raw_series="Old fuel label")
    with pytest.raises(ValueError, match="Missing required CEUD observations"):
        extend_ceud_to_base_year(
            pd.concat([source_frame(), old], ignore_index=True),
            base_year=2025, policy="constant", required_series=[(21, "Old fuel label")],
        )


@pytest.mark.parametrize("value", [-1.0, np.inf, -np.inf])
def test_nonphysical_observation_is_rejected(value):
    with pytest.raises(ValueError, match="finite and nonnegative"):
        extend_ceud_to_base_year(
            source_frame(values=(10.0, 11.0, value)), base_year=2025, policy="constant",
        )


def test_zero_endpoint_is_observed_zero_not_missing():
    result, _ = extend_ceud_to_base_year(
        source_frame(values=(10.0, 0.0, 0.0)), base_year=2025, policy="constant",
    )
    assert result.loc[result.year.ge(2023), "value"].eq(0.0).all()


def test_duplicate_years_and_changed_units_fail():
    with pytest.raises(ValueError, match="duplicate observation years"):
        extend_ceud_to_base_year(
            source_frame(years=(2021, 2021, 2023)), base_year=2025, policy="constant",
        )
    source = source_frame()
    source.loc[1, "unit"] = "millions"
    with pytest.raises(ValueError, match="units change"):
        extend_ceud_to_base_year(source, base_year=2025, policy="constant")


def test_estimates_cannot_be_reloaded_as_native_observations():
    result, _ = extend_ceud_to_base_year(source_frame(), base_year=2025, policy="constant")
    with pytest.raises(ValueError, match="originate from native observations"):
        extend_ceud_to_base_year(result, base_year=2026, policy="constant")


@pytest.fixture
def isolated_ceud(tmp_path, monkeypatch):
    native = tmp_path / "native"
    native.mkdir()
    output = tmp_path / "parameter_evidence"
    source_frame().to_csv(native / "ceud_on.csv", index=False)
    source_frame().assign(
        source_id="nrcan_ceud_transport_national", region="national",
    ).to_csv(native / "ceud_national.csv", index=False)
    rules = {
        "nrcan_ceud": {"interim_subdir": "native", "region_output_template": "ceud_{region}.csv"},
        "ceud_inputs": {"cagr": {"history_start_year": 2013, "excluded_years": [2020, 2021], "zero_endpoint": "constant"},
            "audit_file": "audit.csv", "manifest_file": "manifest.json"},
    }
    monkeypatch.setattr(ceud_inputs, "load_harmonization_rules", lambda bundle, name: rules[name])
    monkeypatch.setattr(
        ceud_inputs, "resolve_input_path", lambda bundle, layer, directory, filename: native / filename,
    )
    monkeypatch.setattr(ceud_inputs, "resolve_artifact_path", lambda bundle, family: output)
    selection = SimpleNamespace(year=2023)
    bundle = SimpleNamespace(
        scenario_path=tmp_path / "scenario.yaml",
        scenario=SimpleNamespace(
            scenario=SimpleNamespace(name="ceud_test"), geography=SimpleNamespace(regions=["ON"]),
            periods=SimpleNamespace(base_year=2025),
            sources=SimpleNamespace(
                nrcan_ceud_beyond_observed="constant",
                selections={
                    "nrcan_ceud_transport_provincial": selection,
                    "nrcan_ceud_transport_national": selection,
                },
            ),
        ),
    )
    return bundle, native, output


def test_preparation_preserves_native_hashes_and_read_recomputes(isolated_ceud):
    bundle, native, output = isolated_ceud
    before = {p.name: file_sha256(p) for p in native.glob("*.csv")}
    manifest = ceud_inputs.prepare_ceud_inputs(bundle)
    assert manifest["extended_rows"] == 4
    assert manifest["observed_history_preserved"] is True
    assert manifest["native_source_files_modified"] is False
    assert {entry["selected_source_edition"] for entry in manifest["sources"]} == {2023}
    assert before == {p.name: file_sha256(p) for p in native.glob("*.csv")}
    persisted_hash = file_sha256(output / "manifest.json")
    ceud_inputs.prepare_ceud_inputs(bundle)
    assert file_sha256(output / "manifest.json") == persisted_hash
    (output / "ceud_on.csv").write_text("tampered persisted view", encoding="utf-8")
    result = ceud_inputs.read_ceud_input(bundle, "ON")
    assert result.loc[result.year.eq(2025), "value"].iloc[0] == 12.0


def test_earlier_base_year_clips_only_runtime_view(isolated_ceud):
    bundle, native, output = isolated_ceud
    bundle.scenario.periods.base_year = 2022
    manifest = ceud_inputs.prepare_ceud_inputs(bundle)
    assert manifest["extended_rows"] == 0
    assert pd.read_csv(output / "ceud_on.csv").year.max() == 2023
    assert pd.read_csv(native / "ceud_on.csv").year.max() == 2023
    assert ceud_inputs.read_ceud_input(bundle, "ON").year.max() == 2022


def test_failed_extension_publishes_no_evidence(isolated_ceud):
    bundle, _, output = isolated_ceud
    bundle.scenario.sources.nrcan_ceud_beyond_observed = "error"
    with pytest.raises(ValueError, match="exceeds observed endpoint"):
        ceud_inputs.prepare_ceud_inputs(bundle)
    assert not output.exists()


def test_extrapolated_cagr_excludes_pandemic_and_estimated_observations():
    from parameterization.ceud_inputs import historical_cagr
    source = source_frame(years=tuple(range(2017, 2024)), values=(100., 110., 121., 5., 8., 160., 176.))
    rules = {"history_start_year": 2013, "excluded_years": [2020, 2021], "zero_endpoint": "constant"}
    result, audit = extend_ceud_to_base_year(source, base_year=2025, policy="extrapolated", cagr_rules=rules)
    assert result.loc[result.year.eq(2025), "value"].iloc[0] == pytest.approx(176 * 1.1 ** 2)
    assert audit.iloc[0].growth_intervals == "2018|2019|2023"
    rate, years = historical_cagr(result, history_start_year=2013, excluded_years=[2020, 2021])
    assert rate == pytest.approx(.1)
    assert years == [2018, 2019, 2023]
    pd.testing.assert_frame_equal(result.loc[result.year.le(2023), source.columns].reset_index(drop=True), source)


def test_extrapolated_zero_and_unsupported_positive_history():
    rules = {"history_start_year": 2013, "excluded_years": [2020, 2021], "zero_endpoint": "constant"}
    result, _ = extend_ceud_to_base_year(source_frame(values=(0, 0, 0)), base_year=2025, policy="extrapolated", cagr_rules=rules)
    assert result.value.eq(0).all()
    with pytest.raises(ValueError, match="consecutive"):
        extend_ceud_to_base_year(source_frame(years=(2023,), values=(1,)), base_year=2025, policy="extrapolated", cagr_rules=rules)
