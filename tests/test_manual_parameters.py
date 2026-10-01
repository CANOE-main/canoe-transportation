from collections import defaultdict
from pathlib import Path

import pandas as pd
import pytest

from parameterization.manual_parameters import (
    ManualParameterError,
    resolve_manual_parameters,
    validate_manual_registry,
    validate_technology_selectors,
)
from utils import (
    load_config_bundle,
    load_harmonization_rules,
    resolve_input_path,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/legacy_reproduction.yaml"
SOURCE_COLUMN = "source -> data_source"


def test_every_manual_csv_and_cited_row_is_registered() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    manual_dir = REPO_ROOT / bundle.paths.inputs.manual
    actual_files = {path.name for path in manual_dir.glob("*.csv")}
    registered_files: set[str] = set()
    covered_rows: dict[str, set[int]] = defaultdict(set)
    frames: dict[str, pd.DataFrame] = {}

    for source in bundle.sources.sources.values():
        for component in source.components.values():
            adapter = component.adapter
            filename = adapter.get("manual_parameter_path")
            if filename is None:
                continue

            registered_files.add(filename)
            frame = frames.setdefault(filename, pd.read_csv(manual_dir / filename))
            assert list(frame.columns) == adapter["expected_columns"]
            assert not frame.duplicated(adapter["unique_key"]).any()

            selector = adapter["source_selector"]
            selected = frame.index[frame[SOURCE_COLUMN].eq(selector)]
            assert len(selected) == adapter["expected_rows"]
            assert not covered_rows[filename].intersection(selected)
            covered_rows[filename].update(selected)

    internal_specs = load_harmonization_rules(bundle, "manual_parameters")[
        "internal_assumption_files"
    ]
    assert registered_files | set(internal_specs) == actual_files
    for filename, spec in internal_specs.items():
        frame = pd.read_csv(manual_dir / filename)
        assert list(frame.columns) == spec["expected_columns"]
        assert len(frame) == spec["expected_rows"]
        assert not frame.duplicated(spec["unique_key"]).any()

    for filename, frame in frames.items():
        cited_rows = set(frame.index[frame[SOURCE_COLUMN].fillna("").str.strip().ne("")])
        assert covered_rows[filename] == cited_rows
        uncited = frame.loc[~frame.index.isin(cited_rows)]
        assert uncited["notes"].fillna("").str.strip().ne("").all()


def test_current_compact_manual_selectors_resolve_to_technology_categories() -> None:
    bundle = load_config_bundle(SCENARIO, repo_root=REPO_ROOT)
    rules = load_harmonization_rules(bundle, "manual_parameters")
    registry, frames = validate_manual_registry(
        bundle,
        source_column=rules["source_column"],
        notes_column=rules["notes_column"],
    )
    technology = validate_technology_selectors(
        pd.read_csv(
            resolve_input_path(
                bundle,
                "template",
                rules["technology_template_file"],
            ),
            dtype=str,
            keep_default_na=False,
        ),
        rules=rules,
    )

    resolution, reconciliation, findings = resolve_manual_parameters(
        frames,
        technology,
        rules=rules,
    )

    assert registry["manual_file"].nunique() == 7
    assert len(registry) == 19
    assert len(resolution) == 34
    assert resolution["tech"].nunique() == 34
    assert not resolution.duplicated(
        ["manual_file", "manual_row", "tech", "selector_year"]
    ).any()
    charger_rows = reconciliation.loc[
        reconciliation["manual_file"].eq("charger_parameters.csv")
    ]
    assert len(charger_rows) == 42
    assert set(charger_rows["resolution_status"]) == {"behavior_specific_owner"}

    lifetime = resolution.loc[
        resolution["manual_file"].eq("lifetime_process.csv")
    ]
    assert lifetime.groupby("technology_class")["tech"].nunique().to_dict() == {
        "charger": 4,
        "freight_air": 3,
        "freight_marine": 5,
        "freight_rail": 4,
        "h2_refuel": 2,
        "heavy_trucks": 7,
        "motorcycles": 3,
        "passenger_air": 3,
        "passenger_rail": 3,
    }

    assert findings.empty
    behavior = reconciliation.loc[
        reconciliation["manual_file"].isin(rules["behavior_specific_files"])
    ]
    assert set(behavior["resolution_status"]) == {"behavior_specific_owner"}
    wards = reconciliation.loc[
        reconciliation["manual_file"].eq("vehicle_class_market_shares.csv")
    ]
    assert len(wards) == 32
    assert set(wards["resolution_status"]) == {"not_technology_scoped"}


def test_remainder_cannot_overlap_all_selector() -> None:
    rules = load_harmonization_rules(
        load_config_bundle(SCENARIO, repo_root=REPO_ROOT),
        "manual_parameters",
    )
    frames = {
        "fixture.csv": pd.DataFrame(
            {
                "technology_class": ["passenger_rail", "passenger_rail"],
                "powertrain": ["all", "remainder"],
                "parameter": ["annual_improvement_rate"] * 2,
                "value": ["0.1", "0.2"],
            }
        )
    }
    technology = pd.DataFrame(
        {
            "tech": ["T_DSL", "T_H2"],
            "category": ["passenger_rail", "passenger_rail"],
            "sub_category": ["diesel", "h2"],
        }
    )

    with pytest.raises(ManualParameterError, match="remainder alongside all"):
        resolve_manual_parameters(frames, technology, rules=rules)


def test_unknown_technology_class_is_rejected() -> None:
    rules = load_harmonization_rules(
        load_config_bundle(SCENARIO, repo_root=REPO_ROOT),
        "manual_parameters",
    )
    frames = {
        "fixture.csv": pd.DataFrame(
            {
                "technology_class": ["not_a_category"],
                "powertrain": ["all"],
                "parameter": ["lifetime"],
                "value": ["10"],
            }
        )
    }
    technology = pd.DataFrame(
        {
            "tech": ["T_DSL"],
            "category": ["passenger_rail"],
            "sub_category": ["diesel"],
        }
    )

    with pytest.raises(
        ManualParameterError,
        match="absent from technology.category",
    ):
        resolve_manual_parameters(frames, technology, rules=rules)
