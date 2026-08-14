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

    assert registered_files == actual_files

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

    assert registry["manual_file"].nunique() == 6
    assert len(registry) == 12
    assert len(resolution) == 103
    assert resolution["tech"].nunique() == 35
    assert not resolution.duplicated(
        ["manual_file", "parameter", "tech", "selector_year"]
    ).any()

    lifetime = resolution.loc[
        resolution["manual_file"].eq("lifetime_process.csv")
    ]
    assert lifetime.groupby("technology_class")["tech"].nunique().to_dict() == {
        "charger": 4,
        "freight_air": 3,
        "freight_marine": 5,
        "freight_rail": 4,
        "h2_refuel": 3,
        "heavy_trucks": 7,
        "motorcycles": 3,
        "passenger_air": 3,
        "passenger_rail": 3,
    }

    jet_fuel = resolution.loc[
        resolution["manual_file"].eq("cost_invest_multipliers.csv")
        & resolution["technology_class"].eq("passenger_air")
        & resolution["powertrain"].eq("jet_fuel")
    ]
    assert set(jet_fuel["technology_sub_category"]) == {"jet fuel", "jet_fuel"}
    marine_mdo = resolution.loc[
        resolution["manual_file"].eq("cost_invest_multipliers.csv")
        & resolution["technology_class"].eq("freight_marine")
        & resolution["powertrain"].eq("mdo")
    ]
    assert set(marine_mdo["technology_sub_category"]) == {
        "marine diesel oil",
        "mdo",
    }

    h2_2035 = resolution.loc[
        resolution["manual_file"].eq("cost_invest_multipliers.csv")
        & resolution["technology_class"].eq("freight_rail")
        & resolution["powertrain"].eq("h2_2035")
    ]
    assert h2_2035["selector_year"].tolist() == [2035]
    assert h2_2035["technology_sub_category"].tolist() == ["h2"]

    passenger_remainder = resolution.loc[
        resolution["manual_file"].eq("efficiency_multipliers.csv")
        & resolution["technology_class"].eq("passenger_rail")
        & resolution["powertrain"].eq("remainder")
    ]
    assert set(passenger_remainder["technology_sub_category"]) == {"diesel"}
    freight_remainder = resolution.loc[
        resolution["manual_file"].eq("efficiency_multipliers.csv")
        & resolution["technology_class"].eq("freight_rail")
        & resolution["powertrain"].eq("remainder")
    ]
    assert set(freight_remainder["technology_sub_category"]) == {
        "diesel",
        "lng",
    }

    variable_all = resolution.loc[
        resolution["manual_file"].eq("cost_variable_multipliers.csv")
    ]
    assert variable_all.groupby("technology_class")["tech"].nunique().to_dict() == {
        "freight_marine": 5,
        "freight_rail": 4,
        "passenger_rail": 3,
    }

    assert set(
        zip(
            findings["technology_class"],
            findings["powertrain"],
            strict=True,
        )
    ) == {
        ("passenger_rail", "lng_2035"),
        ("passenger_rail", "lng_2050"),
        ("freight_marine", "h2_2035"),
        ("freight_marine", "h2_2050"),
        ("passenger_rail", "lng"),
        ("passenger_rail", "electric"),
        ("freight_rail", "electric"),
        ("freight_marine", "h2"),
    }
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
