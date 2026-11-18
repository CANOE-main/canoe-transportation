"""Routing, isolation and identity contracts for partitioned parameter controls."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from utils import (
    harmonization_rule_fingerprints, harmonization_rule_paths, load_harmonization_rules,
    load_rules_config, load_rules_for_owner, load_yaml, rule_config_paths,
)


def write_yaml(path, payload):
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


@pytest.fixture
def controls(tmp_path):
    directory = tmp_path / "parameters"
    directory.mkdir()
    index = {"version": 3, "partitions": [
        {"file": "source_rules.yaml", "fetching": ["observations"]},
        {"file": "model_rules.yaml", "parameterization": ["consumption"]},
    ]}
    source = {"labels": ["Car", "Light Truck"], "year": 2023, "enabled": False, "missing": None}
    model = {"weights": {2: 0.25, 3: 0.75}, "unit": "MJ/vkm", "ordered": [2035, 2025]}
    write_yaml(directory / "rules.yaml", index)
    write_yaml(directory / "source_rules.yaml", {"version": 1, "fetching": {"observations": source}})
    write_yaml(directory / "model_rules.yaml", {"version": 1, "parameterization": {"consumption": model}})
    bundle = SimpleNamespace(repo_root=tmp_path, paths=SimpleNamespace(config=SimpleNamespace(parameters="parameters")))
    return directory, bundle, index, source, model


def test_current_and_explicit_historical_formats_preserve_owner_payloads(controls):
    directory, bundle, _, source, model = controls
    logical = {"version": 2, "fetching": {"observations": source}, "parameterization": {"consumption": model}}
    assert load_rules_config(directory) == logical
    assert load_harmonization_rules(bundle, "consumption") == model
    assert list(load_harmonization_rules(bundle, "consumption")["weights"]) == [2, 3]
    assert type(load_harmonization_rules(bundle, "observations")["enabled"]) is bool
    write_yaml(directory / "rules.yaml", logical)
    assert load_rules_config(directory) == logical
    assert load_harmonization_rules(bundle, "observations") == source
    assert harmonization_rule_paths(bundle) == (directory / "rules.yaml",)


def test_complete_identity_uses_declared_order_and_detects_member_only_changes(controls):
    directory, bundle, *_ = controls
    expected = ("rules.yaml", "source_rules.yaml", "model_rules.yaml")
    assert tuple(path.name for path in rule_config_paths(directory)) == expected
    before = harmonization_rule_fingerprints(bundle)
    path = directory / "model_rules.yaml"
    path.write_text(path.read_text() + "# review clarification\n", encoding="utf-8")
    after = harmonization_rule_fingerprints(bundle)
    assert list(after) == list(expected)
    assert {key for key in after if after[key] != before[key]} == {"model_rules.yaml"}
    assert load_rules_config(directory)["parameterization"]["consumption"]["unit"] == "MJ/vkm"


def test_owner_reads_are_fresh_independent_and_do_not_parse_other_partitions(controls, monkeypatch):
    import utils

    directory, bundle, *_ = controls
    read = []
    original = utils.load_yaml
    monkeypatch.setattr(utils, "load_yaml", lambda path: (read.append(Path(path).name), original(path))[1])
    rules = load_harmonization_rules(bundle, "consumption")
    rules["weights"][2] = 99
    assert read == ["rules.yaml", "model_rules.yaml"]
    assert load_harmonization_rules(bundle, "consumption")["weights"][2] == .25
    payload = load_yaml(directory / "model_rules.yaml")
    payload["parameterization"]["consumption"]["unit"] = "kWh/vkm"
    write_yaml(directory / "model_rules.yaml", payload)
    assert load_harmonization_rules(bundle, "consumption")["unit"] == "kWh/vkm"


def test_missing_current_member_never_uses_live_or_legacy_fallback(controls, tmp_path, monkeypatch):
    directory, bundle, *_ = controls
    (directory / "model_rules.yaml").unlink()
    live = tmp_path / "other"
    live.mkdir()
    write_yaml(live / "model_rules.yaml", {"version": 1, "parameterization": {"consumption": {"unit": "wrong"}}})
    monkeypatch.chdir(live)
    with pytest.raises(FileNotFoundError, match="Missing declared rules partition"):
        load_harmonization_rules(bundle, "consumption")
    with pytest.raises(FileNotFoundError, match="Missing declared rules partition"):
        load_rules_config(directory)


def test_unlisted_files_do_not_join_the_contract_and_external_root_is_explicit(controls, tmp_path, monkeypatch):
    directory, bundle, *_ = controls
    write_yaml(directory / "unregistered.yaml", {"version": 1, "fetching": {"observations": {"wrong": 1}}})
    bundle.paths.config.parameters = str(directory)
    monkeypatch.chdir(tmp_path.parent)
    assert len(harmonization_rule_paths(bundle)) == 3
    assert "wrong" not in load_harmonization_rules(bundle, "observations")
    with pytest.raises(KeyError, match="Missing extraction/harmonization rules"):
        load_harmonization_rules(bundle, "unknown")


@pytest.mark.parametrize("filename", ["../escape.yaml", "/absolute.yaml", "C:/absolute.yaml", "nested/rules.yaml", "rules.yaml", "other.yml"])
def test_member_paths_are_flat_bounded_and_cannot_include_the_index(controls, filename):
    directory, _, index, *_ = controls
    index["partitions"][0]["file"] = filename
    write_yaml(directory / "rules.yaml", index)
    with pytest.raises(ValueError):
        load_rules_config(directory)


@pytest.mark.parametrize("change", ["file", "owner", "cross_section", "empty", "extra"])
def test_manifest_rejects_ambiguous_or_unsupported_routing(controls, change):
    directory, _, index, *_ = controls
    if change == "file":
        index["partitions"].append(deepcopy(index["partitions"][0]))
    elif change == "owner":
        index["partitions"][1]["parameterization"] = ["observations"]
    elif change == "cross_section":
        index["partitions"][0]["parameterization"] = ["observations"]
    elif change == "empty":
        index["partitions"][0]["fetching"] = []
    else:
        index["overrides"] = {"consumption": {}}
    write_yaml(directory / "rules.yaml", index)
    with pytest.raises(ValueError):
        load_rules_config(directory)


@pytest.mark.parametrize("change", ["missing_owner", "undeclared_owner", "wrong_section", "bad_version", "scalar_owner"])
def test_partition_membership_is_exact_without_merging_or_overrides(controls, change):
    directory, *_ = controls
    path = directory / "model_rules.yaml"
    payload = load_yaml(path)
    if change == "missing_owner":
        payload["parameterization"].clear()
    elif change == "undeclared_owner":
        payload["parameterization"]["other"] = {}
    elif change == "wrong_section":
        payload["fetching"] = {}
    elif change == "bad_version":
        payload["version"] = 2
    else:
        payload["parameterization"]["consumption"] = 1
    write_yaml(path, payload)
    with pytest.raises(ValueError, match="Rules partition"):
        load_rules_config(directory)


def test_invalid_unselected_member_is_rejected_by_complete_validation(controls):
    directory, bundle, *_ = controls
    (directory / "source_rules.yaml").write_text("version: 1\nfetching: {}\n")
    assert load_harmonization_rules(bundle, "consumption")["unit"] == "MJ/vkm"
    with pytest.raises(ValueError, match="missing or undeclared"):
        load_rules_config(directory)


def test_low_level_owner_lookup_has_no_unrelated_payload_dependency(controls):
    directory, _, _, _, model = controls
    (directory / "source_rules.yaml").unlink()
    assert load_rules_for_owner(directory, "consumption") == model
    with pytest.raises(FileNotFoundError, match="Missing declared rules partition"):
        load_rules_config(directory)


def test_legacy_duplicate_ownership_and_nonmapping_are_rejected(controls):
    directory, *_ = controls
    write_yaml(directory / "rules.yaml", {"version": 2, "fetching": {"shared": {}}, "parameterization": {"shared": {}}})
    with pytest.raises(ValueError, match="exactly one owner"):
        load_rules_config(directory)
    write_yaml(directory / "rules.yaml", {"version": 2, "fetching": {"shared": 1}, "parameterization": {}})
    with pytest.raises(ValueError, match="Expected mapping"):
        load_rules_config(directory)
