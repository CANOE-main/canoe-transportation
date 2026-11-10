"""Documentation publication boundaries do not depend on transport scenarios."""

import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

mkdocs_files = pytest.importorskip("mkdocs.structure.files")
File, Files = mkdocs_files.File, mkdocs_files.Files
spec = importlib.util.spec_from_file_location("docs_hooks", Path(__file__).resolve().parents[1] / "scripts/docs_hooks.py")
docs_hooks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(docs_hooks)


def test_publication_excludes_notebooks_plans_and_raw_evidence(tmp_path, monkeypatch):
    docs = tmp_path / "docs"
    vendor = tmp_path / "vendor"
    theme = tmp_path / "theme"
    for directory in (docs, vendor, theme):
        directory.mkdir()
    (vendor / "renderer.js").write_text("// renderer", encoding="utf-8")
    digest = hashlib.sha256((vendor / "renderer.js").read_bytes()).hexdigest()
    (vendor / "manifest.json").write_text(json.dumps({"packages": {}, "files": {"renderer.js": digest}}))
    config = SimpleNamespace(docs_dir=str(docs), site_dir=str(tmp_path / "site"),
                             use_directory_urls=True, extra={"publication_assets": []},
                             plugins=SimpleNamespace(_current_plugin=None))
    paths = ["index.md", "insights/private.py", "planned.md", "evidence.csv", "assets/figures/selected.svg"]
    files = Files([File(path, str(docs), config.site_dir, True) for path in paths])
    files.append(File("assets/theme.js", str(theme), config.site_dir, True))
    monkeypatch.setattr(docs_hooks, "_PAGES", {"index.md"})
    monkeypatch.setattr(docs_hooks, "_CANONICAL", {})
    monkeypatch.setattr(docs_hooks, "artifact_path", lambda name: vendor)
    result = docs_hooks.on_files(files, config)
    assert {file.src_uri for file in result} == {
        "index.md", "assets/figures/selected.svg", "assets/theme.js", "assets/vendor/renderer.js",
    }


def test_changed_renderer_fails_instead_of_downloading(tmp_path, monkeypatch):
    (tmp_path / "renderer.js").write_text("changed", encoding="utf-8")
    (tmp_path / "manifest.json").write_text(json.dumps({"packages": {}, "files": {"renderer.js": "wrong"}}))
    config = SimpleNamespace(docs_dir=str(tmp_path), extra={"publication_assets": []})
    monkeypatch.setattr(docs_hooks, "_PAGES", set())
    monkeypatch.setattr(docs_hooks, "_CANONICAL", {})
    monkeypatch.setattr(docs_hooks, "artifact_path", lambda name: tmp_path)
    with pytest.raises(ValueError, match="Prepared documentation asset changed"):
        docs_hooks.on_files(Files([]), config)


def test_canonical_scenario_links_resolve_to_published_pages(tmp_path, monkeypatch):
    (tmp_path / "config/scenarios").mkdir(parents=True)
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs/assumptions.md").write_text("# Assumptions")
    monkeypatch.setattr(docs_hooks, "ROOT", tmp_path)
    monkeypatch.setattr(docs_hooks, "_PAGES", {"assumptions.md", "scenario_configuration.md"})
    monkeypatch.setattr(docs_hooks, "_CANONICAL", {"scenario_configuration.md": "config/scenarios/README.md"})
    config = SimpleNamespace(docs_dir=str(tmp_path / "docs"), repo_url="https://example.com/repository", extra={"repository_branch": "v2.0"})
    assert docs_hooks.rewrite_target("../../docs/assumptions.md#demand", "scenario_configuration.md", config) == "assumptions.md#demand"


def test_repository_links_use_configured_branch_and_preserve_fragments(tmp_path, monkeypatch):
    (tmp_path / "docs/model").mkdir(parents=True)
    (tmp_path / "config").mkdir()
    (tmp_path / "config/rules.yaml").write_text("version: 1")
    monkeypatch.setattr(docs_hooks, "ROOT", tmp_path)
    monkeypatch.setattr(docs_hooks, "_PAGES", {"model/energy.md"})
    monkeypatch.setattr(docs_hooks, "_CANONICAL", {})
    config = SimpleNamespace(docs_dir=str(tmp_path / "docs"), repo_url="https://example.com/repository", extra={"repository_branch": "v2.0"})
    assert docs_hooks.rewrite_target("../../config/rules.yaml#L1", "model/energy.md", config) == "https://example.com/repository/blob/v2.0/config/rules.yaml#L1"
