"""Documentation publication boundaries do not depend on transport scenarios."""

import hashlib
import importlib.util
import json
import re
import shutil
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from urllib.error import URLError
from urllib.request import urlopen

import pytest
import yaml

mkdocs_files = pytest.importorskip("mkdocs.structure.files")
File, Files = mkdocs_files.File, mkdocs_files.Files
spec = importlib.util.spec_from_file_location("docs_hooks", Path(__file__).resolve().parents[1] / "scripts/docs_hooks.py")
docs_hooks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(docs_hooks)
ROOT = Path(__file__).resolve().parents[1]


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
    config = SimpleNamespace(docs_dir=str(tmp_path / "docs"), repo_url="https://example.com/repository", extra={"repository_branch": "main"})
    assert docs_hooks.rewrite_target("../../docs/assumptions.md#demand", "scenario_configuration.md", config) == "assumptions.md#demand"


@pytest.mark.parametrize("branch", ["main", "review-branch"])
def test_repository_links_use_configured_branch_and_preserve_fragments(tmp_path, monkeypatch, branch):
    (tmp_path / "docs/model").mkdir(parents=True)
    (tmp_path / "config").mkdir()
    (tmp_path / "config/rules.yaml").write_text("version: 1")
    monkeypatch.setattr(docs_hooks, "ROOT", tmp_path)
    monkeypatch.setattr(docs_hooks, "_PAGES", {"model/energy.md"})
    monkeypatch.setattr(docs_hooks, "_CANONICAL", {})
    config = SimpleNamespace(docs_dir=str(tmp_path / "docs"), repo_url="https://example.com/repository", extra={"repository_branch": branch})
    assert docs_hooks.rewrite_target("../../config/rules.yaml#L1", "model/energy.md", config) == f"https://example.com/repository/blob/{branch}/config/rules.yaml#L1"


@pytest.mark.parametrize("destination", ["default", "preview", "custom"])
def test_output_routing_preserves_native_preview_and_caller_directories(tmp_path, monkeypatch, destination):
    from mkdocs.config.defaults import MkDocsConfig

    default = tmp_path / MkDocsConfig.site_dir.default
    selected = {"default": default, "preview": tmp_path / "mkdocs_preview", "custom": tmp_path / "custom"}[destination]
    publication = tmp_path / "publication"
    canonical = "config/scenarios/README.md"
    config = SimpleNamespace(
        nav=[{"Scenario": "scenario_configuration.md"}],
        extra={"canonical_pages": {"scenario_configuration.md": canonical}},
        mdx_configs={"pymdownx.superfences": {"custom_fences": []}},
        site_dir=str(selected), watch=[],
    )
    monkeypatch.setattr(docs_hooks, "ROOT", tmp_path)
    monkeypatch.setattr(docs_hooks, "artifact_path", lambda name: publication)
    monkeypatch.setattr(docs_hooks.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout="fixture"))
    monkeypatch.setattr(docs_hooks, "_CANONICAL", {})
    monkeypatch.setattr(docs_hooks, "_PAGES", set())

    docs_hooks.on_config(config)
    docs_hooks.on_config(config)

    assert Path(config.site_dir) == (publication if destination == "default" else selected)
    assert config.watch == [str((tmp_path / canonical).resolve())]


def test_preview_build_preserves_existing_publication_identity(tmp_path, monkeypatch):
    publication = tmp_path / "publication"
    validation = tmp_path / "validation"
    validation.mkdir()
    identity = validation / "build_identity.json"
    identity.write_bytes(b"prior publication identity\n")
    monkeypatch.setattr(docs_hooks, "artifact_path", lambda name: {
        "documentation_site": publication,
        "documentation_render_validation": validation,
    }[name])

    docs_hooks.on_post_build(SimpleNamespace(site_dir=str(tmp_path / "preview")))

    assert identity.read_bytes() == b"prior publication identity\n"


@pytest.mark.parametrize("port", [None, 8123])
def test_wrapper_uses_native_server_and_respects_default_address(monkeypatch, port):
    from mkdocs.commands import serve as native

    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    spec = importlib.util.spec_from_file_location("build_docs_test", ROOT / "scripts/build_docs.py")
    build_docs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(build_docs)
    calls = []
    monkeypatch.setattr(native, "serve", lambda **kwargs: calls.append(kwargs))

    build_docs.serve(port)

    expected = {"config_file": str(ROOT / "mkdocs.yml")}
    if port is not None:
        expected["dev_addr"] = f"127.0.0.1:{port}"
    assert calls == [expected]


@pytest.fixture
def documentation_project(tmp_path):
    """A tiny real MkDocs project with all writes isolated from publication assets."""
    for directory in ("scripts/docs/overrides/partials", "docs/assets/figures", "docs/assets/stylesheets",
                      "config/scenarios", "vendor"):
        (tmp_path / directory).mkdir(parents=True, exist_ok=True)
    for name in ("build_docs.py", "docs_hooks.py"):
        shutil.copyfile(ROOT / "scripts" / name, tmp_path / "scripts" / name)
    (tmp_path / "docs/index.md").write_text("# Home\n\nOriginal Markdown.\n", encoding="utf-8")
    (tmp_path / "config/scenarios/README.md").write_text("# Scenario\n\nOriginal scenario.\n", encoding="utf-8")
    (tmp_path / "docs/assets/stylesheets/documentation.css").write_text("/* original CSS */\n", encoding="utf-8")
    (tmp_path / "scripts/docs/overrides/main.html").write_text(
        '{% extends "base.html" %}{% block footer %}Original template{% endblock %}', encoding="utf-8",
    )
    (tmp_path / "scripts/docs/overrides/partials/source.html").write_text("", encoding="utf-8")
    (tmp_path / "scripts/docs/report_cover.html").write_text("", encoding="utf-8")
    (tmp_path / "uv.lock").write_text("fixture lock\n", encoding="utf-8")
    renderer = b"// verified local renderer\n"
    (tmp_path / "vendor/renderer.js").write_bytes(renderer)
    (tmp_path / "vendor/manifest.json").write_text(json.dumps({
        "packages": {}, "files": {"renderer.js": hashlib.sha256(renderer).hexdigest()},
    }), encoding="utf-8")
    (tmp_path / "config/paths.yaml").write_text(yaml.safe_dump({"artifacts": {
        name: {"path": path} for name, path in {
            "documentation_site": "publication/site",
            "documentation_render_validation": "publication/validation",
            "documentation_vendor": "vendor",
        }.items()
    }}), encoding="utf-8")
    (tmp_path / "mkdocs.yml").write_text(yaml.safe_dump({
        "site_name": "Preview fixture", "site_url": "https://example.invalid/canoe-transportation/",
        "docs_dir": "docs", "strict": True,
        "theme": {"name": "mkdocs", "custom_dir": "scripts/docs/overrides"},
        "nav": [{"Home": "index.md"}, {"Scenario": "scenario_configuration.md"}],
        "hooks": ["scripts/docs_hooks.py"], "plugins": [], "watch": ["scripts/docs"],
        "markdown_extensions": [{"pymdownx.superfences": {"custom_fences": []}}],
        "extra_css": ["assets/stylesheets/documentation.css"],
        "extra": {"canonical_pages": {"scenario_configuration.md": "config/scenarios/README.md"},
                  "publication_assets": ["assets/stylesheets/documentation.css"], "renderer_packages": {}},
    }), encoding="utf-8")
    subprocess.run(["git", "init", "--quiet"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "-c", "user.name=Documentation test", "-c", "user.email=tests@example.invalid",
                    "commit", "--allow-empty", "--quiet", "-m", "Isolated documentation fixture"],
                   cwd=tmp_path, check=True, capture_output=True)
    return tmp_path


def wait_for_preview(url, expected, process, log, after_epoch=None):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        assert process.poll() is None, log.read_text(encoding="utf-8")
        try:
            with urlopen(url, timeout=1) as response:
                content = response.read().decode("utf-8")
            epoch = re.search(r"livereload\((\d+),", content)
            if expected in content and (after_epoch is None or (epoch and int(epoch[1]) > after_epoch)):
                return content
        except (URLError, TimeoutError):
            pass
        time.sleep(0.05)
    pytest.fail(f"Preview did not update at {url}:\n{log.read_text(encoding='utf-8')}")


@contextmanager
def documentation_server(project, native_options=None):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    if native_options is None:
        command = [sys.executable, "scripts/build_docs.py", "serve", "--port", str(port)]
    else:
        command = [sys.executable, "-m", "mkdocs", "serve", "--dev-addr", f"127.0.0.1:{port}", *native_options]
    log = project / "server.log"
    with log.open("w", encoding="utf-8") as stream:
        process = subprocess.Popen(command, cwd=project, stdout=stream, stderr=subprocess.STDOUT)
        try:
            yield f"http://127.0.0.1:{port}/canoe-transportation/", process, log
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def test_live_preview_rebuilds_markdown_canonical_styles_and_templates(documentation_project):
    project = documentation_project
    command = [sys.executable, "scripts/build_docs.py"]
    subprocess.run([*command, "build"], cwd=project, check=True, capture_output=True, text=True)
    publication = project / "publication"
    prior = {path: path.read_bytes() for path in publication.rglob("*") if path.is_file()}
    assert json.loads((publication / "validation/links.json").read_text())["failures"] == []

    with documentation_server(project) as (base, process, log):
        html = wait_for_preview(base, "Original Markdown", process, log)
        epoch = int(re.search(r"livereload\((\d+),", html)[1])
        (project / "docs/index.md").write_text("# Home\n\nSaved Markdown.\n", encoding="utf-8")
        html = wait_for_preview(base, "Saved Markdown", process, log, after_epoch=epoch)
        with urlopen(base.replace("/canoe-transportation/", f"/livereload/{epoch}/1"), timeout=2) as response:
            assert int(response.read()) > epoch

        (project / "config/scenarios/README.md").write_text("# Scenario\n\nSaved scenario.\n", encoding="utf-8")
        wait_for_preview(base + "scenario_configuration/", "Saved scenario", process, log)

        epoch = int(re.search(r"livereload\((\d+),", wait_for_preview(base, "Saved Markdown", process, log))[1])
        (project / "docs/assets/stylesheets/documentation.css").write_text("/* saved CSS */\n", encoding="utf-8")
        wait_for_preview(base, "Saved Markdown", process, log, after_epoch=epoch)
        wait_for_preview(base + "assets/stylesheets/documentation.css", "saved CSS", process, log)

        (project / "scripts/docs/overrides/main.html").write_text(
            '{% extends "base.html" %}{% block footer %}Saved template{% endblock %}', encoding="utf-8",
        )
        wait_for_preview(base, "Saved template", process, log)

    assert {path: path.read_bytes() for path in publication.rglob("*") if path.is_file()} == prior
    stale_pdf = subprocess.run([*command, "pdf"], cwd=project, capture_output=True, text=True)
    assert stale_pdf.returncode != 0
    assert "Documentation changed after site build: docs/index.md; run build again" in stale_pdf.stderr


@pytest.mark.parametrize("options", [[], ["--clean"], ["--no-livereload"]])
def test_native_preview_variants_serve_without_publishing(documentation_project, options):
    with documentation_server(documentation_project, native_options=options) as (base, process, log):
        html = wait_for_preview(base, "Original Markdown", process, log)
        assert ("Enabled live reload" in html) == ("--no-livereload" not in options)
    assert not (documentation_project / "publication").exists()
