"""Publish only selected documentation; never import or execute transport ETL."""

from __future__ import annotations

import hashlib
import json
import logging
import posixpath
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import yaml
from mkdocs.plugins import event_priority
from mkdocs.structure.files import File
from pymdownx.arithmatex import arithmatex_fenced_format

ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger("mkdocs.canoe")
_CANONICAL = {}
_PAGES = set()


def artifact_path(name: str) -> Path:
    """Read only the registered documentation route, without loading a scenario."""
    paths = yaml.safe_load((ROOT / "config/paths.yaml").read_text(encoding="utf-8"))
    path = (ROOT / paths["artifacts"][name]["path"]).resolve()
    if not path.is_relative_to(ROOT):
        raise ValueError(f"Documentation artifact is outside the repository: {name}")
    return path


def nav_pages(nav):
    for item in nav:
        if isinstance(item, dict):
            for target in item.values():
                if isinstance(target, list):
                    yield from nav_pages(target)
                elif target.endswith(".md"):
                    yield target


@event_priority(100)
def on_config(config):
    global _CANONICAL, _PAGES
    _CANONICAL = config.extra.get("canonical_pages", {})
    _PAGES = set(nav_pages(config.nav))
    for fence in config.mdx_configs["pymdownx.superfences"]["custom_fences"]:
        if fence["name"] == "math":
            fence["format"] = arithmatex_fenced_format(mode="generic")
    config.site_dir = str(artifact_path("documentation_site"))
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True,
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain"], cwd=ROOT, check=True, capture_output=True, text=True,
    ).stdout.strip()
    config.extra["documentation_revision"] = revision + (" + working changes" if dirty else "")
    config.extra["documentation_built_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return config


@event_priority(100)
def on_files(files, config):
    allowed = _PAGES | set(config.extra["publication_assets"])
    for file in list(files):
        if Path(file.src_dir).resolve() != Path(config.docs_dir).resolve():
            continue  # Theme/plugin assets are controlled by the locked dependencies.
        is_figure = file.src_uri.startswith("assets/figures/") and Path(file.src_uri).suffix in {".svg", ".png", ".json"}
        if file.src_uri not in allowed and not is_figure:
            files.remove(file)
    for uri, source in _CANONICAL.items():
        files.append(File.generated(config, uri, abs_src_path=str(ROOT / source)))
    vendor = artifact_path("documentation_vendor")
    manifest_path = vendor / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError("Missing prepared render assets. Run: uv run --group docs python scripts/build_docs.py prepare-assets")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("packages") != config.extra.get("renderer_packages", {}):
        raise ValueError("Prepared renderer versions differ from mkdocs.yml; run prepare-assets explicitly")
    for relative, expected in manifest["files"].items():
        source = (vendor / relative).resolve()
        if not source.is_relative_to(vendor) or hashlib.sha256(source.read_bytes()).hexdigest() != expected:
            raise ValueError(f"Prepared documentation asset changed: {relative}")
        files.append(File.generated(config, f"assets/vendor/{relative}", abs_src_path=str(source)))
    return files


def rewrite_target(target: str, page_uri: str, config) -> str:
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc or not parsed.path:
        return target
    source = ROOT / _CANONICAL.get(page_uri, "docs/" + page_uri)
    resolved = (source.parent / unquote(parsed.path)).resolve()
    if not resolved.is_relative_to(ROOT):
        return target
    repository_path = resolved.relative_to(ROOT).as_posix()
    reverse = {value: key for key, value in _CANONICAL.items()}
    published = reverse.get(repository_path)
    if repository_path.startswith("docs/"):
        candidate = repository_path[5:]
        if candidate in _PAGES or candidate.startswith("assets/figures/"):
            published = candidate
    elif resolved.is_relative_to(Path(config.docs_dir).resolve()):
        candidate = resolved.relative_to(Path(config.docs_dir).resolve()).as_posix()
        if candidate in _PAGES or candidate.startswith("assets/figures/"):
            published = candidate
    suffix = ("?" + parsed.query if parsed.query else "") + ("#" + parsed.fragment if parsed.fragment else "")
    if published:
        return posixpath.relpath(published, posixpath.dirname(page_uri) or ".") + suffix
    if not resolved.exists():
        return target  # Strict MkDocs validation reports a genuine broken reference.
    return config.repo_url.rstrip("/") + "/blob/" + config.extra["repository_branch"] + "/" + quote(repository_path, safe="/") + suffix


def on_page_markdown(markdown, page, config, files):
    uri = page.file.src_uri
    # Keep repository review markers visible without interpreting them as ATX
    # headings (Python-Markdown accepts a heading without a space after '#').
    markdown = re.sub(r"(?m)^#to-review[ \t]*$", "`#to-review`", markdown)
    if not re.search(r"(?m)^#\s+", markdown):
        # Material otherwise inserts this visible title only after the print
        # plugin captures the content. Keep canonical Markdown untouched.
        markdown = f"# {page.title}\n\n" + markdown
    # Only Markdown links; diagrams and their user-controlled source stay byte-identical.
    markdown = re.sub(
        r"(?P<start>\]\()(?P<url>[^\s)]+)(?P<end>\))",
        lambda match: match["start"] + rewrite_target(match["url"], uri, config) + match["end"],
        markdown,
    )
    return re.sub(
        r"(?m)^(?P<start>\[[^\]]+\]:\s*)(?P<url>\S+)",
        lambda match: match["start"] + rewrite_target(match["url"], uri, config),
        markdown,
    )


def on_post_build(config):
    output = artifact_path("documentation_render_validation")
    output.mkdir(parents=True, exist_ok=True)
    identity = {
        "revision": config.extra["documentation_revision"],
        "built_at": config.extra["documentation_built_at"],
        "site_url": config.site_url,
        "ordered_pages": list(nav_pages(config.nav)),
        "source_sha256": {
            page: hashlib.sha256((ROOT / _CANONICAL.get(page, "docs/" + page)).read_bytes()).hexdigest()
            for page in nav_pages(config.nav)
        },
        "source_paths": {page: _CANONICAL.get(page, "docs/" + page) for page in nav_pages(config.nav)},
        "configuration_sha256": hashlib.sha256((ROOT / "mkdocs.yml").read_bytes()).hexdigest(),
        "dependency_lock_sha256": hashlib.sha256((ROOT / "uv.lock").read_bytes()).hexdigest(),
        "renderer_manifest_sha256": hashlib.sha256((artifact_path("documentation_vendor") / "manifest.json").read_bytes()).hexdigest(),
        "figure_sha256": {
            file.relative_to(ROOT).as_posix(): hashlib.sha256(file.read_bytes()).hexdigest()
            for file in sorted((ROOT / "docs/assets/figures").glob("*")) if file.is_file()
        },
        "publication_support_sha256": {
            path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            for path in [
                "scripts/docs_hooks.py", "scripts/build_docs.py",
                "scripts/docs/report_cover.html", "scripts/docs/overrides/partials/source.html",
                *("docs/" + path for path in config.extra["publication_assets"]),
            ]
        },
        "rendering_runs_transport_etl": False,
    }
    (output / "build_identity.json").write_text(json.dumps(identity, indent=2) + "\n", encoding="utf-8")
