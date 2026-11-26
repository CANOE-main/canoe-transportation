"""Explicit, audited adoption of registered rolling-source snapshots.

The default is a network-enabled review with no registry/cache changes. ``--apply``
validates the entire selected batch before publishing new immutable caches and an
atomic registry replacement. Production builds never call this maintenance module.
"""

from __future__ import annotations

import argparse
import copy
import difflib
import hashlib
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4
from zipfile import ZipFile

import requests
import yaml
from pydantic import BaseModel, ConfigDict, Field
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from yaml.nodes import MappingNode, ScalarNode, SequenceNode

from utils import (
    ConfigBundle,
    UniqueKeyLoader,
    file_sha256,
    harmonization_rule_fingerprints,
    load_config_bundle,
    load_harmonization_rules,
    resolve_artifact_path,
    resolve_input_path,
    resolve_parameter_path,
    write_text_atomic,
)
from validation.config_models import SourcesConfig


class UpdateError(ValueError):
    """A candidate cannot safely replace the registered snapshot."""

    def __init__(self, message: str, *, evidence: dict | None = None):
        super().__init__(message)
        self.evidence = evidence or {}


class CatalogueResource(BaseModel):
    """Only shared CKAN transport fields; source-native contracts stay with adapters."""

    model_config = ConfigDict(extra="ignore")
    id: str = Field(min_length=1)
    url: str = Field(pattern=r"^https://[^\s]+$")
    name: str = ""
    format: str = ""
    hash: str | None = None
    size: int | None = Field(default=None, ge=0)
    last_modified: str | None = None
    metadata_modified: str | None = None


@dataclass
class Candidate:
    source: str
    component: str
    edits: list[tuple[tuple[str | int, ...], Any]] = field(default_factory=list)
    files: dict[Path, bytes] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    review_required: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class UpdateRun:
    report_path: Path
    log_path: Path
    diff_path: Path
    applied: bool
    errors: tuple[str, ...]


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


_REVIEW_KEYS = (
    "review_schema_version", "registry_sha256_before", "scenario_sha256",
    "rules_sha256", "rules_fingerprints", "selected_sources", "candidates", "cache_snapshots",
    "acquisitions",
)


def _review_fingerprint(report):
    evidence = {key: report.get(key) for key in _REVIEW_KEYS}
    # Reports predating component selection remain replayable without a
    # filter. A concrete selection is additional reviewed evidence, never implied.
    if report.get("selected_components") is not None:
        evidence["selected_components"] = report["selected_components"]
    return _sha(json.dumps(
        evidence,
        sort_keys=True, ensure_ascii=False,
    ).encode("utf-8"))


class _AcquisitionSession:
    """Retain official responses for exact, network-free reviewed revalidation."""

    def __init__(self, client, folder, *, acquisitions=None):
        self.client = client
        self.folder = folder
        self.replaying = acquisitions is not None
        self.acquisitions = [] if acquisitions is None else acquisitions
        self.position = 0

    @staticmethod
    def _request(method, url, kwargs):
        return {"method": method, "url": url,
                "params": kwargs.get("params"), "payload": kwargs.get("json")}

    def _send(self, method, url, kwargs):
        if not self.replaying:
            return getattr(self.client, method)(url, **kwargs)
        request = self._request(method, url, kwargs)
        if self.position >= len(self.acquisitions):
            raise UpdateError("Reviewed acquisition is missing; run report-only review again")
        entry = self.acquisitions[self.position]
        if entry["request"] != request:
            raise UpdateError("Reviewed acquisition request changed; run report-only review again")
        self.position += 1
        relative = Path(entry["file"])
        path = (self.folder / relative).resolve()
        if (relative.is_absolute() or not path.is_relative_to(
            (self.folder / "downloads").resolve()
        )):
            raise UpdateError("Reviewed acquisition path is outside its downloads directory")
        if (not path.is_file() or path.stat().st_size != entry["bytes"]
            or file_sha256(path) != entry["sha256"]):
            raise UpdateError("Reviewed acquisition is missing or changed; run report-only review again")
        response = requests.Response()
        response.status_code = 200
        response.url = entry["url"]
        response.headers.update(entry["headers"])
        response._content = path.read_bytes()
        response._content_consumed = True
        return response

    def get(self, url, **kwargs):
        return self._send("get", url, kwargs)

    def post(self, url, **kwargs):
        return self._send("post", url, kwargs)

    def record(self, url, response, *, payload, params):
        if self.replaying:
            return
        content = response.content
        sha = _sha(content)
        relative = Path("downloads") / f"{sha}.bin"
        path = self.folder / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if file_sha256(path) != sha:
                raise UpdateError(f"Conflicting review download: {path}")
        else:
            with path.open("xb") as handle:
                handle.write(content)
        self.acquisitions.append({
            "request": {"method": "post" if payload is not None else "get",
                        "url": url, "params": params, "payload": payload},
            "file": relative.as_posix(), "sha256": sha, "bytes": len(content),
            "url": _public_url(str(getattr(response, "url", "") or url)),
            "headers": {key: value for key, value in response.headers.items()
                        if key.lower() in {"last-modified", "content-type", "etag"}},
        })


def _cache_identity(path, expected, *, algorithm="sha256", expected_bytes=None):
    if not path.is_file():
        return {"path": str(path), "status": "missing"}
    with path.open("rb") as handle:
        actual = hashlib.file_digest(handle, algorithm).hexdigest()
    valid = actual == expected and (
        expected_bytes is None or path.stat().st_size == expected_bytes
    )
    return {
        "path": str(path),
        "status": "valid" if valid else "mismatch",
        "algorithm": algorithm,
        "actual_digest": actual,
        "expected_digest": expected,
    }


def _public_url(value: str) -> str:
    """Omit signed redirect query strings from durable review evidence."""
    return urlparse(value)._replace(query="", fragment="").geturl()


def _url(value: str) -> str:
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
        raise UpdateError(f"Expected an official HTTPS source URL: {parsed.hostname or '(no host)'}")
    return value


def _response(
    client, url: str, settings, *, payload=None, params=None, allowed_hosts=()
):
    method = client.post if payload is not None else client.get
    kwargs = {"timeout": settings.timeout_seconds}
    if payload is not None:
        kwargs["json"] = payload
    if params is not None:
        kwargs["params"] = params
    response = None
    try:
        response = method(_url(url), stream=True, **kwargs)
        response.raise_for_status()
        final_url = str(getattr(response, "url", "") or url)
        _url(final_url)
        # Redirects can change paths, but moving a source to another host needs review.
        if urlparse(final_url).hostname not in {urlparse(url).hostname, *allowed_hosts}:
            raise UpdateError(
                f"Source redirected to an unregistered host: {urlparse(final_url).hostname}"
            )
        parts, size = [], 0
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            size += len(chunk)
            if size > settings.max_download_bytes:
                raise UpdateError(
                    f"Source exceeds configured max_download_bytes: {url}"
                )
            parts.append(chunk)
        response._content = b"".join(parts)
        response._content_consumed = True
        if isinstance(client, _AcquisitionSession):
            client.record(url, response, payload=payload, params=params)
    except requests.RequestException as exc:
        method_name = "POST" if payload is not None else "GET"
        status = exc.response.status_code if exc.response is not None else None
        reason = f"HTTP {status}" if status is not None else type(exc).__name__
        public_url = _public_url(url)
        raise UpdateError(
            f"{method_name} {public_url} failed: {reason}",
            evidence={"url": public_url, "method": method_name, "params": params,
                      "exception": type(exc).__name__, "http_status": status},
        ) from None
    finally:
        if response is not None:
            response.close()
    return response


def _json(client, url: str, settings, *, payload=None, params=None):
    response = _response(client, url, settings, payload=payload, params=params)
    try:
        return response.json()
    except requests.exceptions.JSONDecodeError:
        public_url = _public_url(url)
        raise UpdateError(
            f"Expected JSON metadata from {public_url}; the response could not be decoded",
            evidence={"url": public_url, "params": params,
                      "content_type": response.headers.get("Content-Type")},
        ) from None


def _resource(client, endpoint: str, resource_id: str, settings) -> CatalogueResource:
    payload = _json(client, endpoint, settings, params={"id": resource_id})
    if (not isinstance(payload, dict) or payload.get("success") is not True
        or not isinstance(payload.get("result"), dict)):
        raise UpdateError(f"Catalogue lookup failed for {resource_id}")
    result = CatalogueResource.model_validate(payload["result"])
    if result.id != resource_id:
        raise UpdateError(f"Catalogue returned a different resource for {resource_id}")
    return result


def _package(client, endpoint: str, package_id: str, settings) -> dict:
    payload = _json(client, endpoint, settings, params={"id": package_id})
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise UpdateError(f"Catalogue package lookup failed for {package_id}")
    result = payload["result"]
    if not isinstance(result, dict) or not isinstance(result.get("resources"), list):
        raise UpdateError(f"Catalogue package has no resource list: {package_id}")
    if package_id not in {result.get("id"), result.get("name")}:
        raise UpdateError(f"Catalogue returned a different package for {package_id}")
    return result


def _verified_bytes(
    client,
    resource: CatalogueResource,
    settings,
    *,
    require_md5=False,
    allowed_hosts=(),
) -> bytes:
    content = _response(
        client, resource.url, settings, allowed_hosts=allowed_hosts
    ).content
    if not content:
        raise UpdateError(f"Empty download: {resource.url}")
    if resource.size and len(content) != resource.size:
        raise UpdateError(f"Download size differs from catalogue for {resource.id}")
    if resource.hash:
        algorithm = {32: "md5", 64: "sha256"}.get(len(resource.hash))
        if algorithm is None or not re.fullmatch(r"[0-9a-fA-F]+", resource.hash):
            raise UpdateError(f"Unrecognized publisher checksum for {resource.id}")
        if (
            hashlib.new(algorithm, content, usedforsecurity=False).hexdigest()
            != resource.hash.lower()
        ):
            raise UpdateError(
                f"Download checksum differs from catalogue for {resource.id}"
            )
    elif require_md5:
        raise UpdateError(
            f"Catalogue publishes no MD5 for {resource.id}; review manually"
        )
    if require_md5 and len(resource.hash) != 32:
        raise UpdateError(
            f"Catalogue publishes no MD5 for {resource.id}; review manually"
        )
    return content


def _snapshot_path(bundle, original: Path, content: bytes) -> Path:
    cache = resolve_input_path(bundle, "cache").resolve()
    original = original.resolve()
    if not original.is_relative_to(cache):
        raise UpdateError(f"Source snapshot is outside configured cache: {original}")
    suffix = "".join(original.suffixes)
    stem = original.name[: -len(suffix)] if suffix else original.name
    stem = re.sub(r"-[0-9a-f]{16}(?:-recovered-\d+)?$", "", stem)
    digest = _sha(content)
    target = original.with_name(f"{stem}-{digest[:16]}{suffix}")
    recovery = 0
    while target.exists() and (not target.is_file() or file_sha256(target) != digest):
        recovery += 1
        target = original.with_name(f"{stem}-{digest[:16]}-recovered-{recovery}{suffix}")
    return target


def _relative(bundle, path: Path) -> str:
    return path.relative_to(resolve_input_path(bundle, "cache")).as_posix()


def _staged(folder: Path, original: Path, content: bytes) -> Path:
    path = folder / f"{_sha(content)[:16]}-{original.name}"
    path.write_bytes(content)
    return path


def _version(value: str | None) -> str:
    if not value:
        raise UpdateError("Publisher has no release date; review manually")
    try:
        return date.fromisoformat(value[:10]).isoformat()
    except ValueError as exc:
        raise UpdateError(f"Publisher release date is invalid: {value}") from exc


def _node_at(node, path):
    for key in path:
        if isinstance(node, MappingNode):
            matches = [
                v
                for k, v in node.value
                if yaml.safe_load(k.value) == key or k.value == str(key)
            ]
            if len(matches) != 1:
                raise UpdateError(f"Ambiguous or missing YAML path: {path}")
            node = matches[0]
        elif isinstance(node, SequenceNode) and isinstance(key, int):
            node = node.value[key]
        else:
            raise UpdateError(f"Invalid YAML path: {path}")
    return node


def edit_registry(text: str, edits: list[tuple[tuple[str | int, ...], Any]]) -> str:
    """Edit addressed YAML nodes, retaining unrelated bytes, comments and ordering."""
    expected = yaml.load(text, Loader=UniqueKeyLoader)
    newline = "\r\n" if "\r\n" in text else "\n"
    expanded = []

    def expand(path, value):
        previous = _value_at(expected, path)
        if isinstance(previous, dict) and isinstance(value, dict):
            if set(previous) - set(value):
                raise UpdateError(
                    f"Updater cannot remove registered mapping entries: {path}"
                )
            for key, child in value.items():
                expand((*path, key), child)
        elif previous != value:
            expanded.append((path, value))

    for path, value in edits:
        expand(path, value)
    for path, value in expanded:
        parent = expected
        for key in path[:-1]:
            parent = parent[key]
        existed = path[-1] in parent if isinstance(parent, dict) else True
        parent[path[-1]] = copy.deepcopy(value)
        tree = yaml.compose(text)
        node = _node_at(tree, path if existed else path[:-1])
        if existed:
            if isinstance(node, ScalarNode):
                rendered = (
                    json.dumps(value, ensure_ascii=False)
                    if isinstance(value, str)
                    else yaml.safe_dump(value).split("\n...")[0].strip()
                )
            elif isinstance(node, SequenceNode) and not node.flow_style:
                lines = yaml.safe_dump(value, sort_keys=False, default_flow_style=False).strip().splitlines()
                rendered = (newline + " " * node.start_mark.column).join(lines)
            else:
                rendered = yaml.safe_dump(
                    value, sort_keys=False, default_flow_style=True, width=100000
                ).strip()
            if (isinstance(node, ScalarNode) and node.style in {"|", ">"}) or (
                isinstance(node, (MappingNode, SequenceNode)) and not node.flow_style
            ):
                rendered += newline + " " * node.end_mark.column
            text = (
                text[: node.start_mark.index] + rendered + text[node.end_mark.index :]
            )
        elif isinstance(node, MappingNode) and not node.flow_style:
            indent = node.value[0][0].start_mark.column
            addition = yaml.safe_dump(
                {path[-1]: value}, sort_keys=False, allow_unicode=True
            )
            addition = "".join(
                " " * indent + line + newline for line in addition.splitlines()
            )
            position = node.end_mark.index - node.end_mark.column
            text = text[:position] + addition + text[position:]
        elif isinstance(node, MappingNode):
            rendered = yaml.safe_dump(
                parent, sort_keys=False, default_flow_style=True, width=100000
            ).strip()
            text = (
                text[: node.start_mark.index] + rendered + text[node.end_mark.index :]
            )
        else:
            raise UpdateError(f"Cannot add a field at YAML path {path}")
    if yaml.load(text, Loader=UniqueKeyLoader) != expected:
        raise UpdateError("Registry edit changed unrelated YAML values")
    SourcesConfig.model_validate(expected)
    return text


def _nrcan(bundle, client, settings, folder) -> list[Candidate]:
    from fetching import nrcan_ceud as adapter

    source_key = "nrcan_fuel_consumption_ratings"
    source = bundle.sources.sources[source_key]
    endpoint = source.update.metadata_url
    candidates = []
    for request in adapter.iter_rating_requests(bundle):
        resource = _resource(client, endpoint, request.resource_id, settings)
        if urlparse(resource.url).hostname != urlparse(request.url).hostname:
            raise UpdateError(
                f"NRCan resource moved to a different host: {resource.url}"
            )
        content = _verified_bytes(
            client,
            resource,
            settings,
            require_md5=True,
            allowed_hosts=source.update.download_hosts,
        )
        candidate_request = request.model_copy(
            update={
                "cache_path": _staged(folder, request.cache_path, content),
                "expected_md5": resource.hash.lower(),
                "expected_bytes": len(content),
            }
        )
        raw, _ = adapter.read_rating_csv(
            candidate_request,
            encoding_candidates=adapter.ratings_rules(bundle)["encoding_candidates"],
        )
        normalized = adapter.normalize_rating_dataframe(
            raw, candidate_request, enforce_coverage=False
        )
        years = sorted(normalized["Model year"].unique().tolist())
        if missing := set(request.expected_model_years) - set(years):
            raise UpdateError(
                f"NRCan {request.component_key} lost registered model years: {sorted(missing)}"
            )
        expanded = years != list(request.expected_model_years)
        if expanded:
            adapter.FuelConsumptionRatingRequest.model_validate(
                candidate_request.model_dump() | {"expected_model_years": years}
            )
        rows = len(normalized)
        if _resource(client, endpoint, request.resource_id, settings) != resource:
            raise UpdateError(
                f"NRCan metadata changed during download: {request.component_key}"
            )
        changed = (resource.hash.lower(), len(content)) != (
            request.expected_md5,
            request.expected_bytes,
        )
        version = _version(resource.last_modified)
        base = ("sources", source_key, "components", request.component_key)
        edits = [
            (base + ("version",), version),
            (base + ("adapter", "url"), resource.url),
        ]
        if expanded:
            edits.append((base + ("adapter", "expected_model_years"), years))
        if (
            isinstance(request.component_meta.label, str)
            and request.component_meta.version
        ):
            edits.append(
                (
                    base + ("label",),
                    request.component_meta.label.replace(
                        request.component_meta.version, version
                    ),
                )
            )
        local = _cache_identity(
            request.cache_path,
            request.expected_md5,
            algorithm="md5",
            expected_bytes=request.expected_bytes,
        )
        files = {}
        if changed or local["status"] != "valid":
            target = _snapshot_path(bundle, request.cache_path, content)
            edits.extend(
                [
                    (base + ("adapter", "cache_path"), _relative(bundle, target)),
                    (base + ("adapter", "expected_md5"), resource.hash.lower()),
                    (base + ("adapter", "expected_bytes"), len(content)),
                ]
            )
            files[target] = content
        candidates.append(
            Candidate(
                source_key,
                request.component_key,
                edits,
                files,
                {
                    "metadata_url": endpoint,
                    "resource": resource.model_dump(),
                    "rows": rows,
                    "sha256": _sha(content),
                    "bytes_changed": changed,
                    "registered_cache": local,
                    "model_years": {
                        "before": list(request.expected_model_years), "after": years,
                    },
                    "validation": "publisher MD5/size, required columns, nonshrinking model-year coverage",
                },
                ["NRCan model-year coverage expanded"] if expanded else [],
            )
        )
    return candidates


def _tc_izev(bundle, client, settings, folder) -> list[Candidate]:
    """Adopt revisions of registered iZEV resources without discovering new scope."""
    from fetching import transport_canada_izev as adapter

    source_key = adapter.SOURCE
    source = bundle.sources.sources[source_key]
    endpoint = source.update.metadata_url
    rules = adapter.module_rules(bundle)
    candidates = []
    for request in adapter.build_requests(bundle):
        resource = _resource(client, endpoint, str(request.resource_id), settings)
        try:
            download_request = adapter.IzevRequest.model_validate(
                request.model_dump() | {"url": resource.url}
            )
        except ValueError as exc:
            raise UpdateError(
                f"Catalogue URL for iZEV {request.component_key} does not identify "
                f"its registered official HTTPS CSV: {resource.url}"
            ) from exc
        content = _verified_bytes(
            client, resource, settings, allowed_hosts=source.update.download_hosts,
        )
        md5 = hashlib.md5(content, usedforsecurity=False).hexdigest()
        sha256 = _sha(content)
        changed = (md5, len(content)) != (request.expected_md5, request.expected_bytes)
        if changed and not resource.hash:
            raise UpdateError(
                f"iZEV {request.component_key} differs from the registered bytes and "
                "the catalogue publishes no checksum. Verify the publisher's revision before adoption.",
                evidence={"resource": resource.model_dump(), "sha256": sha256, "bytes": len(content)},
            )
        staged_request = download_request.model_copy(update={
            "cache_path": _staged(folder, request.cache_path, content),
        })
        audit = adapter.parse_csv(staged_request, rules=rules, enforce_coverage=False).attrs["audit"]
        coverage = {
            "model_years": request.expected_model_years,
            "provinces": request.expected_provinces,
            "powertrains": request.expected_powertrains,
        }
        for key, expected in coverage.items():
            if missing := set(expected) - set(audit[key]):
                raise UpdateError(f"iZEV {request.component_key} lost registered {key}: {sorted(missing)}")
        first_date = date.fromisoformat(audit["request_date_from"])
        last_date = date.fromisoformat(audit["request_date_to"])
        if first_date > request.expected_request_date_from or last_date < request.expected_request_date_to:
            raise UpdateError(f"iZEV {request.component_key} request-date coverage shrank")
        if _resource(client, endpoint, str(request.resource_id), settings) != resource:
            raise UpdateError(f"iZEV metadata changed during download: {request.component_key}")

        local = _cache_identity(request.cache_path, request.expected_md5, algorithm="md5", expected_bytes=request.expected_bytes)
        base = ("sources", source_key, "components", request.component_key)
        version = request.component_meta.version
        if resource.last_modified:
            version = _version(resource.last_modified)
        elif changed:
            version = f"sha256-{sha256[:16]}"
        edits = [(base + ("adapter", "url"), resource.url)]
        reviews = []
        if changed:
            reviews.append("iZEV registered snapshot bytes changed")
        if resource.url != request.url or version != request.component_meta.version:
            reviews.append("iZEV publication identity changed")
        if version != request.component_meta.version:
            edits.extend([
                (base + ("version",), version),
                (base + ("label",), f"iZEV {request.component_key} incentive records ({version})"),
            ])

        expanded = {key: audit[key] for key, expected in coverage.items() if set(audit[key]) != set(expected)}
        if first_date != request.expected_request_date_from or last_date != request.expected_request_date_to:
            expanded.update(request_date_from=audit["request_date_from"], request_date_to=audit["request_date_to"])
        if expanded:
            reviews.append(f"iZEV registered coverage expanded: {', '.join(expanded)}")
            edits.extend((base + ("adapter", f"expected_{key}"), value) for key, value in expanded.items())
        files = {}
        target = request.cache_path
        if not reviews and local["status"] == "missing" and not target.exists():
            files[target] = content
        elif changed or local["status"] != "valid":
            target = _snapshot_path(bundle, request.cache_path, content)
            edits.extend([
                (base + ("adapter", "cache_path"), _relative(bundle, target)),
                (base + ("adapter", "expected_md5"), md5),
                (base + ("adapter", "expected_bytes"), len(content)),
            ])
            files[target] = content
        adapter.IzevRequest.model_validate(download_request.model_dump() | {
            "cache_path": target, "expected_md5": md5, "expected_bytes": len(content),
            **{f"expected_{key}": value for key, value in expanded.items()},
        })
        candidates.append(Candidate(source_key, request.component_key, edits, files, {
            "metadata_url": endpoint, "resource": resource.model_dump(),
            "sha256": sha256, "bytes_changed": changed,
            "registered_cache": local, **audit,
            "validation": "publisher checksum or exact registered bytes, complete native records, distinct vehicle/request years, nonshrinking registered coverage",
        }, reviews))
    intervals = sorted((item.evidence["request_date_from"], item.evidence["request_date_to"]) for item in candidates)
    if any(a[1] >= b[0] for a, b in zip(intervals, intervals[1:])):
        raise UpdateError("iZEV revised resources have overlapping request-date coverage; review manually")
    return candidates


def _fuel_record_changes(before, after):
    """Compare consumed FuelEconomy rows as multisets, retaining duplicate counts."""
    from collections import Counter

    def records(frame):
        values = frame.astype(object).where(frame.notna(), None).to_dict("records")
        return Counter(json.dumps(row, sort_keys=True, ensure_ascii=False, allow_nan=False)
                       for row in values)

    current = records(after)
    previous = records(before) if before is not None else None

    def fingerprint(rows):
        return _sha("\n".join(sorted(rows.elements())).encode("utf-8"))

    evidence = {
        "before_rows": len(before) if before is not None else None,
        "after_rows": len(after),
        "before_sha256": fingerprint(previous) if previous is not None else None,
        "after_sha256": fingerprint(current),
        "changed": previous != current if previous is not None else None,
    }
    if previous is not None:
        for name, difference in (("added", current - previous), ("removed", previous - current)):
            evidence[f"{name}_rows"] = difference.total()
            evidence[f"{name}_sample"] = [
                {"record": json.loads(row), "count": difference[row]}
                for row in sorted(difference)[:10]
            ]
    return evidence


def _fueleconomy(bundle, client, settings, folder) -> list[Candidate]:
    from fetching import fueleconomy_vehicles as adapter

    source_key = adapter.SOURCE_KEY
    source = bundle.sources.sources[source_key]
    request = adapter.build_request(bundle)
    response = _response(
        client, request.url, settings, allowed_hosts=source.update.download_hosts
    )
    content = response.content
    sha = _sha(content)
    changed = sha != request.expected_sha256 or len(content) != request.expected_bytes
    staged = _staged(folder, request.cache_path, content)
    local = _cache_identity(
        request.cache_path,
        request.expected_sha256,
        expected_bytes=request.expected_bytes,
    )
    counts, observations, coverage = {}, {}, {}
    reasons, coverage_edits = [], []
    rules = adapter.module_rules(bundle)
    for component in source.components:
        original_request = adapter.build_request(bundle, component)
        component_request = original_request.model_copy(
            update={
                "cache_path": staged,
                "expected_sha256": sha,
                "expected_bytes": len(content),
            }
        )
        adapter.validate_cache(component_request)
        frame = adapter.read_selected_vehicle_columns(component_request, enforce_coverage=False)
        years = sorted(frame.year.unique().tolist())
        previous = None
        if local["status"] == "valid":
            previous = adapter.read_selected_vehicle_columns(original_request)
            old_years = sorted(previous.year.unique().tolist())
        else:
            old_years = [original_request.expected_model_year_from,
                         original_request.expected_model_year_to]
        if missing := set(old_years) - set(years):
            raise UpdateError(f"FuelEconomy {component} lost registered model years: {sorted(missing)}")
        expected_bounds = (original_request.expected_model_year_from, original_request.expected_model_year_to)
        bounds = (years[0], years[-1])
        if bounds[0] > expected_bounds[0] or bounds[1] < expected_bounds[1]:
            raise UpdateError(f"FuelEconomy {component} model-year coverage shrank")
        if bounds != expected_bounds:
            reasons.append(f"FuelEconomy {component} model-year coverage expanded")
            for key, value in zip(("expected_model_year_from", "expected_model_year_to"), bounds, strict=True):
                owner = component if key in source.component(component).adapter else "vehicles"
                coverage_edits.append((("sources", source_key, "components", owner, "adapter", key), value))
        coverage[component] = {
            "before": old_years if local["status"] == "valid" else None,
            "registered_bounds": list(expected_bounds), "after": years,
        }
        if component == "range_evidence":
            frame = adapter.normalize_range_evidence(frame, rules=rules)
            if previous is not None:
                previous = adapter.normalize_range_evidence(previous, rules=rules)
        else:
            adapter.normalize_vehicle_classes(frame, rules=rules)
        observations[component] = _fuel_record_changes(previous, frame)
        if changed and observations[component]["changed"]:
            reasons.append(f"FuelEconomy {component} selected records changed")
        counts[component] = len(frame)
    propulsion_columns = load_harmonization_rules(bundle, "efficiencies")["ratings"]["evidence_columns"]
    propulsion = adapter.read_configuration_evidence(
        request.model_copy(update={"cache_path": staged}), columns=propulsion_columns,
    )
    previous_propulsion = (
        adapter.read_configuration_evidence(request, columns=propulsion_columns)
        if local["status"] == "valid" else None
    )
    observations["propulsion_configuration"] = _fuel_record_changes(previous_propulsion, propulsion)
    counts["propulsion_configuration"] = len(propulsion)
    if changed and observations["propulsion_configuration"]["changed"]:
        reasons.append("FuelEconomy propulsion configuration selected records changed")
    if changed and local["status"] != "valid":
        reasons.append("Exact registered cache unavailable; previous FuelEconomy records cannot be compared")
    with ZipFile(staged) as archive:
        member_sha = _sha(archive.read(request.archive_member))
    previous_member_sha = None
    if local["status"] == "valid":
        with ZipFile(request.cache_path) as archive:
            previous_member_sha = _sha(archive.read(request.archive_member))
    selected_changed = any(row["changed"] for row in observations.values())
    change_kind = (
        "unchanged" if not changed else
        "unverified_baseline" if local["status"] != "valid" else
        "selected_records" if selected_changed else
        "zip_packaging_only" if previous_member_sha == member_sha else
        "unconsumed_fields_or_csv_formatting"
    )
    base = ("sources", source_key)
    edits, files = list(dict(coverage_edits).items()), {}
    modified = response.headers.get("Last-Modified", "")
    if changed or local["status"] != "valid":
        try:
            version = parsedate_to_datetime(modified).date().isoformat()
        except (TypeError, ValueError, OverflowError):
            version = f"sha256-{sha[:12]}"
            reasons.append("FuelEconomy publisher Last-Modified missing or invalid; review content-addressed snapshot")
        target = _snapshot_path(bundle, request.cache_path, content)
        adapter_base = base + ("components", "vehicles", "adapter")
        edits.extend(
            [
                (adapter_base + ("cache_path",), _relative(bundle, target)),
                (adapter_base + ("expected_sha256",), sha),
                (adapter_base + ("expected_bytes",), len(content)),
                (base + ("version",), f"{version} vehicle snapshot"),
            ]
        )
        for component in source.components:
            label = source.component(component).label
            if isinstance(label, str):
                edits.append(
                    (
                        base + ("components", component, "label"),
                        re.sub(r"\d{4}-\d{2}-\d{2}|sha256-[0-9a-f]{12}", version, label),
                    )
                )
        files[target] = content
    return [
        Candidate(
            source_key,
            "vehicles+range_evidence",
            edits,
            files,
            {
                "url": request.url,
                "last_modified": modified,
                "sha256": sha,
                "publisher_checksum": None,
                "integrity_basis": "official HTTPS bytes; measured SHA-256",
                "bytes_changed": changed,
                "registered_cache": local,
                "rows": counts,
                "change_kind": change_kind,
                "archive_member_sha256": {"before": previous_member_sha, "after": member_sha},
                "model_years": coverage,
                "selected_records": observations,
                "propulsion_columns": propulsion_columns,
                "validation": "ZIP CRC, component columns/nulls, nonshrinking years, VClass vocabulary, range semantics and consumed propulsion configurations",
            },
            list(dict.fromkeys(reasons)),
        )
    ]


def _dunsky(bundle, client, settings, _folder) -> list[Candidate]:
    from fetching import dunsky_charger_inputs as adapter

    source = bundle.sources.sources[adapter.SOURCE]
    request = adapter.ReportRequest.model_validate(source.adapter["diagnostic_report"])
    cache = resolve_input_path(bundle, "cache", request.cache_path)
    response = _response(client, request.acquisition_url, settings, allowed_hosts=source.update.download_hosts)
    content = response.content
    if not content:
        raise UpdateError(f"Dunsky acquisition returned an empty HTTP {response.status_code} response: {request.acquisition_url}")
    sha = _sha(content)
    evidence = adapter.validate_report_content(content, bundle, request)
    local = _cache_identity(cache, request.expected_sha256, expected_bytes=request.expected_bytes)
    changed = (sha, len(content)) != (request.expected_sha256, request.expected_bytes)
    review = []
    if changed:
        if local["status"] != "valid":
            raise UpdateError("Restore the authenticated Dunsky baseline before reviewing different HTML bytes")
        previous = adapter.validate_report_content(cache.read_bytes(), bundle, request)
        if previous["source_sha256"] != request.expected_sha256:
            raise UpdateError("The authenticated Dunsky baseline changed during review")
        for key in ("publication_date", "tables", "context"):
            if evidence[key] != previous[key]:
                raise UpdateError(f"Dunsky report {key} changed. Review the source extraction and modeling inputs explicitly.")
        review.append("Dunsky HTML bytes changed with identical normalized tables and report context")

    edits, files = [], {}
    if changed or local["status"] != "valid":
        target = _snapshot_path(bundle, cache, content) if changed or cache.exists() else cache
        base = ("sources", adapter.SOURCE, "adapter", "diagnostic_report")
        edits = [(base + ("cache_path",), _relative(bundle, target)),
                 (base + ("expected_sha256",), sha), (base + ("expected_bytes",), len(content))]
        files[target] = content
    return [Candidate(adapter.SOURCE, "published_report", edits, files, {
        "publication_url": request.url, "acquisition_url": request.acquisition_url,
        "publication_date": request.publication_date, "bytes": len(content), "sha256": sha,
        "tables": list(evidence["tables"]), "context": list(evidence["context"]),
        "registered_cache": local,
        "same_normalized_evidence": True,
    }, review)]


def _ontario(bundle, client, settings, folder) -> list[Candidate]:
    from fetching import vehicle_population as adapter

    source_key = adapter.SOURCE_ID
    source = bundle.sources.sources[source_key]
    access = source.adapter["access"]
    package = _package(
        client, source.update.metadata_url, access["package_id"], settings
    )
    discovered = adapter.discover_ckan_resources(
        package, selector=access["resource_selector"]
    )
    by_year = {resource.year: resource for resource in discovered}
    raw_by_id = {item["id"]: item for item in package["resources"]}
    requests_by_year = {
        request.year: request
        for request in adapter.registered_requests(bundle, year=None)
    }
    registered = source.adapter["registered_resources"]
    known_years = {item["year"] for item in registered}
    excluded = set(
        adapter.module_rules(bundle)["reports"]["A"].get("excluded_years", [])
    )
    new_years = sorted(set(by_year) - known_years - excluded)
    candidates = []
    for index, previous in enumerate(registered):
        year = previous["year"]
        if year not in requests_by_year or year not in by_year:
            raise UpdateError(
                f"Registered Ontario year {year} is missing or excluded; review coverage"
            )
        discovered_resource = by_year[year]
        resource = CatalogueResource.model_validate(
            raw_by_id[discovered_resource.resource_id]
        )
        content = _verified_bytes(
            client, resource, settings, allowed_hosts=source.update.download_hosts
        )
        old_request = requests_by_year[year]
        candidate_request = old_request.model_copy(
            update={
                "cache_path": _staged(folder, old_request.cache_path, content),
                "expected_sha256": _sha(content),
            }
        )
        reports = adapter.validate_source(
            candidate_request, require_auxiliary=year == max(known_years)
        )
        rows = {}
        rules = adapter.module_rules(bundle)
        for key, member in reports.items():
            raw = adapter.read_report_from_zip(candidate_request.cache_path, member)
            if key == "A":
                normalized, _, _, _ = adapter.normalize_report_a(
                    raw,
                    source_id=source_key,
                    year=year,
                    raw_file=member.display_name,
                    cached_zip=candidate_request.cache_path,
                    rules=rules,
                )
                if (
                    normalized.empty
                    or normalized["STATUS_TOTAL_DIFFERENCE"].fillna(1).ne(0).any()
                ):
                    raise UpdateError(
                        f"Ontario {year} Report A statuses do not reconcile"
                    )
            else:
                required = set(rules["reports"][int(key)]["required_columns"])
                if raw.empty or not required <= set(raw.columns):
                    raise UpdateError(
                        f"Ontario {year} Report {key} columns or rows changed"
                    )
            rows[key] = len(raw)
        changed = _sha(content) != previous["cache_sha256"]
        base = ("sources", source_key, "adapter", "registered_resources", index)
        updates = discovered_resource.model_dump()
        edits = [
            (base + (key,), value) for key, value in updates.items() if key != "year"
        ]
        files = {}
        if changed:
            target = _snapshot_path(bundle, old_request.cache_path, content)
            edits += [
                (base + ("cache_path",), _relative(bundle, target)),
                (base + ("cache_sha256",), _sha(content)),
            ]
            files[target] = content
        candidates.append(
            Candidate(
                source_key,
                str(year),
                edits,
                files,
                {
                    "resource": resource.model_dump(),
                    "metadata_url": source.update.metadata_url,
                    "sha256": _sha(content),
                    "bytes_changed": changed,
                    "report_rows": rows,
                    "new_years_for_review": new_years,
                    "excluded_years": sorted(excluded),
                    "validation": "ZIP CRC, configured reports/columns and Report A status reconciliation",
                },
            )
        )
    # Recheck the same package, including resource identity and URL, before publication.
    if (
        _package(client, source.update.metadata_url, access["package_id"], settings)[
            "resources"
        ]
        != package["resources"]
    ):
        raise UpdateError("Ontario resources changed during the update check")
    return candidates


def _statcan_periods(request, metadata, chunksize):
    """Require the archive's periods to agree with the advertised WDS release."""
    import pandas as pd
    from fetching.statcan_tables import main_csv_member

    periods = set()
    with (
        ZipFile(request.archive_cache_path) as zipped,
        zipped.open(main_csv_member(request)) as handle,
    ):
        for chunk in pd.read_csv(handle, usecols=["REF_DATE"], chunksize=chunksize):
            if chunk["REF_DATE"].isna().any():
                raise UpdateError(
                    f"StatCan {request.table_id} has missing reference periods"
                )
            periods.update(chunk["REF_DATE"].astype(str).unique())
    dates = []
    for period in periods:
        match = re.fullmatch(r"(\d{4})(?:-(?:(\d{2})(?:-(\d{2}))?|Q([1-4])))?", period)
        if match is None:
            raise UpdateError(
                f"StatCan {request.table_id} has an unrecognized reference period: {period}"
            )
        year, month, day, quarter = match.groups()
        dates.append(
            date(
                int(year),
                int(month or (int(quarter) * 3 - 2 if quarter else 1)),
                int(day or 1),
            )
        )
    expected = (
        date.fromisoformat(_version(metadata.get("cubeStartDate"))),
        date.fromisoformat(_version(metadata.get("cubeEndDate"))),
    )
    if not dates or (min(dates), max(dates)) != expected:
        raise UpdateError(
            f"StatCan {request.table_id} archive periods do not match its cube metadata"
        )
    return periods


def _statcan(bundle, client, settings, folder, *, components: set[str] | None = None) -> list[Candidate]:
    from fetching import statcan_tables as adapter
    from zipfile import BadZipFile

    candidates = []
    rules = adapter.module_rules(bundle)
    for request in adapter.build_requests(bundle):
        if components is not None and request.table_id not in components:
            continue
        payload = _json(
            client,
            request.metadata_url,
            settings,
            payload=[{"productId": request.product_id}],
        )
        metadata = adapter.metadata_object(payload, request)
        adapter.validate_metadata_contract(metadata, request)
        release = str(metadata.get("releaseTime") or "")
        _version(release)
        previous, previous_periods, registered_periods, review = None, None, None, []
        baseline = {"status": "unregistered"}
        periods = {key: _version(metadata.get(key)) for key in ("cubeStartDate", "cubeEndDate")}
        if periods["cubeStartDate"] > periods["cubeEndDate"]:
            raise UpdateError(f"StatCan {request.table_id} has reversed reference dates")
        if request.snapshot is not None or request.metadata_cache_path.exists():
            # Only errors in the historical cache become an unknown baseline.
            # Downloaded candidates retain all validation below, outside this block.
            try:
                previous = adapter.read_cached_metadata(request)
                adapter.validate_metadata_contract(previous, request)
                previous_periods = {key: _version(previous.get(key)) for key in periods}
                if previous_periods["cubeStartDate"] > previous_periods["cubeEndDate"]:
                    raise UpdateError("Historical StatCan reference dates are reversed")
                if request.snapshot is not None:
                    adapter.validate_source(request)
                    with ZipFile(request.archive_cache_path) as zipped:
                        if zipped.testzip() is not None:
                            raise UpdateError("Historical StatCan ZIP failed CRC")
                    registered_periods = _statcan_periods(request, previous, int(rules["read_chunksize"]))
                baseline = {"status": "authenticated" if request.snapshot is not None else "legacy_metadata"}
            except (OSError, ValueError, TypeError, BadZipFile) as exc:
                previous, previous_periods, registered_periods = None, None, None
                baseline = {"status": "unavailable", "reason": str(exc)}
                review.append("Registered StatCan baseline unavailable or invalid; historical coverage cannot be compared")
        if previous is not None:
            if previous.get("frequencyCode") != metadata.get("frequencyCode"):
                raise UpdateError(f"StatCan {request.table_id} frequencyCode changed; review frequency")
            if (periods["cubeStartDate"] > previous_periods["cubeStartDate"]
                or periods["cubeEndDate"] < previous_periods["cubeEndDate"]):
                raise UpdateError(f"StatCan {request.table_id} registered reference coverage shrank")
            if previous_periods != periods:
                review.append("StatCan reference-period coverage expanded")
        resolved = _json(client, request.download_api_url, settings)
        if (
            not isinstance(resolved, dict)
            or resolved.get("status") != "SUCCESS"
            or not isinstance(resolved.get("object"), str)
        ):
            raise UpdateError(f"StatCan full-table resolver failed: {request.table_id}")
        url = resolved["object"]
        if urlparse(url).hostname != urlparse(request.download_api_url).hostname:
            raise UpdateError(f"StatCan archive moved to another host: {url}")
        archive = _response(client, url, settings).content
        # Same canonical bytes are used by the adopted runtime metadata fetch seam.
        metadata_bytes = (
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
        ).encode("utf-8")
        staged_request = request.model_copy(
            update={
                "archive_cache_path": _staged(
                    folder, request.archive_cache_path, archive
                ),
                "metadata_cache_path": _staged(
                    folder, request.metadata_cache_path, metadata_bytes
                ),
                "snapshot": None,
            }
        )
        adapter.validate_source(staged_request)
        with ZipFile(staged_request.archive_cache_path) as zipped:
            if zipped.testzip() is not None:
                raise UpdateError(f"StatCan {request.table_id} ZIP failed CRC")
        actual_periods = _statcan_periods(staged_request, metadata, int(rules["read_chunksize"]))
        if registered_periods is not None:
            if missing := registered_periods - actual_periods:
                raise UpdateError(f"StatCan {request.table_id} lost registered reference periods: {sorted(missing)}")
        selected, _, rows = adapter.read_selected_table(
            staged_request,
            regions=list(bundle.scenario.geography.regions),
            geography_rules=rules["geography"],
            chunksize=int(rules["read_chunksize"]),
        )
        if selected.empty or selected["REF_DATE"].isna().any():
            raise UpdateError(f"StatCan {request.table_id} selected coverage is empty")
        if (
            _json(
                client,
                request.metadata_url,
                settings,
                payload=[{"productId": request.product_id}],
            )
            != payload
        ):
            raise UpdateError(
                f"StatCan release changed during download: {request.table_id}"
            )
        target_zip = _snapshot_path(bundle, request.archive_cache_path, archive)
        target_metadata = _snapshot_path(
            bundle, request.metadata_cache_path, metadata_bytes
        )
        snapshot = {
            "archive_cache_path": _relative(bundle, target_zip),
            "metadata_cache_path": _relative(bundle, target_metadata),
            "archive_sha256": _sha(archive),
            "metadata_sha256": _sha(metadata_bytes),
            "release_time": release,
        }
        base = (
            "sources",
            adapter.SOURCE_ID,
            "components",
            request.table_id,
            "adapter",
            "snapshot",
        )
        candidates.append(
            Candidate(
                adapter.SOURCE_ID,
                request.table_id,
                [(base, snapshot)],
                {target_zip: archive, target_metadata: metadata_bytes},
                {
                    "url": url,
                    "metadata_url": request.metadata_url,
                    "release_time": release,
                    "archive_sha256": _sha(archive),
                    "metadata_sha256": _sha(metadata_bytes),
                    "rows": rows,
                    "selected_rows": len(selected),
                    "registered_baseline": baseline,
                    "reference_periods": {"before": previous_periods, "after": periods},
                    "archive_periods": {"before": sorted(registered_periods) if registered_periods is not None else None,
                                        "after": sorted(actual_periods)},
                    "frequency_code": metadata.get("frequencyCode"),
                    "validation": "coherent release, PID/title/dimensions/members, ZIP CRC and selected CSV coverage",
                },
                review,
            )
        )
    return candidates


def _cer(bundle, client, settings, folder) -> list[Candidate]:
    from fetching import cer_enerfuture as adapter

    source_key = adapter.SOURCE_ID
    source = bundle.sources.sources[source_key]
    # Only the scenario-selected, already registered edition is considered.
    requests_to_make = adapter.build_requests(bundle)
    package_id = requests_to_make[0].open_government_record_id
    package = _package(client, source.update.metadata_url, package_id, settings)
    edition = requests_to_make[0].edition
    snapshots = copy.deepcopy(source.adapter.get("registered_snapshots", {}))
    previous_edition = snapshots.get(edition, snapshots.get(str(edition), {}))
    edition_snapshots = copy.deepcopy(previous_edition)
    files, details = {}, []
    for request in requests_to_make:
        filename = Path(urlparse(request.url).path).name
        matches = [
            item
            for item in package["resources"]
            if Path(urlparse(str(item.get("url", ""))).path).name == filename
            and str(item.get("format", "")).casefold() == "csv"
        ]
        if len(matches) != 1:
            raise UpdateError(
                f"CER {edition}/{request.component_id} resource is missing or ambiguous"
            )
        resource = CatalogueResource.model_validate(matches[0])
        if urlparse(resource.url).hostname != urlparse(request.url).hostname:
            raise UpdateError(f"CER resource moved to another host: {resource.url}")
        content = _verified_bytes(
            client, resource, settings, allowed_hosts=source.update.download_hosts
        )
        staged_request = request.model_copy(
            update={
                "cache_path": _staged(folder, request.cache_path, content),
                "expected_sha256": None,
                "expected_bytes": None,
            }
        )
        frame = adapter.read_and_validate_source(staged_request)
        selected = adapter.normalize_component(
            frame,
            staged_request,
            region_labels=adapter.scenario_region_labels(bundle),
            rules=adapter.module_rules(bundle),
        )
        if selected.empty:
            raise UpdateError(
                f"CER {edition}/{request.component_id} selected coverage is empty"
            )
        target = _snapshot_path(bundle, request.cache_path, content)
        edition_snapshots[request.component_id] = {
            "url": resource.url,
            "cache_path": _relative(bundle, target),
            "expected_sha256": _sha(content),
            "expected_bytes": len(content),
        }
        files[target] = content
        details.append(
            {
                "component": request.component_id,
                "resource": resource.model_dump(),
                "sha256": _sha(content),
                "rows": len(frame),
                "selected_rows": len(selected),
            }
        )
    edition_key = (
        edition
        if edition in snapshots or str(edition) not in snapshots
        else str(edition)
    )
    snapshots[edition_key] = edition_snapshots
    if (
        _package(client, source.update.metadata_url, package_id, settings)["resources"]
        != package["resources"]
    ):
        raise UpdateError("CER catalogue changed during download")
    return [
        Candidate(
            source_key,
            str(edition),
            [(("sources", source_key, "adapter", "registered_snapshots"), snapshots)],
            files,
            {
                "metadata_url": source.update.metadata_url,
                "package_id": package_id,
                "resources": details,
                "validation": "same edition, source columns/scenarios/year bounds and selected physical quantities",
            },
        )
    ]


def _value_at(data, path):
    try:
        for key in path:
            data = data[key]
        return data
    except (KeyError, IndexError, TypeError):
        return None


def _publish(
    bundle, original: bytes, updated: str, files: dict[Path, bytes]
) -> list[str]:
    """Create immutable caches, then publish the registry; roll back new files on failure."""
    lock = bundle.sources_path.with_name(f".{bundle.sources_path.name}.update.lock")
    created = []
    committed = False
    warnings = []
    # Windows rename and POSIX hard links are atomic and fail if the destination exists.
    publish_snapshot = os.rename if os.name == "nt" else os.link
    # An updater lock and an optimistic byte check also protect against concurrent edits.
    lock_handle = lock.open("xb")
    try:
        if bundle.sources_path.read_bytes() != original:
            raise UpdateError(
                "Source registry changed during review; rerun the updater"
            )
        for path, content in files.items():
            expected = _sha(content)
            if path.exists():
                if not path.is_file() or file_sha256(path) != expected:
                    raise UpdateError(f"Refusing to replace a conflicting cache: {path}")
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(
                    dir=path.parent, delete=False, suffix=".tmp"
                ) as handle:
                    temporary = Path(handle.name)
                    handle.write(content)
                try:
                    publish_snapshot(temporary, path)
                except FileExistsError as exc:
                    if not path.is_file() or file_sha256(path) != expected:
                        raise UpdateError(f"Refusing to replace a conflicting cache: {path}") from exc
                else:
                    created.append(path)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        if bundle.sources_path.read_bytes() != original:
            raise UpdateError(
                "Source registry changed before publication; rerun the updater"
            )
        if updated.encode("utf-8") != original:
            write_text_atomic(updated, bundle.sources_path, newline="")
        committed = True
    except BaseException:
        for path in reversed(created):
            path.unlink(missing_ok=True)
        raise
    finally:
        for cleanup in (lock_handle.close, lock.unlink):
            try:
                cleanup()
            except OSError as exc:
                if not committed:
                    raise
                warnings.append(
                    f"Registry was applied, but updater lock cleanup failed: {exc}"
                )
    return warnings


def _update_session() -> requests.Session:
    # The updater's POST requests are read-only catalogue queries.
    retries = Retry(total=3, allowed_methods={"GET", "POST"},
                    status_forcelist=(429, 500, 502, 503, 504),
                    backoff_factor=0.5, backoff_max=10, retry_after_max=10,
                    raise_on_status=False)
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _update_rule_fingerprints(bundle: ConfigBundle, sources: set[str] | None) -> dict[str, str]:
    fingerprints = harmonization_rule_fingerprints(bundle)
    key = "dunsky_ev_charging_infrastructure"
    source = bundle.sources.sources.get(key)
    if source is not None and source.update is not None and (sources is None or key in sources):
        path = resolve_parameter_path(bundle, "dunsky_charger_inputs.yaml")
        fingerprints[path.name] = file_sha256(path)
    return fingerprints


def run_update(
    bundle: ConfigBundle,
    *,
    sources: set[str] | None = None,
    components: set[str] | None = None,
    apply: bool = False,
    session: requests.Session | None = None,
    review_report: Path | None = None,
) -> UpdateRun:
    """Inspect every selected policy and persist the exact proposed/adopted diff."""
    timestamp = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "_" + uuid4().hex[:8]
    )
    audit = resolve_artifact_path(bundle, "source_update_audits", timestamp)
    audit.mkdir(parents=True, exist_ok=False)
    report_path, diff_path = audit / "report.json", audit / "sources.diff"
    log_path = resolve_artifact_path(bundle, "source_update_logs", f"{timestamp}.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"source_update.{timestamp}")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    original = bundle.sources_path.read_bytes()
    text = original.decode("utf-8")
    registry = yaml.load(text, Loader=UniqueKeyLoader)
    rule_fingerprints = _update_rule_fingerprints(bundle, sources)
    report = {
        "review_schema_version": 3,
        "mode": "apply" if apply else "report",
        "status": "checking",
        "started_utc": timestamp,
        "registry": str(bundle.sources_path),
        "registry_sha256_before": _sha(original),
        "scenario": str(bundle.scenario_path),
        "scenario_sha256": file_sha256(bundle.scenario_path),
        "rules_sha256": _sha(json.dumps(rule_fingerprints, sort_keys=True, separators=(",", ":")).encode("utf-8")),
        "rules_fingerprints": rule_fingerprints,
        "selected_sources": sorted(sources) if sources is not None else None,
        "selected_components": sorted(components) if components is not None else None,
        "sources": [],
        "candidates": [],
        "acquisitions": [],
        "errors": [],
    }
    errors = report["errors"]
    applied = False
    owned_client = session is None
    network_client = session or _update_session()
    client = _AcquisitionSession(network_client, audit)
    try:
        if review_report is not None and not apply:
            raise UpdateError("--review-report requires --apply")
        if review_report is not None:
            reviewed = json.loads(Path(review_report).read_text(encoding="utf-8"))
            if reviewed.get("review_schema_version") != 3:
                raise UpdateError("Reviewed report predates the complete rules-file identity contract; "
                                  "run report-only review again with the current rules configuration")
            if (reviewed.get("status") != "report_only" or reviewed.get("errors")
                or reviewed.get("review_fingerprint") != _review_fingerprint(reviewed)
                or any(reviewed.get(key) != report[key] for key in (
                    "registry_sha256_before", "scenario_sha256", "rules_sha256", "rules_fingerprints",
                    "selected_sources",
                    "selected_components",
                ))):
                raise UpdateError("Reviewed report no longer matches this registry, scenario, rules or source/component selection, or its evidence changed; run report-only review again")
            client = _AcquisitionSession(
                None, Path(review_report).resolve().parent,
                acquisitions=reviewed["acquisitions"],
            )
            logger.info("REPLAYING exact reviewed downloads without network: %s", review_report)
        report["acquisitions"] = client.acquisitions
        settings = bundle.sources.updater
        if settings is None:
            raise UpdateError(
                "Source updater settings must be configured in sources.yaml"
            )
        if sources is not None and (
            not sources or (unknown := sources - set(bundle.sources.sources))
        ):
            raise UpdateError(
                f"Unknown or empty source selection: {sorted(unknown) if sources else []}"
            )
        if components is not None:
            if not components or sources is None or len(sources) != 1:
                raise UpdateError("Component selection must be nonempty and requires exactly one source")
            selected_source = next(iter(sources))
            if selected_source != "statcan_transport_tables":
                raise UpdateError("Component selection is supported only for statcan_transport_tables")
            if unknown_components := components - set(bundle.sources.sources[selected_source].components):
                raise UpdateError(f"Unknown component selection for {selected_source}: {sorted(unknown_components)}")
        dispatch = {
            "nrcan_ratings": ("nrcan_fuel_consumption_ratings", _nrcan),
            "tc_izev": ("transport_canada_izev", _tc_izev),
            "ontario_population": (
                "ontario_ministry_transport_vehicle_population",
                _ontario,
            ),
            "statcan_tables": ("statcan_transport_tables", _statcan),
            "cer_energy_future": ("cer_canadas_energy_future", _cer),
            "fueleconomy": ("fueleconomy_gov_vehicle_data", _fueleconomy),
            "dunsky_report": ("dunsky_ev_charging_infrastructure", _dunsky),
        }
        candidates = []
        with tempfile.TemporaryDirectory(prefix="source_update_") as directory:
            folder = Path(directory)
            for key, source in bundle.sources.sources.items():
                reason = None
                if sources is not None and key not in sources:
                    reason = "not selected"
                elif source.status != "active":
                    reason = (
                        "inactive research/reference source; no production adoption"
                    )
                elif source.update is None:
                    reason = source.refresh_notes or "No automatic update policy is registered; use the source's explicit acquisition or manual refresh procedure"
                if reason:
                    report["sources"].append(
                        {"source": key, "status": "skipped", "reason": reason}
                    )
                    logger.info("SKIPPED %s: %s", key, reason)
                    if sources is not None and key in sources:
                        errors.append(
                            f"Selected source {key} is not eligible for automated updates: {reason}"
                        )
                    continue
                expected_source, collect = dispatch[source.update.strategy]
                if expected_source != key:
                    raise UpdateError(
                        f"Update strategy {source.update.strategy} cannot handle {key}"
                    )
                logger.info("CHECKING %s (%s)", key, source.update.strategy)
                try:
                    found = (collect(bundle, client, settings, folder, components=components)
                             if components is not None else collect(bundle, client, settings, folder))
                    if not found:
                        raise UpdateError(f"No registered update candidates for {key}")
                    if components is not None:
                        if {candidate.component for candidate in found} != components or len(found) != len(components):
                            raise UpdateError("Updater candidates differ from the selected components")
                        for candidate in found:
                            prefix = ("sources", key, "components", candidate.component)
                            if candidate.source != key or any(path[:len(prefix)] != prefix for path, _ in candidate.edits):
                                raise UpdateError("A selected component proposed registry edits outside its component")
                    candidates.extend(found)
                    report["sources"].append(
                        {"source": key, "status": "validated", "candidates": len(found)}
                    )
                except Exception as exc:
                    errors.append(f"{key}: {exc}")
                    report["sources"].append({
                        "source": key, "status": "error", "reason": str(exc),
                        "evidence": getattr(exc, "evidence", {}),
                    })
                    logger.exception("ERROR %s: %s", key, exc)
            edits, files = [], {}
            for candidate in candidates:
                effective = [
                    (path, value)
                    for path, value in candidate.edits
                    if _value_at(registry, path) != value
                ]
                missing_files = any(not path.exists() for path in candidate.files)
                status = "changed" if effective else "cache_recovery" if missing_files else "current"
                edits.extend(effective)
                for path, content in candidate.files.items():
                    if path in files and files[path] != content:
                        raise UpdateError(f"Conflicting candidate cache: {path}")
                    files[path] = content
                row = {
                    "source": candidate.source,
                    "component": candidate.component,
                    "status": status,
                    "changes": [
                        {
                            "path": list(path),
                            "before": _value_at(registry, path),
                            "after": value,
                        }
                        for path, value in effective
                    ],
                    "evidence": candidate.evidence,
                    "review_required": candidate.review_required,
                }
                report["candidates"].append(row)
                logger.info(
                    "%s %s/%s %s",
                    status.upper(),
                    candidate.source,
                    candidate.component,
                    json.dumps(row, ensure_ascii=False, sort_keys=True),
                )
            updated = edit_registry(text, edits)
            for path, expected in (
                (bundle.scenario_path, report["scenario_sha256"]),
            ):
                if file_sha256(path) != expected:
                    errors.append(f"Configuration changed during source review: {path}; rerun report-only review")
            if _update_rule_fingerprints(bundle, sources) != report["rules_fingerprints"]:
                errors.append("Rules configuration changed during source review; rerun report-only review")
            diff = "".join(
                difflib.unified_diff(
                    text.splitlines(keepends=True),
                    updated.splitlines(keepends=True),
                    fromfile="registered sources.yaml",
                    tofile="proposed sources.yaml",
                )
            )
            write_text_atomic(diff, diff_path, newline="")
            report.update(
                status="blocked" if errors else "reviewed",
                registry_sha256_proposed=_sha(updated.encode("utf-8")),
                registry_diff=diff,
                cache_snapshots={
                    str(path): _sha(content) for path, content in files.items()
                },
            )
            report["review_fingerprint"] = _review_fingerprint(report)
            needs_review = any(candidate.review_required for candidate in candidates)
            if review_report is not None:
                if (client.position != len(client.acquisitions)
                    or reviewed["review_fingerprint"] != report["review_fingerprint"]):
                    errors.append("Reviewed report no longer matches revalidated candidates or registered caches; run report-only review again")
                else:
                    report["reviewed_report"] = str(Path(review_report).resolve())
                    # Preserve a self-contained apply audit too.
                    for entry in client.acquisitions:
                        target = audit / entry["file"]
                        target.parent.mkdir(parents=True, exist_ok=True)
                        if not target.exists():
                            target.write_bytes((client.folder / entry["file"]).read_bytes())
            elif apply and needs_review:
                errors.append("Changed observations or publication identity require --review-report <report.json> from a matching report-only run")
            # Durable evidence precedes any adoption, including full source replacement diffs.
            write_text_atomic(
                json.dumps(report, indent=2, ensure_ascii=False) + "\n",
                report_path,
                newline="\n",
            )
            logger.info("REGISTRY DIFF\n%s", diff or "(no changes)")
            handler.flush()
            if apply and not errors and (edits or any(not path.exists() for path in files)):
                for path, expected in (
                    (bundle.scenario_path, report["scenario_sha256"]),
                ):
                    if file_sha256(path) != expected:
                        raise UpdateError(f"Configuration changed before publication: {path}")
                if _update_rule_fingerprints(bundle, sources) != report["rules_fingerprints"]:
                    raise UpdateError("Rules configuration changed before publication; rerun report-only review")
                warnings = _publish(bundle, original, updated, files)
                applied = True
                report["warnings"] = warnings
                for warning in warnings:
                    logger.warning("%s", warning)
                for candidate in report["candidates"]:
                    if candidate["status"] in {"changed", "cache_recovery"}:
                        candidate["status"] = "applied"
                logger.info(
                    "APPLIED validated batch; rebuild and review model outputs before committing"
                )
    except Exception as exc:
        errors.append(str(exc))
        logger.exception("FAILED source update: %s", exc)
    finally:
        report["status"] = (
            "failed"
            if errors
            else "applied"
            if applied
            else "report_only"
            if not apply
            else "current"
        )
        report["applied"] = applied
        report["registry_sha256_after"] = file_sha256(bundle.sources_path)
        if not diff_path.exists():
            write_text_atomic("", diff_path, newline="\n")
        write_text_atomic(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n",
            report_path,
            newline="\n",
        )
        logger.info(
            "FINISHED %s; report=%s; log=%s", report["status"], report_path, log_path
        )
        logger.removeHandler(handler)
        handler.close()
        if owned_client:
            network_client.close()
    return UpdateRun(report_path, log_path, diff_path, applied, tuple(errors))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--review-report",
        type=Path,
        help="with --apply, revalidate and adopt an exact captured report-only proposal without downloading again",
    )
    parser.add_argument(
        "--scenario",
        required=True,
        help="scenario YAML supplying source/period selections",
    )
    parser.add_argument(
        "--source",
        action="append",
        help="registry source key (repeatable); otherwise inspect all eligible sources",
    )
    parser.add_argument(
        "--component",
        action="append",
        help="StatCan table component key (repeatable); requires exactly one --source",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="explicitly adopt the complete validated selection",
    )
    args = parser.parse_args()
    if args.component and (not args.source or len(args.source) != 1):
        parser.error("--component requires exactly one --source")
    result = run_update(
        load_config_bundle(args.scenario),
        sources=set(args.source) if args.source else None,
        components=set(args.component) if args.component else None,
        apply=args.apply,
        review_report=args.review_report,
    )
    print(
        f"Source update {'applied' if result.applied else 'not applied'}; report: {result.report_path}"
    )
    print(f"Diff: {result.diff_path}\nLog: {result.log_path}")
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    for candidate in report["candidates"]:
        if candidate.get("review_required"):
            print(f"REVIEW {candidate['source']}: {'; '.join(candidate['review_required'])}")
    if not args.apply and any(row.get("review_required") for row in report["candidates"]):
        print("Inspect the evidence and diff; to adopt this exact proposal, rerun with --apply --review-report <report.json>.")
    if result.errors:
        for error in result.errors:
            print(f"ERROR: {error}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
