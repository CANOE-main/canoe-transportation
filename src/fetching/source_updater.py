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
from yaml.nodes import MappingNode, ScalarNode, SequenceNode

from utils import (
    ConfigBundle,
    UniqueKeyLoader,
    file_sha256,
    load_config_bundle,
    resolve_artifact_path,
    resolve_input_path,
    write_text_atomic,
)
from validation.config_models import SourcesConfig


class UpdateError(ValueError):
    """A candidate cannot safely replace the registered snapshot."""


class CatalogueResource(BaseModel):
    """Only shared CKAN transport fields; source-native contracts stay with adapters."""

    model_config = ConfigDict(extra="ignore")
    id: str = Field(min_length=1)
    url: str = Field(pattern=r"^https://[^\s]+$")
    name: str = ""
    format: str = ""
    hash: str = ""
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


@dataclass(frozen=True)
class UpdateRun:
    report_path: Path
    log_path: Path
    diff_path: Path
    applied: bool
    errors: tuple[str, ...]


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


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


def _url(value: str) -> str:
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
        raise UpdateError(f"Expected an official HTTPS source URL: {value}")
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
    response = method(_url(url), stream=True, **kwargs)
    try:
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
    finally:
        response.close()
    return response


def _json(client, url: str, settings, *, payload=None, params=None):
    return _response(client, url, settings, payload=payload, params=params).json()


def _resource(client, endpoint: str, resource_id: str, settings) -> CatalogueResource:
    payload = _json(client, endpoint, settings, params={"id": resource_id})
    if not isinstance(payload, dict) or payload.get("success") is not True:
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
    stem = re.sub(r"-[0-9a-f]{16}$", "", stem)
    return original.with_name(f"{stem}-{_sha(content)[:16]}{suffix}")


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
        rows = len(adapter.normalize_rating_dataframe(raw, candidate_request))
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
                    "validation": "publisher MD5/size, required columns, exact model-year coverage",
                },
            )
        )
    return candidates


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
    counts = {}
    for component in source.components:
        component_request = adapter.build_request(bundle, component).model_copy(
            update={
                "cache_path": staged,
                "expected_sha256": sha,
                "expected_bytes": len(content),
            }
        )
        adapter.validate_cache(component_request)
        frame = adapter.read_selected_vehicle_columns(component_request)
        if component == "range_evidence":
            frame = adapter.normalize_range_evidence(
                frame, rules=adapter.module_rules(bundle)
            )
        counts[component] = len(frame)
    base = ("sources", source_key)
    edits, files = [], {}
    modified = response.headers.get("Last-Modified", "")
    local = _cache_identity(
        request.cache_path,
        request.expected_sha256,
        expected_bytes=request.expected_bytes,
    )
    if changed or local["status"] != "valid":
        if not modified:
            raise UpdateError(
                "FuelEconomy release has no Last-Modified header; review manually"
            )
        version = parsedate_to_datetime(modified).date().isoformat()
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
                        re.sub(r"\d{4}-\d{2}-\d{2}", version, label),
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
                "validation": "ZIP CRC, both component columns/nulls/year bounds and range semantics",
            },
        )
    ]


def _dashboard_frames(bundle, content):
    from fetching import assorted_sources as adapter

    rules = adapter.module_rules(bundle)["tc_ev_dashboard"]
    year = bundle.scenario.sources.selections["transport_canada_ev_dashboard"].year
    html = content.decode("utf-8")
    shares = adapter.normalize_tc_ev_dashboard_table(
        html,
        table_id=rules["table_id"],
        heading_id=rules["heading_id"],
        year=year,
        quarter_header=rules["quarter_header"],
        ytd_header=rules["ytd_header"],
    )
    charger = rules["public_chargers"]
    provincial, types = adapter.normalize_tc_public_chargers(
        html,
        year=year,
        heading_id=charger["heading_id"],
        chart_table_id=charger["chart_table_id"],
        province_table_id=charger["province_table_id"],
        as_of=charger["as_of"],
        regions=charger["source_regions"],
    )
    return shares, provincial, types


def _tc_dashboard(bundle, client, settings, folder) -> list[Candidate]:
    source_key = "transport_canada_ev_dashboard"
    source = bundle.sources.sources[source_key]
    url = str(source.adapter["access"]["url"])
    content = _response(
        client, url, settings, allowed_hosts=source.update.download_hosts
    ).content
    sha = _sha(content)
    changed = (
        sha != source.adapter["expected_sha256"]
        or len(content) != source.adapter["expected_bytes"]
    )
    frames = _dashboard_frames(bundle, content)
    base = ("sources", source_key, "adapter")
    edits, files = [], {}
    old = resolve_input_path(bundle, "cache", source.adapter["cache_path"])
    local = _cache_identity(
        old,
        source.adapter["expected_sha256"],
        expected_bytes=source.adapter["expected_bytes"],
    )
    if changed:
        if local["status"] != "valid":
            raise UpdateError(
                "TC HTML republish requires the exact old cache to prove unchanged observations"
            )
        previous = _dashboard_frames(bundle, old.read_bytes())
        if not all(a.equals(b) for a, b in zip(frames, previous, strict=True)):
            raise UpdateError(
                "TC observations or reporting periods changed; source review is required"
            )
    if changed or local["status"] != "valid":
        target = _snapshot_path(bundle, old, content)
        files[target] = content
        edits.extend(
            [
                (base + ("cache_path",), _relative(bundle, target)),
                (base + ("expected_sha256",), sha),
                (base + ("expected_bytes",), len(content)),
            ]
        )
    return [
        Candidate(
            source_key,
            "dashboard",
            edits,
            files,
            {
                "url": url,
                "sha256": sha,
                "bytes_changed": changed,
                "registered_cache": local,
                "validation": "same-period market shares and provincial/type charger observations unchanged",
                "rows": [len(frame) for frame in frames],
            },
        )
    ]


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


def _statcan(bundle, client, settings, folder) -> list[Candidate]:
    from fetching import statcan_tables as adapter

    candidates = []
    rules = adapter.module_rules(bundle)
    for request in adapter.build_requests(bundle):
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
        if request.metadata_cache_path.is_file():
            previous = adapter.read_cached_metadata(request)
            for key in ("cubeStartDate", "cubeEndDate", "frequencyCode"):
                if previous.get(key) != metadata.get(key):
                    raise UpdateError(
                        f"StatCan {request.table_id} {key} changed; review periods/frequency"
                    )
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
        _statcan_periods(staged_request, metadata, int(rules["read_chunksize"]))
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
                    "validation": "coherent release, PID/title/dimensions/members, ZIP CRC and selected CSV coverage",
                },
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
    for path, content in files.items():
        if path.exists() and (not path.is_file() or file_sha256(path) != _sha(content)):
            raise UpdateError(f"Refusing to replace a conflicting cache: {path}")
    lock = bundle.sources_path.with_name(f".{bundle.sources_path.name}.update.lock")
    created = []
    committed = False
    warnings = []
    # An updater lock and an optimistic byte check also protect against concurrent edits.
    lock_handle = lock.open("xb")
    try:
        if bundle.sources_path.read_bytes() != original:
            raise UpdateError(
                "Source registry changed during review; rerun the updater"
            )
        for path, content in files.items():
            if path.exists():
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(
                    dir=path.parent, delete=False, suffix=".tmp"
                ) as handle:
                    temporary = Path(handle.name)
                    handle.write(content)
                # Atomic no-clobber publication. A same-volume hard link works on
                # Windows and POSIX, and never overwrites a competing snapshot.
                os.link(temporary, path)
                created.append(path)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        if bundle.sources_path.read_bytes() != original:
            raise UpdateError(
                "Source registry changed before publication; rerun the updater"
            )
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


def run_update(
    bundle: ConfigBundle,
    *,
    sources: set[str] | None = None,
    apply: bool = False,
    session: requests.Session | None = None,
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
    report = {
        "mode": "apply" if apply else "report",
        "status": "checking",
        "started_utc": timestamp,
        "registry": str(bundle.sources_path),
        "registry_sha256_before": _sha(original),
        "scenario": str(bundle.scenario_path),
        "sources": [],
        "candidates": [],
        "errors": [],
    }
    errors = report["errors"]
    applied = False
    owned_client = session is None
    client = session or requests.Session()
    try:
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
        dispatch = {
            "nrcan_ratings": ("nrcan_fuel_consumption_ratings", _nrcan),
            "ontario_population": (
                "ontario_ministry_transport_vehicle_population",
                _ontario,
            ),
            "statcan_tables": ("statcan_transport_tables", _statcan),
            "cer_energy_future": ("cer_canadas_energy_future", _cer),
            "fueleconomy": ("fueleconomy_gov_vehicle_data", _fueleconomy),
            "tc_dashboard_same_values": (
                "transport_canada_ev_dashboard",
                _tc_dashboard,
            ),
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
                    reason = source.refresh_notes
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
                    found = collect(bundle, client, settings, folder)
                    if not found:
                        raise UpdateError(f"No registered update candidates for {key}")
                    candidates.extend(found)
                    report["sources"].append(
                        {"source": key, "status": "validated", "candidates": len(found)}
                    )
                except Exception as exc:
                    errors.append(f"{key}: {exc}")
                    report["sources"].append(
                        {"source": key, "status": "error", "reason": str(exc)}
                    )
                    logger.exception("ERROR %s: %s", key, exc)
            edits, files = [], {}
            for candidate in candidates:
                effective = [
                    (path, value)
                    for path, value in candidate.edits
                    if _value_at(registry, path) != value
                ]
                status = "changed" if effective else "current"
                edits.extend(effective)
                if effective:
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
            # Durable evidence precedes any adoption, including full source replacement diffs.
            write_text_atomic(
                json.dumps(report, indent=2, ensure_ascii=False) + "\n",
                report_path,
                newline="\n",
            )
            logger.info("REGISTRY DIFF\n%s", diff or "(no changes)")
            handler.flush()
            if apply and not errors and edits:
                warnings = _publish(bundle, original, updated, files)
                applied = True
                report["warnings"] = warnings
                for warning in warnings:
                    logger.warning("%s", warning)
                for candidate in report["candidates"]:
                    if candidate["status"] == "changed":
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
            client.close()
    return UpdateRun(report_path, log_path, diff_path, applied, tuple(errors))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
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
        "--apply",
        action="store_true",
        help="explicitly adopt the complete validated selection",
    )
    args = parser.parse_args()
    result = run_update(
        load_config_bundle(args.scenario),
        sources=set(args.source) if args.source else None,
        apply=args.apply,
    )
    print(
        f"Source update {'applied' if result.applied else 'not applied'}; report: {result.report_path}"
    )
    print(f"Diff: {result.diff_path}\nLog: {result.log_path}")
    if result.errors:
        for error in result.errors:
            print(f"ERROR: {error}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
