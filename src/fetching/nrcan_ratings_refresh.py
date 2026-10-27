"""Review, and on request adopt, newer NRCan Fuel Consumption Ratings releases.

Builds stay pinned to the snapshots registered in config/sources.yaml. This command
compares each pinned resource with what the Open Government Portal serves now
(CKAN ``resource_show``: size, MD5 and last-modified date). It changes nothing
unless ``--apply`` is given. With ``--apply``, each changed resource is downloaded,
verified against the portal's own MD5 and size, and checked against the registered
column and model-year contracts; only then is the new snapshot written to the cache
and that component's version, label date, cache path, MD5 and byte count rewritten
in sources.yaml. Review and commit that diff like any other input change.
"""

from __future__ import annotations

import argparse
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from fetching.nrcan_ceud import (
    FuelConsumptionRatingRequest,
    bytes_md5,
    iter_rating_requests,
    normalize_rating_dataframe,
    ratings_rules,
    read_rating_csv,
)
from utils import ConfigBundle, load_config_bundle

RESOURCE_SHOW_URL = "https://open.canada.ca/data/api/3/action/resource_show"
DEFAULT_SCENARIO = "config/scenarios/default.yaml"


@dataclass(frozen=True)
class RatingRefreshStatus:
    """Pinned registry values against the portal's current resource metadata."""

    component_key: str
    pinned_version: str
    pinned_bytes: int
    pinned_md5: str
    portal_version: str
    portal_bytes: int
    portal_md5: str

    @property
    def changed(self) -> bool:
        return (self.portal_md5, self.portal_bytes) != (self.pinned_md5, self.pinned_bytes)


def portal_resource(resource_id: str, *, timeout: int = 60) -> dict[str, Any]:
    """Return CKAN resource metadata for one Open Government Portal resource."""
    response = requests.get(RESOURCE_SHOW_URL, params={"id": resource_id}, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success") or not isinstance(payload.get("result"), dict):
        raise ValueError(f"Open Government Portal resource_show failed for {resource_id}")
    return payload["result"]


def refresh_status(
    request: FuelConsumptionRatingRequest, resource: dict[str, Any]
) -> RatingRefreshStatus:
    """Compare one pinned request with the portal's metadata for the same resource."""
    if resource.get("id") != request.resource_id:
        raise ValueError(f"{request.component_key}: portal returned a different resource")
    portal_md5 = str(resource.get("hash") or "").lower()
    if not re.fullmatch(r"[0-9a-f]{32}", portal_md5):
        raise ValueError(
            f"{request.component_key}: the portal publishes no MD5 for this resource; "
            "review it by hand as described in the source's refresh_notes"
        )
    modified = str(resource.get("last_modified") or resource.get("metadata_modified") or "")
    if not re.match(r"\d{4}-\d{2}-\d{2}", modified):
        raise ValueError(f"{request.component_key}: the portal reports no modification date")
    return RatingRefreshStatus(
        component_key=request.component_key,
        pinned_version=str(request.component_meta.version or ""),
        pinned_bytes=request.expected_bytes,
        pinned_md5=request.expected_md5,
        portal_version=modified[:10],
        portal_bytes=int(resource["size"]),
        portal_md5=portal_md5,
    )


def download_verified(
    request: FuelConsumptionRatingRequest, status: RatingRefreshStatus, *, timeout: int = 60
) -> bytes:
    """Download the current resource and require the portal's own size and MD5."""
    response = requests.get(request.url, timeout=timeout)
    response.raise_for_status()
    content = response.content
    if len(content) != status.portal_bytes or bytes_md5(content) != status.portal_md5:
        raise ValueError(
            f"{request.component_key}: download ({len(content)} bytes, MD5 "
            f"{bytes_md5(content)}) does not match the portal metadata "
            f"({status.portal_bytes} bytes, MD5 {status.portal_md5})"
        )
    return content


def check_contracts(
    request: FuelConsumptionRatingRequest,
    content: bytes,
    *,
    encoding_candidates: list[str],
) -> int:
    """Apply the registered column and model-year contracts; return the data row count."""
    with tempfile.TemporaryDirectory() as folder:
        candidate = request.model_copy(update={
            "cache_path": (Path(folder) / request.cache_path.name).resolve(),
            "expected_md5": bytes_md5(content),
            "expected_bytes": len(content),
        })
        candidate.cache_path.write_bytes(content)
        raw, _ = read_rating_csv(candidate, encoding_candidates=encoding_candidates)
        return len(normalize_rating_dataframe(raw, candidate))


def rewrite_component_pins(yaml_text: str, status: RatingRefreshStatus) -> str:
    """Rewrite one component's version, label date, cache path, MD5 and bytes in place."""
    key = status.component_key
    lines = yaml_text.splitlines(keepends=True)
    heads = [index for index, line in enumerate(lines) if line.rstrip("\r\n") == f"      {key}:"]
    if len(heads) != 1:
        raise ValueError(f"{key}: expected exactly one registry component entry")
    stop = next(
        (index for index in range(heads[0] + 1, len(lines))
         if lines[index].strip() and len(lines[index]) - len(lines[index].lstrip(" ")) <= 6),
        len(lines),
    )
    start, end = sum(map(len, lines[:heads[0]])), sum(map(len, lines[:stop]))
    block = yaml_text[start:end]
    old, new = status.pinned_version, status.portal_version
    replacements = [
        (f'version: "{old}"', f'version: "{new}"'),
        (f"({old})\"", f"({new})\""),
        (f'cache_path: "nrcan_fuel_consumption_ratings/{old}_', f'cache_path: "nrcan_fuel_consumption_ratings/{new}_'),
        (f'expected_md5: "{status.pinned_md5}"', f'expected_md5: "{status.portal_md5}"'),
    ]
    for before, after in replacements:
        if block.count(before) != 1:
            raise ValueError(f"{key}: expected exactly one '{before}' in its registry entry")
        block = block.replace(before, after)
    block, count = re.subn(
        rf"expected_bytes: {status.pinned_bytes}(?=\r?\n)", f"expected_bytes: {status.portal_bytes}", block
    )
    if count != 1:
        raise ValueError(f"{key}: expected exactly one 'expected_bytes: {status.pinned_bytes}' in its registry entry")
    return yaml_text[:start] + block + yaml_text[end:]


def refresh(
    bundle: ConfigBundle, *, components: set[str] | None = None, apply: bool = False
) -> list[RatingRefreshStatus]:
    """Report every selected resource; with apply, adopt the changed ones."""
    encoding_candidates = [str(item) for item in ratings_rules(bundle)["encoding_candidates"]]
    registry = bundle.repo_root / "config" / "sources.yaml"
    statuses = []
    for request in iter_rating_requests(bundle):
        if components and request.component_key not in components:
            continue
        status = refresh_status(request, portal_resource(request.resource_id))
        statuses.append(status)
        if not (apply and status.changed):
            continue
        content = download_verified(request, status)
        rows = check_contracts(request, content, encoding_candidates=encoding_candidates)
        target = request.cache_path.with_name(
            request.cache_path.name.replace(status.pinned_version, status.portal_version, 1)
        )
        if target == request.cache_path:
            raise ValueError(f"{request.component_key}: cache file name carries no version date")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        with registry.open(encoding="utf-8", newline="") as handle:
            text = handle.read()
        with registry.open("w", encoding="utf-8", newline="") as handle:
            handle.write(rewrite_component_pins(text, status))
        print(f"adopted {request.component_key}: {rows} data rows -> {target}")
    return statuses


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", default=DEFAULT_SCENARIO)
    parser.add_argument("--component", action="append", help="limit to one registry component (repeatable)")
    parser.add_argument("--apply", action="store_true", help="adopt changed releases into the cache and registry")
    args = parser.parse_args()
    statuses = refresh(
        load_config_bundle(args.scenario), components=set(args.component or []) or None, apply=args.apply
    )
    print("component | pinned version, bytes | portal version, bytes | status")
    for item in statuses:
        state = "changed" if item.changed else "current"
        print(
            f"{item.component_key} | {item.pinned_version}, {item.pinned_bytes} | "
            f"{item.portal_version}, {item.portal_bytes} | {state}"
        )
    if args.apply and any(item.changed for item in statuses):
        print("Review `git diff config/sources.yaml` and rebuild before committing.")
    elif any(item.changed for item in statuses):
        print("Report only; no files were changed. Rerun with --apply to adopt the changed releases.")


if __name__ == "__main__":
    main()
