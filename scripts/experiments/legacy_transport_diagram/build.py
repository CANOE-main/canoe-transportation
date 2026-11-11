"""Opt-in legacy publication figure. No production ETL imports or network I/O."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import xml.etree.ElementTree as ET

import yaml


ROOT = Path(__file__).resolve().parents[3]
NS = "http://www.w3.org/2000/svg"
ET.register_namespace("", NS)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def repository_path(value: str) -> Path:
    path = (ROOT / value).resolve()
    if not path.is_relative_to(ROOT):
        raise ValueError(f"Path is outside the repository: {value}")
    return path


def extract_topology(config: dict) -> dict:
    """Read unique positive Efficiency links, with vintage variants retained as IDs."""
    db = repository_path(config["source"]["database"])
    original = repository_path(config["source"]["diagram"])
    with sqlite3.connect(db.as_uri() + "?mode=ro", uri=True) as connection:
        technologies = {
            row[0]: {"description": row[1], "category": row[2], "flag": row[3]}
            for row in connection.execute(
                "SELECT tech, tech_desc, tech_category, flag FROM technologies ORDER BY tech"
            )
        }
        commodities = {
            row[0]: {"description": row[1], "flag": row[2], "notes": row[3]}
            for row in connection.execute(
                "SELECT comm_name, comm_desc, flag, additional_notes FROM commodities ORDER BY comm_name"
            )
        }
        links = [
            {"input": row[0], "technology": row[1], "output": row[2]}
            for row in connection.execute(
                "SELECT DISTINCT input_comm, tech, output_comm FROM Efficiency "
                "WHERE efficiency > 0 ORDER BY input_comm, tech, output_comm"
            )
        ]
        demands = dict(
            connection.execute(
                "SELECT DISTINCT demand_comm, demand_units FROM Demand ORDER BY demand_comm"
            )
        )
        emissions = [
            list(row)
            for row in connection.execute(
                "SELECT DISTINCT tech, emis_comm FROM EmissionActivity ORDER BY tech, emis_comm"
            )
        ]
        profiles = [
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT tech FROM CapacityFactorTech ORDER BY tech"
            )
        ]
        embodied_rows = connection.execute(
            "SELECT count(*) FROM EmissionEmbodied"
        ).fetchone()[0]
        skipped_links = connection.execute(
            "SELECT count(*) FROM Efficiency WHERE efficiency IS NULL OR efficiency <= 0"
        ).fetchone()[0]

    assignments: dict[str, list[str]] = {}
    modes = []
    for mode in config["modes"]:
        ids = sorted(
            tech
            for tech in technologies
            if (
                tech in mode.get("technology_ids", [])
                or any(tech.startswith(prefix) for prefix in mode.get("prefixes", []))
            )
        )
        for tech in ids:
            assignments.setdefault(tech, []).append(mode["id"])
        modes.append({**mode, "technologies": ids, "count": len(ids)})

    terminals = {
        commodity: fuel["id"]
        for fuel in config["fuels"]
        for commodity in fuel["commodities"]
    }
    parents: dict[str, set[str]] = {}
    successors: dict[str, set[str]] = {}
    vehicle_links: dict[str, list[dict]] = {}
    for link in links:
        if link["technology"] in assignments:
            vehicle_links.setdefault(link["technology"], []).append(link)
        else:
            parents.setdefault(link["output"], set()).add(link["input"])
            successors.setdefault(link["input"], set()).add(link["output"])

    def reachable(
        start: str, adjacency: dict[str, set[str]], targets: set[str]
    ) -> set[str]:
        pending, visited, result = [start], set(), set()
        while pending:
            commodity = pending.pop()
            if commodity in visited:
                continue
            visited.add(commodity)
            if commodity in targets:
                result.add(commodity)
            else:
                pending.extend(sorted(adjacency.get(commodity, ())))
        return result

    unresolved_inputs, unresolved_outputs = [], []
    for mode in modes:
        fuel_witnesses: dict[str, list[str]] = {}
        service_witnesses: dict[str, list[str]] = {}
        for tech in mode["technologies"]:
            for link in vehicle_links.get(tech, []):
                fuels = {
                    terminals[c]
                    for c in reachable(link["input"], parents, set(terminals))
                }
                services = reachable(link["output"], successors, set(demands))
                if not fuels:
                    unresolved_inputs.append([tech, link["input"]])
                if not services:
                    unresolved_outputs.append([tech, link["output"]])
                for fuel in sorted(fuels):
                    fuel_witnesses.setdefault(fuel, []).append(tech)
                for service in sorted(services):
                    service_witnesses.setdefault(service, []).append(tech)
        mode["fuels"] = {k: sorted(set(v)) for k, v in sorted(fuel_witnesses.items())}
        mode["services"] = {
            k: sorted(set(v)) for k, v in sorted(service_witnesses.items())
        }

    grouped_counts = {
        group: sum(mode["count"] for mode in modes if mode["group"] == group)
        for group in sorted({mode["group"] for mode in modes})
    }
    return {
        "source": {
            "database": config["source"]["database"],
            "database_sha256": digest(db),
            "original_diagram": config["source"]["diagram"],
            "original_sha256": digest(original),
            "read_only": True,
        },
        "technologies": technologies,
        "commodities": commodities,
        "efficiency_links": links,
        "demands": demands,
        "modes": modes,
        "assignments": assignments,
        "grouped_counts": grouped_counts,
        "original_count_reconciliation": {
            key: {"original_svg": value, "inspected_sqlite": grouped_counts[key]}
            for key, value in config["original_counts"].items()
        },
        "emission_activity_membership": emissions,
        "embodied_rows": embodied_rows,
        "hourly_profile_technologies": profiles,
        "external_electricity_interfaces": config["external_electricity_interfaces"],
        "unresolved_inputs": unresolved_inputs,
        "unresolved_outputs": unresolved_outputs,
        "skipped_nonpositive_or_null_efficiency_rows": skipped_links,
        "aggregation": {
            "energy_bus": "Grouped eligible pathways; not an all-to-all fuel connection.",
            "annual_service_bridges": "Four T_dummy_* technologies folded into final demand links.",
            "variant_membership": "Existing/new technologies and range variants remain distinct in counts and audit.",
            "emissions": "Fuel supply-chain and direct emissions recorded at T_IMP_*/T_EA_*; separate process emissions at I_H2_SMR_CCS.",
            "embodied": "Original vehicle-cycle parameter capability retained in the compact node-type legend; inspected EmissionEmbodied is empty.",
            "electricity": "E_elc_dx and I_elc are external interfaces; no internal producer inferred.",
        },
    }


def validate_topology(config: dict, topology: dict) -> dict:
    expected = {
        tech
        for tech, row in topology["technologies"].items()
        if row["category"] in config["vehicle_categories"]
    }
    assigned = set(topology["assignments"])
    errors = []
    if expected != assigned:
        errors.append(
            {
                "vehicle_membership": {
                    "missing": sorted(expected - assigned),
                    "extra": sorted(assigned - expected),
                }
            }
        )
    overlap = {
        tech: modes
        for tech, modes in topology["assignments"].items()
        if len(modes) != 1
    }
    if overlap:
        errors.append({"overlapping_assignments": overlap})
    covered = {service for mode in topology["modes"] for service in mode["services"]}
    if covered != set(topology["demands"]):
        errors.append({"uncovered_demands": sorted(set(topology["demands"]) - covered)})
    if topology["unresolved_inputs"] or topology["unresolved_outputs"]:
        errors.append(
            {
                "unresolved_inputs": topology["unresolved_inputs"],
                "unresolved_outputs": topology["unresolved_outputs"],
            }
        )
    if any(not mode["technologies"] for mode in topology["modes"]):
        errors.append("Empty mode selection")
    if errors:
        raise ValueError(json.dumps(errors, indent=2))
    return {
        "status": "pass",
        "selected_vehicle_and_offroad_technologies": len(assigned),
        "mode_rows": len(topology["modes"]),
        "demand_commodities": len(covered),
        "positive_unique_efficiency_links": len(topology["efficiency_links"]),
        "unresolved_vehicle_inputs": 0,
        "unresolved_vehicle_outputs": 0,
    }


def contrast(foreground: str, background: str) -> float:
    def luminance(color: str) -> float:
        channels = [int(color[i : i + 2], 16) / 255 for i in (1, 3, 5)]
        linear = [
            v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4
            for v in channels
        ]
        return sum(v * weight for v, weight in zip(linear, (0.2126, 0.7152, 0.0722)))

    values = sorted((luminance(foreground), luminance(background)))
    return (values[1] + 0.05) / (values[0] + 0.05)


class Svg:
    """Small native SVG authoring surface with measured text constraints."""

    def __init__(
        self, config: dict, name: str, height: int, title: str, description: str
    ):
        self.config, self.style = config["publication"], config["style"]
        self.width, self.height = self.config["canvas_width"], height
        self.text_bounds: dict[str, tuple[float, float, float, float]] = {}
        self.text_contrasts: list[float] = []
        self.counter = 0
        prefix = name + "-"
        self.root = ET.Element(
            f"{{{NS}}}svg",
            {
                "width": f"{self.config['width_mm']}mm",
                "height": f"{height * self.config['width_mm'] / self.width:g}mm",
                "viewBox": f"0 0 {self.width} {height}",
                "role": "img",
                "aria-labelledby": prefix + "title " + prefix + "description",
            },
        )
        ET.SubElement(
            self.root, f"{{{NS}}}title", {"id": prefix + "title"}
        ).text = title
        ET.SubElement(
            self.root, f"{{{NS}}}desc", {"id": prefix + "description"}
        ).text = description
        defs = ET.SubElement(self.root, f"{{{NS}}}defs")
        for key in ("technology", "fuel", "clean", "service", "emission"):
            marker = ET.SubElement(
                defs,
                f"{{{NS}}}marker",
                {
                    "id": f"arrow-{key}",
                    "viewBox": "0 0 10 10",
                    "refX": "9",
                    "refY": "5",
                    "markerWidth": "8",
                    "markerHeight": "8",
                    "orient": "auto-start-reverse",
                    "markerUnits": "userSpaceOnUse",
                },
            )
            ET.SubElement(
                marker,
                f"{{{NS}}}path",
                {
                    "d": "M 0 0 L 10 5 L 0 10 Z",
                    "fill": self.style[f"{key}_stroke"],
                },
            )
        self.layers = {}
        for layer in ("paper", "zones", "links", "nodes", "labels"):
            self.layers[layer] = ET.SubElement(self.root, f"{{{NS}}}g", {"id": layer})
        self.element(
            "rect",
            {"width": self.width, "height": height, "fill": self.style["paper"]},
            "paper",
        )

    def element(self, tag: str, attributes: dict, layer: str = "nodes") -> ET.Element:
        return ET.SubElement(
            self.layers[layer],
            f"{{{NS}}}{tag}",
            {k: str(v) for k, v in attributes.items()},
        )

    def text(
        self,
        x: float,
        y: float,
        value: str,
        size: int = 28,
        weight: int = 400,
        color: str = "ink",
        anchor: str = "start",
        bounds: tuple | None = None,
        background: str = "paper",
    ) -> None:
        self.counter += 1
        identity = f"label-{self.counter:03d}"
        attributes = {
            "id": identity,
            "x": x,
            "y": y,
            "font-family": self.config["font_family"],
            "font-size": size,
            "font-weight": weight,
            "text-anchor": anchor,
            "fill": self.style.get(color, color),
        }
        self.element("text", attributes, "labels").text = value
        self.text_bounds[identity] = bounds or (
            20,
            10,
            self.width - 20,
            self.height - 10,
        )
        self.text_contrasts.append(
            contrast(
                self.style.get(color, color), self.style.get(background, background)
            )
        )

    def rectangle(
        self,
        identity: str,
        x: float,
        y: float,
        width: float,
        height: float,
        kind: str = "technology",
        radius: float = 6,
        technologies: list | None = None,
        modes: list | None = None,
    ) -> str:
        attrs = {
            "id": identity,
            "x": x,
            "y": y,
            "width": width,
            "height": height,
            "rx": radius,
            "stroke-width": 2,
            "fill": self.style[f"{kind}_fill"],
            "stroke": self.style[f"{kind}_stroke"],
        }
        if technologies:
            attrs["data-technologies"] = " ".join(technologies)
        if modes:
            attrs["data-modes"] = " ".join(modes)
        self.element("rect", attrs)
        return f"{kind}_fill"

    def block(
        self,
        identity: str,
        x: float,
        y: float,
        width: float,
        height: float,
        title: str,
        subtitles: tuple = (),
        kind: str = "technology",
        technologies: list | None = None,
        modes: list | None = None,
        title_size: int = 30,
        subtitle_size: int = 28,
    ) -> None:
        background = self.rectangle(
            identity,
            x,
            y,
            width,
            height,
            kind,
            26 if kind in ("fuel", "clean", "service") else 6,
            technologies,
            modes,
        )
        bounds = (x + 16, y + 8, x + width - 16, y + height - 8)
        if not subtitles:
            self.text(
                x + width / 2,
                y + height / 2 + title_size * 0.34,
                title,
                title_size,
                600,
                anchor="middle",
                bounds=bounds,
                background=background,
            )
        else:
            title_offset = 31 if height < 90 else 34
            subtitle_offset = 60
            self.text(
                x + 20,
                y + title_offset,
                title,
                title_size,
                600,
                bounds=bounds,
                background=background,
            )
            for index, line in enumerate(subtitles):
                self.text(
                    x + 20,
                    y + subtitle_offset + index * 30,
                    line,
                    subtitle_size,
                    bounds=bounds,
                    background=background,
                )

    def route(
        self,
        points: list[tuple[float, float]],
        kind: str = "technology",
        arrow: bool = True,
        dashed: bool = False,
        width: float = 3,
    ) -> None:
        """Rounded orthogonal polyline; each corner is radius-clamped to short segments."""
        path = f"M {points[0][0]:g} {points[0][1]:g}"
        for index in range(1, len(points) - 1):
            a, b, c = points[index - 1], points[index], points[index + 1]
            before, after = math.dist(a, b), math.dist(b, c)
            radius = min(5, before / 2, after / 2)
            if not before or not after:
                raise ValueError("Zero-length path segment")
            start = tuple(b[i] + (a[i] - b[i]) * radius / before for i in (0, 1))
            end = tuple(b[i] + (c[i] - b[i]) * radius / after for i in (0, 1))
            path += f" L {start[0]:g} {start[1]:g} Q {b[0]:g} {b[1]:g} {end[0]:g} {end[1]:g}"
        path += f" L {points[-1][0]:g} {points[-1][1]:g}"
        attrs = {
            "d": path,
            "fill": "none",
            "stroke": self.style[f"{kind}_stroke"],
            "stroke-width": width,
            "stroke-linecap": "round",
            "stroke-linejoin": "round",
        }
        if arrow:
            attrs["marker-end"] = f"url(#arrow-{kind})"
        if dashed:
            attrs["stroke-dasharray"] = "8 6"
        self.element("path", attrs, "links")

    def rule(self, x1: float, y: float, x2: float) -> None:
        self.element(
            "line",
            {
                "x1": x1,
                "y1": y,
                "x2": x2,
                "y2": y,
                "stroke": self.style["rule"],
                "stroke-width": 2,
            },
            "zones",
        )

    def save(self, path: Path) -> None:
        ET.indent(self.root, space="  ")
        ET.ElementTree(self.root).write(path, encoding="utf-8", xml_declaration=True)


def draw_main(config: dict, topology: dict) -> Svg:
    svg = Svg(
        config,
        "legacy-main",
        config["publication"]["main_height"],
        "Legacy CANOE transportation model",
        "Wide overview of imported fuels, endogenous hydrogen and separate demands "
        "by vehicle and mode class. Original technology counts are retained. "
        "Hourly charger load is annotated only for light-duty BEVs. "
        "Node-type parameters form a compact legend at bottom left.",
    )
    modes = {mode["id"]: mode for mode in topology["modes"]}

    def node(
        identity, x, y, width, height, title, kind, technologies=(), modes_=(), size=18
    ):
        background = svg.rectangle(
            identity,
            x,
            y,
            width,
            height,
            kind,
            14 if kind in ("fuel", "clean", "service") else 5,
            technologies=list(technologies),
            modes=list(modes_),
        )
        svg.root.find(f".//{{{NS}}}rect[@id='{identity}']").set("stroke-width", "1.3")
        svg.text(
            x + width / 2,
            y + height / 2 + size * 0.32,
            title,
            size,
            600,
            anchor="middle",
            background=background,
            bounds=(x + 8, y + 4, x + width - 8, y + height - 4),
        )

    def wire(
        identity, points, kind="technology", arrow=True, dashed=False, technologies=()
    ):
        svg.route(points, kind, arrow=arrow, dashed=dashed, width=1.6)
        path = svg.layers["links"][-1]
        path.set("id", identity)
        path.set("data-route-points", json.dumps(points, separators=(",", ":")))
        if technologies:
            path.set("data-technologies", " ".join(technologies))
        return path

    def frame(identity, x, y, width, height, title, kind="technology", title_size=20):
        shape = svg.element(
            "rect",
            {
                "id": identity,
                "x": x,
                "y": y,
                "width": width,
                "height": height,
                "rx": 7,
                "fill": svg.style[f"{kind}_zone_fill"],
                "stroke": svg.style[f"{kind}_stroke"],
                "stroke-width": 1.3,
            },
            "zones",
        )
        svg.text(
            x + 12,
            y + 22,
            title,
            title_size,
            600,
            background=f"{kind}_zone_fill",
            bounds=(x + 10, y + 6, x + width - 10, y + 28),
        )
        return shape

    def annotation(mode_id, techs):
        eligible = {
            fuel
            for fuel, witnesses in modes[mode_id]["fuels"].items()
            if set(witnesses) & set(techs)
        }
        return " · ".join(
            fuel["symbol"] for fuel in config["fuels"] if fuel["id"] in eligible
        )

    def demand(identity, y, demand_id, code):
        title = "Passenger-km" if code == "P" else "Tonne-km"
        node(identity, 1090, y, 170, 30, title, "service")
        svg.root.find(f".//{{{NS}}}rect[@id='{identity}']").set(
            "data-demands", demand_id
        )
        center = y + 15
        wire("service-" + identity, [(966, center), (1090, center)], "service")
        # Borderless typographic ports replace the former circular badges.
        svg.element(
            "rect",
            {
                "x": 1025,
                "y": center - 11,
                "width": 24,
                "height": 22,
                "rx": 3,
                "fill": svg.style["paper"],
            },
        )
        svg.text(
            1037,
            center + 6,
            code,
            18,
            600,
            color="service_stroke",
            anchor="middle",
            bounds=(1027, center - 10, 1047, center + 10),
        )

    def vehicle(identity, y, title, mode_id, demand_id, code):
        techs = modes[mode_id]["services"][demand_id]
        svg.rectangle(identity, 522, y, 444, 30, technologies=techs, modes=[mode_id])
        box = svg.root.find(f".//{{{NS}}}rect[@id='{identity}']")
        box.set("stroke-width", "0")
        box.set("data-output-demand", demand_id)
        box.set(
            "data-carriers",
            " ".join(
                fuel
                for fuel, witnesses in modes[mode_id]["fuels"].items()
                if set(witnesses) & set(techs)
            ),
        )
        svg.text(
            534,
            y + 21,
            title,
            18,
            600,
            background="technology_fill",
            bounds=(530, y + 5, 716, y + 26),
        )
        svg.text(
            722,
            y + 21,
            annotation(mode_id, techs),
            18,
            background="technology_fill",
            color="muted",
            bounds=(718, y + 5, 957, y + 26),
        )
        demand("demand-" + identity, y, demand_id, code)

    def family(identity, y, height, title, group):
        box = frame(identity, 510, y, 468, height, title)
        count = config["original_counts"][group]
        box.set("data-displayed-tech-count", str(count))
        svg.text(
            963,
            y + 22,
            f"{count} tech.",
            18,
            color="technology_stroke",
            anchor="end",
            background="technology_zone_fill",
            bounds=(878, y + 6, 967, y + 28),
        )

    for x, title in (
        (20, "Energy supply"),
        (510, "Transport technologies"),
        (1038, "Class / mode demands"),
    ):
        svg.text(x, 30, title, 20, 600, bounds=(x, 10, 1260, 37))

    fuel_scope = frame(
        "fuel-emissions-envelope", 20, 90, 402, 282, "Imported fuels", "emission", 22
    )
    fuel_scope.set("data-emission-scope", "upstream direct-fuel")
    svg.text(
        32,
        139,
        "Fuel emissions accounting",
        18,
        color="emission_stroke",
        background="emission_zone_fill",
        bounds=(30, 119, 410, 145),
    )
    imports = [
        (148, "diesel", "Diesel / Ren. diesel", ["DSL", "RDSL"], ["T_BLND_RDSL_DSL"]),
        (184, "gasoline", "Gasoline / ethanol", ["GSL", "ETH"], ["T_BLND_ETH_GSL"]),
        (
            220,
            "jet",
            "Jet fuel / synthetic jet fuel",
            ["JTF", "SPK"],
            ["T_BLND_JTF", "T_BLND_SPK"],
        ),
        (256, "gas", "CNG / LNG", ["CNG", "LNG"], []),
        (292, "marine", "Heavy fuel oil / marine diesel", ["HFO", "MDO"], []),
        (328, "ng", "Natural gas", ["NG"], []),
    ]
    for y, key, title, codes, blends in imports:
        node(
            "import-" + key,
            32,
            y,
            378,
            30,
            title,
            "fuel",
            technologies=["T_IMP_" + code for code in codes],
            size=20,
        )
        processors = ["T_EA_" + code for code in codes] + blends
        if key != "ng":
            wire(
                "fuel-" + key,
                [(410, y + 15), (475, y + 15)],
                "fuel",
                arrow=False,
                technologies=processors,
            )
        else:
            wire(
                "ng-to-hydrogen",
                [(232, y + 30), (232, 445), (258, 445)],
                "fuel",
                technologies=processors,
            )
    # Top placement avoids the former perimeter-spanning off-road bypass.
    bypass = wire(
        "direct-diesel-offroad",
        [(410, 163), (442, 163), (442, 50), (1014, 50), (1014, 72), (1090, 72)],
        "fuel",
        technologies=["T_OFF"],
    )
    bypass.set("data-input", "T_dsl")
    bypass.set("data-demands", "T_D_pj_off")
    node("offroad-demand", 1090, 57, 170, 30, "Other off-road", "service")
    svg.root.find(f".//{{{NS}}}rect[@id='offroad-demand']").set(
        "data-demands", "T_D_pj_off"
    )
    svg.element(
        "rect",
        {
            "x": 1022,
            "y": 61,
            "width": 29,
            "height": 22,
            "rx": 3,
            "fill": svg.style["paper"],
        },
    )
    svg.text(
        1037,
        78,
        "PJ",
        18,
        color="service_stroke",
        anchor="middle",
        bounds=(1024, 62, 1050, 82),
    )

    node(
        "electricity-interface",
        32,
        400,
        178,
        32,
        "Electricity",
        "clean",
        technologies=["T_IMP_ELC"],
        size=20,
    )
    svg.root.find(f".//{{{NS}}}rect[@id='electricity-interface']").set(
        "data-external-interfaces", " ".join(config["external_electricity_interfaces"])
    )
    charging = [
        "T_LDV_PHEV_CHRG",
        "T_MDV_CHRG",
        "T_HDV_CHRG",
        "T_BLND_GSL_ELC_PHEV35",
        "T_BLND_GSL_ELC_PHEV50",
        "T_BLND_DSL_ELC_MDV",
        "T_BLND_DSL_ELC_HDV",
    ]
    wire(
        "electricity-to-transport",
        [(210, 416), (220, 416), (220, 382)],
        "clean",
        arrow=False,
        technologies=charging,
    )
    # One explicit crossing, between external electricity and the NG feedstock.
    svg.element(
        "path",
        {
            "d": "M 228 382 A 4 4 0 0 1 236 382",
            "fill": "none",
            "stroke": svg.style["paper"],
            "stroke-width": 6,
        },
        "links",
    )
    svg.element(
        "path",
        {
            "id": "external-power-crossing",
            "d": "M 220 382 H 228 A 4 4 0 0 1 236 382 H 475",
            "fill": "none",
            "stroke": svg.style["clean_stroke"],
            "stroke-width": 1.6,
        },
        "links",
    )
    wire("electricity-to-electrolysis", [(220, 416), (220, 485), (258, 485)], "clean")
    production = frame(
        "endogenous-hydrogen-envelope", 246, 390, 214, 158, "Endogenous H₂"
    )
    production.set("data-production-boundary", "model-produced-hydrogen")
    node(
        "smr",
        258,
        430,
        182,
        30,
        "Steam reforming",
        "technology",
        technologies=["I_H2_SMR", "I_H2_SMR_CCS"],
    )
    node(
        "electrolysis",
        258,
        470,
        182,
        30,
        "Electrolysis",
        "technology",
        technologies=["ELC_AC_DC", "I_H2_ELC_ALK", "I_H2_ELC_PEM"],
    )
    wire(
        "smr-hydrogen",
        [(440, 445), (452, 445), (452, 503), (428, 503), (428, 510)],
        "clean",
    )
    wire("electrolysis-hydrogen", [(440, 485), (452, 485)], "clean", arrow=False)
    node("produced-hydrogen", 408, 510, 40, 24, "H₂", "clean")
    delivery = [
        "H2_distribution",
        "H2_COMP_10_100",
        "H2_COMP_100_700",
        "H2_storage",
        "T_H2_LDV_REFUEL",
        "T_H2_MDV_REFUEL",
        "T_H2_HDV_REFUEL",
    ]
    wire(
        "hydrogen-to-transport",
        [(448, 522), (475, 522)],
        "clean",
        arrow=False,
        technologies=delivery,
    )

    wire("eligible-energy-trunk", [(475, 117), (475, 687)], arrow=False)
    for key, y in (
        ("two-wheel", 117),
        ("light-duty", 222),
        ("medium-duty", 351),
        ("heavy-duty", 552),
    ):
        wire("energy-to-" + key, [(475, y), (510, y)])

    family("two-wheel-family", 74, 66, "Two-wheel vehicles", "motorcycles")
    vehicle("motorcycles", 102, "Motorcycles", "motorcycles", "T_D_pkm_ldv_m", "P")
    family("light-duty-family", 146, 156, "Light duty · classes 1-2", "light_duty")
    vehicle("cars", 174, "Cars", "cars", "T_D_pkm_ldv_c", "P")
    vehicle(
        "passenger-trucks",
        207,
        "Passenger trucks",
        "light_passenger",
        "T_D_pkm_ldv_t",
        "P",
    )
    vehicle(
        "freight-trucks", 240, "Freight trucks", "light_freight", "T_D_tkm_ldv_t", "F"
    )
    target = svg.element(
        "rect",
        {
            "id": "ldv-bev-target",
            "x": 532,
            "y": 276,
            "width": 58,
            "height": 22,
            "fill": "none",
            "stroke": "none",
        },
    )
    svg.text(
        534,
        292,
        "BEVs",
        18,
        600,
        color="technology_stroke",
        background="technology_zone_fill",
        bounds=(530, 274, 594, 299),
    )
    bev_ids = sorted(
        {
            tech
            for key in ("cars", "light_passenger", "light_freight")
            for tech in modes[key]["fuels"]["electricity"]
            if "_BEV" in tech
        }
    )
    target.set("data-hourly-target-technologies", " ".join(bev_ids))
    target.set("data-hourly-target-groups", "light_duty")
    wire(
        "hourly-charger-load",
        [(702, 286), (595, 286)],
        "clean",
        dashed=True,
        technologies=["T_LDV_BEV_CHRG"],
    )
    svg.text(
        715,
        292,
        "Hourly charger load",
        18,
        600,
        color="clean_stroke",
        background="technology_zone_fill",
        bounds=(711, 274, 963, 299),
    )
    family("medium-duty-family", 308, 66, "Medium duty · classes 2b-7", "medium_duty")
    vehicle(
        "medium-trucks",
        336,
        "Trucks",
        "medium_trucks",
        "T_D_tkm_mdv_t",
        "F",
    )
    family(
        "heavy-duty-family",
        380,
        330,
        "Heavy duty · road / air / rail / marine",
        "heavy_duty_and_nonroad",
    )
    rows = [
        ("heavy-trucks", "Trucks · class 8", "heavy_trucks", "T_D_tkm_hdv_t", "F"),
        ("transit-buses", "Transit buses", "transit_buses", "T_D_pkm_hdv_bt", "P"),
        ("school-buses", "School buses", "school_buses", "T_D_pkm_hdv_bs", "P"),
        ("coach-buses", "Coach buses", "intercity_buses", "T_D_pkm_hdv_bic", "P"),
        ("air-passenger", "Jet aircraft", "aircraft", "T_D_pkm_hdv_aj", "P"),
        ("air-freight", "Jet aircraft", "aircraft", "T_D_tkm_hdv_aj", "F"),
        ("rail-passenger", "Rail", "rail", "T_D_pkm_hdv_r", "P"),
        ("rail-freight", "Rail", "rail", "T_D_tkm_hdv_r", "F"),
        ("marine", "Marine vessels", "marine", "T_D_tkm_hdv_wt", "F"),
    ]
    for index, (identity, title, mode_id, demand_id, code) in enumerate(rows):
        vehicle(identity, 408 + index * 33, title, mode_id, demand_id, code)

    # A small glyph-based node legend, with no filled parameter panels or glossary.
    svg.rectangle("legend-technology-glyph", 20, 562, 20, 16, radius=3)
    svg.text(
        50,
        578,
        config["parameters"]["technology"]["title"],
        18,
        600,
        bounds=(46, 560, 264, 584),
    )
    svg.rectangle("legend-fuel-glyph", 286, 562, 24, 16, "fuel", radius=8)
    svg.text(
        320,
        578,
        config["parameters"]["fuel"]["title"],
        18,
        600,
        bounds=(316, 560, 455, 584),
    )
    fields = config["parameters"]["technology"]["fields"]
    for y, label in zip(
        (600, 622, 644, 666),
        (" · ".join(fields[:2]), " · ".join(fields[2:4]), fields[4], fields[5]),
        strict=True,
    ):
        svg.text(20, y, label, 18, bounds=(20, y - 18, 274, y + 5))
    for y, label in zip(
        (600, 622, 644), config["parameters"]["fuel"]["fields"], strict=True
    ):
        svg.text(286, y, label, 18, bounds=(282, y - 18, 456, y + 5))
    svg.text(
        286,
        669,
        " · ".join(config["parameters"]["fuel"]["indexing"]),
        18,
        color="fuel_stroke",
        bounds=(282, 650, 456, 675),
    )
    svg.text(
        20,
        699,
        " · ".join(config["parameters"]["technology"]["indexing"]),
        18,
        color="technology_stroke",
        bounds=(20, 680, 460, 707),
    )
    return svg


def validate_svg(svg: Svg, path: Path) -> dict:
    root = ET.parse(path).getroot()
    ids, texts, references = [], [], []
    for element in root.iter():
        tag = element.tag.rsplit("}", 1)[-1]
        if tag in ("image", "foreignObject", "script"):
            raise ValueError(f"Non-native vector content: {tag}")
        if element.get("id"):
            ids.append(element.get("id"))
        if tag == "text":
            texts.append(element)
        for attribute, value in element.attrib.items():
            if attribute.endswith("href") or "url(" in value:
                references.append(value)
                if not (value.startswith("#") or value.startswith("url(#")):
                    raise ValueError(f"External SVG resource: {value}")
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate SVG IDs")
    minimum = min(float(text.get("font-size")) for text in texts)
    minimum_pt = minimum * svg.config["width_mm"] / svg.width * 72 / 25.4
    if minimum < svg.config["minimum_font_px"] or minimum_pt < 7:
        raise ValueError(f"Type too small at publication width: {minimum_pt}")
    if min(svg.text_contrasts) < 4.5:
        raise ValueError("Insufficient text/fill contrast")
    return {
        "status": "pass",
        "native_text_elements": len(texts),
        "raster_elements": 0,
        "foreign_objects": 0,
        "external_resources": 0,
        "unique_ids": len(ids),
        "physical_width_mm": svg.config["width_mm"],
        "minimum_type_pt": round(minimum_pt, 3),
        "minimum_text_contrast": round(min(svg.text_contrasts), 3),
        "svg_sha256": digest(path),
    }


def run_renderer(inkscape: str, arguments: list[str], env: dict) -> str:
    result = subprocess.run(
        [inkscape, *arguments],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout)
    return result.stdout


def validate_pdf(path: Path, svg: Svg) -> dict:
    from pypdf import PdfReader

    reader = PdfReader(path)
    if len(reader.pages) != 1:
        raise ValueError("Expected one-page figure PDF")
    page = reader.pages[0]
    width, height = float(page.mediabox.width), float(page.mediabox.height)
    expected_width = svg.config["width_mm"] * 72 / 25.4
    expected_height = expected_width * svg.height / svg.width
    if max(abs(width - expected_width), abs(height - expected_height)) > 0.1:
        raise ValueError("PDF physical size differs from SVG")
    image_count, fonts = 0, []
    visited = set()

    def inspect_resources(resources):
        nonlocal image_count
        resources = resources.get_object()
        for font in (
            resources.get("/Font", {}).get_object().values()
            if "/Font" in resources
            else []
        ):
            font = font.get_object()
            descendants = font.get("/DescendantFonts", [font])
            for descendant in descendants:
                descendant = descendant.get_object()
                descriptor = (
                    descendant.get("/FontDescriptor", {}).get_object()
                    if "/FontDescriptor" in descendant
                    else {}
                )
                fonts.append(
                    {
                        "name": str(descendant.get("/BaseFont", "")),
                        "embedded": any(
                            key in descriptor
                            for key in ("/FontFile", "/FontFile2", "/FontFile3")
                        ),
                    }
                )
        for reference in (
            resources.get("/XObject", {}).get_object().values()
            if "/XObject" in resources
            else []
        ):
            obj = reference.get_object()
            key = getattr(reference, "idnum", id(obj))
            if key in visited:
                continue
            visited.add(key)
            if obj.get("/Subtype") == "/Image":
                image_count += 1
            if "/Resources" in obj:
                inspect_resources(obj["/Resources"])

    inspect_resources(page["/Resources"])
    text = page.extract_text() or ""
    if (
        image_count
        or not fonts
        or not all(font["embedded"] for font in fonts)
        or len(text) < 500
    ):
        raise ValueError(
            f"PDF must contain embedded vector text and no images: {fonts}, images={image_count}"
        )
    return {
        "status": "pass",
        "pages": 1,
        "width_mm": round(width * 25.4 / 72, 3),
        "height_mm": round(height * 25.4 / 72, 3),
        "raster_images": image_count,
        "fonts": fonts,
        "extracted_text_characters": len(text),
    }


def render_and_measure(
    inkscape: str, source: Path, svg: Svg, scratch: Path, dpi: int
) -> dict:
    env = dict(os.environ)
    profile = scratch / "inkscape-profile"
    profile.mkdir(parents=True, exist_ok=True)
    env["INKSCAPE_PROFILE_DIR"] = str(profile)
    pdf, png = source.with_suffix(".pdf"), source.with_suffix(".png")
    run_renderer(
        inkscape, [str(source), "--export-type=pdf", f"--export-filename={pdf}"], env
    )
    run_renderer(
        inkscape,
        [
            str(source),
            "--export-type=png",
            f"--export-filename={png}",
            f"--export-dpi={dpi}",
        ],
        env,
    )
    run_renderer(
        inkscape,
        [
            str(pdf),
            "--pdf-poppler",
            "--export-type=png",
            f"--export-filename={scratch / (source.stem + '-pdf-preview.png')}",
            f"--export-width={svg.width}",
        ],
        env,
    )
    query = run_renderer(inkscape, [str(source), "--query-all"], env)
    found, overflow = set(), []
    # Inkscape reports physical CSS pixels (96 dpi), not the SVG viewBox units.
    scale = svg.width / (svg.config["width_mm"] * 96 / 25.4)
    for row in csv.reader(query.splitlines()):
        if len(row) != 5 or row[0] not in svg.text_bounds:
            continue
        found.add(row[0])
        x, y, width, height = (float(value) * scale for value in row[1:])
        left, top, right, bottom = svg.text_bounds[row[0]]
        if (
            x < left - 1
            or y < top - 1
            or x + width > right + 1
            or y + height > bottom + 1
        ):
            overflow.append(
                {
                    "id": row[0],
                    "actual": [x, y, x + width, y + height],
                    "allowed": [left, top, right, bottom],
                }
            )
    missing = sorted(set(svg.text_bounds) - found)
    if overflow or missing:
        raise ValueError(
            json.dumps(
                {"text_overflow": overflow, "missing_text_bounds": missing}, indent=2
            )
        )
    return {
        "text_bounds": {
            "status": "pass",
            "measured_labels": len(found),
            "overflows": 0,
        },
        "pdf": validate_pdf(pdf, svg),
        "png_dpi": dpi,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path, default=Path(__file__).with_name("figure.yaml")
    )
    parser.add_argument(
        "--render",
        action="store_true",
        help="Export PDF/600-dpi PNG with optional external Inkscape",
    )
    parser.add_argument(
        "--inkscape",
        default=shutil.which("inkscape"),
        help="Path to installed Inkscape; never auto-downloads",
    )
    arguments = parser.parse_args()
    config = yaml.safe_load(arguments.config.read_text(encoding="utf-8"))
    registry = yaml.safe_load(
        repository_path(config["paths"]["registry"]).read_text(encoding="utf-8")
    )
    route = registry["artifacts"][config["paths"]["artifact"]]
    if route["owner"] != config["owner"] or route["layer"] != "legacy":
        raise ValueError(
            "Artifact route must match the experiment owner and legacy layer"
        )
    output = repository_path(route["path"])
    if not output.is_relative_to(repository_path(registry["legacy"]["root"])):
        raise ValueError("Figure output must be within the configured legacy root")
    if arguments.render and not arguments.inkscape:
        raise ValueError(
            "Pass --inkscape pointing to an installed Inkscape; no dependency is installed automatically"
        )
    output.mkdir(parents=True, exist_ok=True)
    scratch = ROOT / "tmp" / "legacy_diagram"
    scratch.mkdir(parents=True, exist_ok=True)
    topology = extract_topology(config)
    report = {"topology": validate_topology(config, topology), "figures": {}}
    figures = [
        (config["paths"]["main"], draw_main(config, topology)),
    ]
    for name, svg in figures:
        path = output / (name + ".svg")
        svg.save(path)
        result = validate_svg(svg, path)
        if name == config["paths"]["main"]:
            represented = {
                tech
                for element in svg.root.iter()
                for tech in element.get("data-technologies", "").split()
            }
            collapsed = {
                tech for tech in topology["technologies"] if tech.startswith("T_dummy_")
            }
            represented_demands = {
                demand
                for element in svg.root.iter()
                for demand in element.get("data-demands", "").split()
            }
            if represented | collapsed != set(topology["technologies"]):
                raise ValueError("Main SVG technology IDs differ from the source model")
            if represented_demands != set(topology["demands"]):
                raise ValueError("Main SVG demand IDs differ from the source model")
            result["model_membership"] = {
                "drawn_technology_ids": len(represented),
                "folded_annual_bridges": sorted(collapsed),
                "represented_demand_ids": len(represented_demands),
            }
        if arguments.render:
            result.update(
                render_and_measure(
                    arguments.inkscape,
                    path,
                    svg,
                    scratch,
                    config["publication"]["png_dpi"],
                )
            )
        report["figures"][name] = result
    source = topology["source"]
    if (
        digest(repository_path(source["database"])) != source["database_sha256"]
        or digest(repository_path(source["original_diagram"]))
        != source["original_sha256"]
    ):
        raise ValueError("Legacy inputs changed during figure generation")
    report["source_hashes_unchanged"] = True
    report["installed_python_dependencies"] = []
    report["network_access"] = False
    for filename, value in (("topology.json", topology), ("validation.json", report)):
        (output / filename).write_text(
            json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    caption = (
        "Overview of the legacy CANOE transportation model and its embedded fuel supply. "
        "The fuel-supply envelope locates upstream and direct fuel-use emissions accounting. "
        "Hydrogen is produced endogenously through steam reforming or electrolysis. "
        "Each passenger-km or tonne-km endpoint remains specific to its vehicle class and mode; "
        "repeated unit labels do not imply a shared demand or cross-class substitution. "
        "Separate passenger and freight air/rail rows and individual bus classes retain the legacy demand distinctions. "
        "The dashed Hourly charger load annotation applies to light-duty BEVs. "
        "Other off-road demand receives diesel directly in PJ. Original family counts 3/54/10/57 are retained. "
        "Eligible carrier labels and the compact node-type parameter legend summarize the model; "
        "supply processing, charging variants and hydrogen conditioning remain in the audit without separate plotted stages.\n"
    )
    (output / "captions.txt").write_text(caption, encoding="utf-8")
    print(
        json.dumps(
            {
                "output": route["path"],
                "topology": report["topology"],
                "source_hashes_unchanged": True,
                "rendered": arguments.render,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
