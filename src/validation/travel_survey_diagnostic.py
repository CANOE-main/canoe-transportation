"""Walk through maintained NHTS evidence and reviewed RAMP input derivation."""

import argparse
import json
import logging
from dataclasses import dataclass

import altair as alt
import numpy as np
import pandas as pd

from fetching.nhts_ldv_travel import SOURCE, build_requests, read_tables, validate_cache
from fetching.legacy_charging_profiles import build_request, validate_run_metadata
from utils import (
    ConfigBundle,
    file_sha256,
    find_repo_root,
    load_config_bundle,
    load_conversion_factors,
    load_parameter_yaml,
    resolve_artifact_path,
    resolve_parameter_path,
    write_dataframe_atomic,
    write_text_atomic,
)
from validation.notebook_figures import identified_chart

LOGGER = logging.getLogger(__name__)
DERIVATION = "RAMP derivation (unweighted)"
WEIGHTED = "Survey-weighted context"


@dataclass(frozen=True)
class TravelSurveyDiagnostic:
    tables: dict[str, pd.DataFrame]
    metadata: dict
    settings: dict


def curate_trips(
    trips: pd.DataFrame, vehicles: pd.DataFrame, settings: dict, *, weighted: bool
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the reviewed derivation screens; weighted context uses the trip-driver flag."""
    rules = settings["filters"]
    deduplicated = trips.drop_duplicates(subset=rules["deduplication_columns"])
    frame = deduplicated.copy()
    speed = frame.TRPMILES / (frame.TRVLCMIN / 60)
    hhmm = frame.STRTTIME
    criteria = [
        (
            "LDV trip mode",
            frame.TRPTRANS.le(max(rules["ldv_modes"]))
            if not weighted
            else frame.TRPTRANS.isin(rules["ldv_modes"]),
        ),
        ("LDV vehicle type", frame.VEHTYPE.isin(rules["vehicle_types"])),
        (
            "Respondent is a driver" if not weighted else "Driver on this trip",
            frame.DRIVER.eq(1) if not weighted else frame.DRVR_FLG.eq(1),
        ),
        ("Household vehicle", frame.TRPHHVEH.eq(1)),
        (
            "Identified vehicle",
            frame.VEHID.ne(-1) if not weighted else frame.VEHID.gt(0),
        ),
        (
            "Positive bounded trip distance",
            frame.TRPMILES.gt(0) & frame.TRPMILES.lt(rules["maximum_trip_miles"]),
        ),
        (
            "Positive bounded driver VMT",
            frame.VMT_MILE.gt(0) & frame.VMT_MILE.lt(rules["maximum_trip_miles"]),
        ),
        (
            "Positive bounded duration",
            frame.TRVLCMIN.gt(0) & frame.TRVLCMIN.lt(rules["maximum_trip_minutes"]),
        ),
        (
            "Known origin purpose",
            frame.WHYFROM.ge(rules["minimum_purpose_code"])
            & frame.WHYFROM.lt(rules["maximum_purpose_code_exclusive"]),
        ),
        (
            "Known destination purpose",
            frame.WHYTO.ge(rules["minimum_purpose_code"])
            & frame.WHYTO.lt(rules["maximum_purpose_code_exclusive"]),
        ),
        (
            "Valid start clock",
            hhmm.ge(0)
            if not weighted
            else hhmm.ge(0) & hhmm.lt(2400) & hhmm.mod(100).lt(60),
        ),
        (
            "Plausible average speed",
            speed.ge(rules["minimum_speed_mph"]) & speed.lt(rules["maximum_speed_mph"]),
        ),
    ]
    if weighted:
        criteria += [
            (
                "Positive finite trip weight",
                frame.WTTRDFIN.gt(0) & np.isfinite(frame.WTTRDFIN),
            ),
            (
                "Known day of week",
                frame.TRAVDAY.isin(settings["presentation"]["day_type_map"]),
            ),
        ]
    keep = pd.Series(True, index=frame.index)
    audit = [
        {
            "screen": "Duplicate selected trip records",
            "entering_rows": len(trips),
            "excluded_rows": len(trips) - len(frame),
            "remaining_rows": len(frame),
        }
    ]
    for label, criterion in criteria:
        before = int(keep.sum())
        keep &= criterion.fillna(False)
        audit.append(
            {
                "screen": label,
                "entering_rows": before,
                "excluded_rows": before - int(keep.sum()),
                "remaining_rows": int(keep.sum()),
            }
        )
    frame = frame.loc[keep].copy()
    frame["speed_mph"] = speed.loc[keep]
    frame["start_minute"] = frame.STRTTIME.floordiv(100) * 60 + frame.STRTTIME.mod(100)
    frame["midpoint_minute"] = (frame.start_minute + frame.TRVLCMIN / 2).mod(1440)
    frame["arrival_minute"] = (frame.start_minute + frame.TRVLCMIN).mod(1440)
    frame["day_type"] = frame.TRAVDAY.map(settings["presentation"]["day_type_map"])
    frame["purpose"] = np.where(
        frame.WHYTRP1S.isin(rules["occupation_purpose_codes"]), "Occupation", "Personal"
    )
    frame["activity"] = np.select(
        [
            frame.WHYFROM.isin(rules["work_location_codes"])
            | frame.WHYTO.isin(rules["work_location_codes"]),
            frame.WHYFROM.isin(rules["school_location_codes"])
            | frame.WHYTO.isin(rules["school_location_codes"]),
        ],
        ["To/from work", "To/from school"],
        default="Other activities",
    )
    frame["work_related"] = frame.WHYFROM.isin(
        rules["work_location_codes"]
    ) | frame.WHYTO.isin(rules["work_location_codes"])
    frame["school_related"] = frame.WHYFROM.isin(
        rules["school_location_codes"]
    ) | frame.WHYTO.isin(rules["school_location_codes"])
    frame = frame.merge(
        vehicles[["HOUSEID", "VEHID", "VEHFUEL"]],
        on=["HOUSEID", "VEHID"],
        how="left",
        validate="many_to_one",
        indicator=True,
    )
    if frame._merge.ne("both").any():
        raise ValueError(
            "Curated NHTS household-vehicle trips have unmatched vehicle keys"
        )
    frame = frame.drop(columns="_merge")
    frame["weight"] = 1.0 if not weighted else frame.WTTRDFIN
    return frame, pd.DataFrame(audit)


def _weighted_mean(frame: pd.DataFrame, column: str) -> float:
    return (
        float(np.average(frame[column], weights=frame.weight))
        if len(frame)
        else float("nan")
    )


def day_type_weights(frame: pd.DataFrame, *, weighted: bool) -> pd.Series:
    """FHWA user guide §7.2: five-day weekday and two-day weekend trip weights."""
    weights = (
        pd.Series(1.0, index=frame.index)
        if not weighted
        else frame.WTTRDFIN5D.where(frame.day_type.eq("Weekday"), frame.WTTRDFIN2D)
    )
    if weights.le(0).any() or not np.isfinite(weights).all():
        raise ValueError(
            "NHTS day-specific trip weights must be positive and finite in the selected day domain"
        )
    return weights


def electric_distance_potential(
    distances: np.ndarray, weights: np.ndarray, ranges: list[float]
) -> pd.DataFrame:
    """Exact empirical E[min(distance, range)] / E[distance], without histogram bias."""
    if (
        not len(distances)
        or (distances <= 0).any()
        or not np.isfinite(distances).all()
        or (weights <= 0).any()
        or not np.isfinite(weights).all()
    ):
        raise ValueError(
            "Electric-distance potential requires finite positive daily distances and weights"
        )
    denominator = np.dot(distances, weights)
    return pd.DataFrame(
        [
            {
                "range_km": value,
                "distance_potential": float(
                    np.dot(np.minimum(distances, value), weights) / denominator
                ),
                "daily_distance_cdf": float(
                    weights[distances <= value].sum() / weights.sum()
                ),
            }
            for value in ranges
        ]
    )


def diagnose_tables(
    native: dict[str, pd.DataFrame],
    settings: dict,
    *,
    mile_to_km: float,
    methods: list[tuple[str, bool]] | None = None,
) -> dict[str, pd.DataFrame]:
    """Compute compact evidence with explicit, separately weighted grains."""
    output = {
        key: []
        for key in [
            "summary",
            "daily",
            "clock",
            "activity",
            "purpose",
            "distance",
            "utility",
            "exclusions",
            "fuel",
        ]
    }
    for method, weighted in (
        methods if methods is not None else [(DERIVATION, False), (WEIGHTED, True)]
    ):
        trips, exclusions = curate_trips(
            native["trip"], native["vehicle"], settings, weighted=weighted
        )
        if trips.empty:
            raise ValueError(f"No NHTS records remain for {method}")
        trips["distance_km"] = trips.TRPMILES * mile_to_km
        output["exclusions"].append(exclusions.assign(method=method))
        keys = (
            ["HOUSEID", "PERSONID", "VEHID", "TRAVDAY"]
            if not weighted
            else ["HOUSEID", "PERSONID", "TRAVDAY"]
        )
        daily = trips.groupby(keys, as_index=False).agg(
            distance_km=("distance_km", "sum"), day_type=("day_type", "first")
        )
        if not weighted:
            daily["weight"] = 1.0
        else:
            daily = daily.merge(
                native["person"][["HOUSEID", "PERSONID", "WTPERFIN"]],
                on=["HOUSEID", "PERSONID"],
                how="left",
                validate="many_to_one",
            )
            if (
                daily.WTPERFIN.isna().any()
                or daily.WTPERFIN.le(0).any()
                or not np.isfinite(daily.WTPERFIN).all()
            ):
                raise ValueError(
                    "NHTS travelling driver days require positive matched person weights"
                )
            daily["weight"] = daily.WTPERFIN
        daily["method"] = method
        output["distance"].append(daily)
        potential = electric_distance_potential(
            daily.distance_km.to_numpy(),
            daily.weight.to_numpy(),
            settings["presentation"]["electric_ranges_km"],
        )
        output["utility"].append(
            potential.assign(method=method, observed_days=len(daily))
        )
        output["summary"].append(
            pd.DataFrame(
                [
                    {
                        "method": method,
                        "native_trips": len(native["trip"]),
                        "retained_trips": len(trips),
                        "retained_share": len(trips) / len(native["trip"]),
                        "range_days": len(daily),
                        "trip_mean_km": _weighted_mean(trips, "distance_km"),
                        "trip_mean_minutes": _weighted_mean(trips, "TRVLCMIN"),
                        "range_day_mean_km": _weighted_mean(daily, "distance_km"),
                        "trip_weight_sum": float(trips.weight.sum()),
                        "daily_weight_sum": float(daily.weight.sum()),
                        "distance_vmt_mismatch_rows": int(
                            trips.TRPMILES.ne(trips.VMT_MILE).sum()
                        ),
                        "range_day_grain": "travelling person-vehicle day"
                        if not weighted
                        else "travelling driver-person day",
                        "trip_weight": "none" if not weighted else "WTTRDFIN",
                        "daily_weight": "none" if not weighted else "WTPERFIN",
                    }
                ]
            )
        )
        person_days = trips.groupby(
            ["HOUSEID", "PERSONID", "TRAVDAY"], as_index=False
        ).agg(distance_km=("distance_km", "sum"), day_type=("day_type", "first"))
        if not weighted:
            person_days["weight"] = 1.0
        else:
            person_days = person_days.merge(
                native["person"][["HOUSEID", "PERSONID", "WTPERFIN"]],
                on=["HOUSEID", "PERSONID"],
                validate="many_to_one",
            )
            person_days["weight"] = person_days.WTPERFIN
        output["summary"][-1]["travelling_person_days"] = len(person_days)
        output["summary"][-1]["person_day_mean_km"] = _weighted_mean(
            person_days, "distance_km"
        )
        for day_label, day_mask in [
            (name, person_days.day_type.eq(name))
            for name in settings["presentation"]["day_types"]
        ] + [("Weekend", person_days.day_type.isin(["Saturday", "Sunday"]))]:
            part = person_days.loc[day_mask]
            if len(part):
                output["daily"].append(
                    pd.DataFrame(
                        [
                            {
                                "method": method,
                                "day_type": day_label,
                                "person_days": len(part),
                                "mean_km": _weighted_mean(part, "distance_km"),
                                "weight_sum": float(part.weight.sum()),
                                "weighting": "Unweighted"
                                if not weighted
                                else "Custom person-weighted conditional distribution",
                            }
                        ]
                    )
                )
        width = settings["presentation"]["clock_bins_minutes"]
        for clock in ["start", "midpoint", "arrival"]:
            clock_frame = trips.assign(
                hour=trips[f"{clock}_minute"].floordiv(width) * width / 60,
                weight=day_type_weights(trips, weighted=weighted),
            )
            for day, part in clock_frame.groupby("day_type"):
                group = (
                    part.groupby("hour")
                    .agg(observations=("weight", "size"), weight=("weight", "sum"))
                    .reindex(np.arange(0, 1440, width) / 60, fill_value=0)
                    .reset_index()
                )
                group["share"] = group.weight / part.weight.sum()
                output["clock"].append(
                    group.assign(
                        method=method,
                        day_type=day,
                        clock=clock,
                        denominator_rows=len(part),
                    )
                )
        for (day, purpose), part in trips.assign(
            weight=day_type_weights(trips, weighted=weighted)
        ).groupby(["day_type", "purpose"]):
            output["purpose"].append(
                pd.DataFrame(
                    [
                        {
                            "method": method,
                            "day_type": day,
                            "purpose": purpose,
                            "trips": len(part),
                            "mean_km": _weighted_mean(part, "distance_km"),
                            "mean_minutes": _weighted_mean(part, "TRVLCMIN"),
                            "weight_sum": float(part.weight.sum()),
                        }
                    ]
                )
            )
        activity_groups = [
            ("Worker trips", trips.loc[trips.work_related]),
            ("Student trips", trips.loc[trips.school_related]),
            ("Inactive trips", trips.loc[~trips.work_related & ~trips.school_related]),
        ]
        for activity, part in activity_groups:
            if part.empty:
                continue
            activity_width = settings["presentation"]["activity_bins_minutes"]
            clock_group = (
                part.assign(
                    hour=part.start_minute.floordiv(activity_width)
                    * activity_width
                    / 60
                )
                .groupby("hour")
                .agg(observations=("weight", "size"), weight=("weight", "sum"))
                .reindex(np.arange(0, 1440, activity_width) / 60, fill_value=0)
                .reset_index()
            )
            clock_group["hour_end"] = clock_group.hour + activity_width / 60
            clock_group["share"] = clock_group.weight / part.weight.sum()
            output["activity"].append(
                clock_group.assign(
                    method=method, activity=activity, denominator_rows=len(part)
                )
            )
            plug_in = part.VEHFUEL.isin(settings["filters"]["plug_in_fuel_codes"])
            output["fuel"].append(
                pd.DataFrame(
                    [
                        {
                            "method": method,
                            "activity": activity,
                            "trips": len(part),
                            "plug_in_trips": int(plug_in.sum()),
                            "trip_share": float(part.weight.sum() / trips.weight.sum()),
                            "plug_in_trip_share": float(
                                part.loc[plug_in, "weight"].sum() / part.weight.sum()
                            ),
                        }
                    ]
                )
            )
    return {key: pd.concat(frames, ignore_index=True) for key, frames in output.items()}


def read_ramp_reference(
    bundle: ConfigBundle, settings: dict
) -> tuple[dict[str, pd.DataFrame], dict]:
    """Authenticate aggregate inputs against frozen, source-registered run metadata."""
    selection = settings["ramp_reference"]
    request = build_request(bundle, selection["survey"], strategy=selection["strategy"])
    metadata = validate_run_metadata(request)
    directory = resolve_artifact_path(
        bundle, "ramp_mobility_external", selection["database_subdirectory"]
    )
    fingerprints = metadata["input_sha256"]
    paths, identities = {}, {}
    for role, filename in selection["files"].items():
        expected = [
            value
            for key, value in fingerprints.items()
            if key.replace("\\", "/").split("/")[-1] == filename
        ]
        path = directory / filename
        if len(expected) != 1 or not path.is_file() or file_sha256(path) != expected[0]:
            raise ValueError(
                f"Registered RAMP {filename} differs from authenticated run inputs"
            )
        paths[role] = path
        identities[filename] = expected[0]
    country = selection["country"]
    raw_windows = pd.read_csv(paths["windows"], header=[0, 1])
    main = raw_windows.loc[raw_windows.iloc[:, 1].eq(selection["main_activity"])].copy()
    windows = pd.DataFrame(
        {
            "activity": main.iloc[:, 0].map(selection["proxy_labels"]),
            "window_number": pd.to_numeric(main.iloc[:, 2], errors="raise"),
            "start_hour": pd.to_numeric(main[(country, "Start")], errors="raise"),
            "end_hour": pd.to_numeric(main[(country, "End")], errors="raise"),
        }
    )
    if (
        windows.activity.isna().any()
        or windows.duplicated(["activity", "window_number"]).any()
        or not (
            windows.start_hour.ge(0)
            & windows.end_hour.le(24)
            & windows.start_hour.lt(windows.end_hour)
        ).all()
    ):
        raise ValueError(
            "Registered RAMP main windows have invalid identities or day boundaries"
        )
    windows["label"] = windows.apply(
        lambda row: (
            f"{int(row.start_hour):02d}:{round(row.start_hour % 1 * 60):02d}–{int(row.end_hour):02d}:{round(row.end_hour % 1 * 60):02d}"
        ),
        axis=1,
    )
    result = {
        "windows": windows.reset_index(drop=True),
        "daily": pd.read_csv(paths["daily"], index_col=0),
        "distance": pd.read_csv(paths["distance"], index_col=[0, 1]),
        "duration": pd.read_csv(paths["duration"], index_col=[0, 1]),
    }
    return result, {
        "survey": selection["survey_label"],
        "country": country,
        "strategy": selection["strategy"],
        "run_metadata_sha256": request.metadata_sha256,
        "input_sha256": identities,
        "selection": "Frozen aggregate input values are read for comparison only; simulations and model inputs are not rewritten",
    }


def parameter_comparison(
    tables: dict[str, pd.DataFrame], references: dict[str, pd.DataFrame], settings: dict
) -> pd.DataFrame:
    """Place current survey derivation beside the values used by registered runs."""
    selection = settings["ramp_reference"]
    rows = []
    daily = tables["daily"].loc[tables["daily"].method.eq(DERIVATION)]
    for day in ["Weekday", "Weekend"]:
        row = daily.loc[daily.day_type.eq(day)].iloc[0]
        reference = float(references["daily"].loc[selection["country"], day.lower()])
        rows.append(
            {
                "parameter": selection["files"]["daily"],
                "day_type": day,
                "purpose": "All retained trips",
                "derived_value": float(row.mean_km),
                "run_input_value": reference,
                "difference": float(row.mean_km) - reference,
                "unit": "km/person-day",
                "observations": int(row.person_days),
            }
        )
    purposes = tables["purpose"].loc[tables["purpose"].method.eq(DERIVATION)]
    inverse_purpose = {value: key for key, value in selection["purpose_labels"].items()}
    for row in purposes.itertuples():
        key = (inverse_purpose[row.purpose], row.day_type.lower())
        for role, field, unit in [
            ("distance", "mean_km", "km/trip"),
            ("duration", "mean_minutes", "min/trip"),
        ]:
            derived = float(getattr(row, field))
            reference = float(references[role].loc[key, selection["country"]])
            rows.append(
                {
                    "parameter": selection["files"][role],
                    "day_type": row.day_type,
                    "purpose": row.purpose,
                    "derived_value": derived,
                    "run_input_value": reference,
                    "difference": derived - reference,
                    "unit": unit,
                    "observations": int(row.trips),
                }
            )
    return pd.DataFrame(rows)


def build_diagnostic(
    bundle: ConfigBundle | None = None, *, write: bool = False
) -> TravelSurveyDiagnostic:
    if bundle is None:
        root = find_repo_root()
        from utils import load_yaml

        settings = load_yaml(root / "config/parameters/travel_survey.yaml")
        bundle = load_config_bundle(root / settings["scenario"])
    settings = load_parameter_yaml(bundle, "travel_survey.yaml")
    prepare = "Run uv run --offline python -m fetching.nhts_ldv_travel " \
        f"--scenario {settings['scenario']} from the repository root."
    requests_to_read = build_requests(bundle)
    for request in requests_to_read:
        validate_cache(request)
    directory = resolve_artifact_path(bundle, "nhts_interim")
    files = settings["normalization"]["files"]
    manifest_path = directory / files["manifest"]
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing normalized NHTS evidence at {manifest_path}. {prepare}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest["edition"] != settings["edition"] or manifest["source"] != SOURCE:
        raise ValueError(f"NHTS normalized manifest identifies the wrong source edition. {prepare}")
    native = {}
    for name in ["trip", "vehicle", "person"]:
        path = directory / files[name]
        if not path.is_file():
            raise FileNotFoundError(f"Missing normalized NHTS table at {path}. {prepare}")
        if file_sha256(path) != manifest["normalized_sha256"][name]:
            raise ValueError(f"NHTS normalized {name} changed after ingestion. {prepare}")
        native[name] = pd.read_csv(path)
    # Reconcile normalized evidence with registered physical data, not only a self-reported manifest.
    registered = read_tables(
        next(r for r in requests_to_read if r.component == "public_use"),
        settings["normalization"]["columns"],
    )
    for name in native:
        pd.testing.assert_frame_equal(
            native[name],
            registered[name],
            check_dtype=False,
            check_exact=False,
            rtol=1e-12,
        )
    conversion = load_conversion_factors(bundle)["length"]["mile_to_km"]
    tables = diagnose_tables(native, settings, mile_to_km=conversion)
    references, reference_audit = read_ramp_reference(bundle, settings)
    tables["windows"] = references["windows"]
    tables["parameters"] = parameter_comparison(tables, references, settings)
    metadata = {
        "source": manifest,
        "mile_to_km": conversion,
        "settings_sha256": file_sha256(
            resolve_parameter_path(bundle, "travel_survey.yaml")
        ),
        "conversion_sha256": file_sha256(
            resolve_parameter_path(bundle, "conversion.yaml")
        ),
        "ramp_reference": reference_audit,
        "derivation": "Reviewed unweighted screens applied only to the maintained 2022 V2.1 public-use release. DRIVER=1 is respondent driver status; positive VMT screens are preserved. No historical raw files or notebooks are read.",
        "trip_estimate": "Optional survey-weighted context uses DRVR_FLG=1; WTTRDFIN for annual trip means/activity windows, WTTRDFIN5D for weekday timing/purpose, WTTRDFIN2D for Saturday/Sunday timing/purpose, following FHWA user guide §7.2.",
        "daily_estimate": "d_tot: sum retained trip miles by HOUSEID/PERSONID, then average positive-travel person days within the day domain. The range illustration preserves person-vehicle-day grain for unweighted derivation. Optional WTPERFIN daily distributions are exploratory, not the official trip-weighted miles estimator.",
        "parameter_mapping": {
            "d_tot.csv": "Mean positive-travel person-day distance by weekday/weekend (km)",
            "d_min.csv": "Mean trip distance by occupation/personal purpose and day type (km)",
            "t_func.csv": "Mean trip duration by occupation/personal purpose and day type (minutes)",
            "trips_by_time_*.csv": "Trip midpoint-hour occurrence fractions, separately normalized within day type",
            "windows.csv": "Reviewed nominal mobility windows, not estimated automatically from the histogram",
        },
        "utility_interpretation": "Idealized E[min(D,R)]/E[D], not observed PHEV utility; one full charge per retained travelling day, no access/efficiency/battery constraints.",
        "activity_interpretation": "Worker proxy: either trip endpoint work code2–5; student proxy: either endpoint school code6; inactive proxy: neither. Work-to-school trips overlap. These are trip-activity proxies, not person employment estimates.",
        "window_interpretation": "Authenticated CA Main windows from the frozen RAMP run database are shaded as nominal reviewed assumptions. They are not threshold fits or strict observed limits. Working/student classes use inactive windows on weekends; model variation can move nominal boundaries.",
        "clock_interpretation": "Departure uses native local HHMM; midpoint/arrival add reported duration modulo24h. RAMP occurrence derivation uses midpoint hour; activity-window histograms use departure in48 half-hour bins.",
        "limitations": [
            "US national survey does not establish Ontario representativeness",
            "No replicate-weight confidence intervals",
            "Zero-travel days are absent",
            "Current survey-derived values are diagnostic comparisons; frozen simulation inputs are not changed",
        ],
    }
    if write:
        output = resolve_artifact_path(bundle, "travel_survey_diagnostic")
        for name, frame in tables.items():
            write_dataframe_atomic(
                frame, output / settings["presentation"]["files"][name]
            )
        write_text_atomic(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n",
            output / settings["presentation"]["files"]["manifest"],
        )
        LOGGER.info(
            "Wrote NHTS research diagnosis to %s; no production inputs changed", output
        )
    return TravelSurveyDiagnostic(tables, metadata, settings)


def clock_chart(
    diagnostic: TravelSurveyDiagnostic,
    *,
    method: str,
    days: list[str],
    clock: str = "start",
):
    frame = diagnostic.tables["clock"]
    frame = frame.loc[
        frame.method.eq(method) & frame.day_type.isin(days) & frame.clock.eq(clock)
    ]
    p = diagnostic.settings["presentation"]
    chart = (
        alt.Chart(frame)
        .mark_line(strokeWidth=2.5)
        .encode(
            x=alt.X(
                "hour:Q",
                title="Local hour",
                scale=alt.Scale(domain=[0, 24]),
                axis=alt.Axis(values=list(range(0, 25, 2))),
            ),
            y=alt.Y(
                "share:Q",
                title="Share of trips within day type",
                axis=alt.Axis(format="%"),
            ),
            color=alt.Color(
                "day_type:N",
                title="Day type",
                scale=alt.Scale(
                    domain=p["day_types"], range=["#087e8b", "#cc6b49", "#756bb1"]
                ),
                legend=alt.Legend(orient="bottom", direction="horizontal"),
            ),
            tooltip=[
                "day_type:N",
                "hour:Q",
                alt.Tooltip("share:Q", format=".2%"),
                "observations:Q",
                "denominator_rows:Q",
            ],
        )
        .properties(width=p["line_width"], height=p["chart_height"])
    )
    return identified_chart(
        chart,
        "travel-survey-clock",
        f"When do LDV trips {dict(start='start', midpoint='reach their midpoint', arrival='arrive')[clock]}?",
    )


def utility_chart(diagnostic: TravelSurveyDiagnostic):
    frame = diagnostic.tables["utility"].melt(
        id_vars=["range_km", "method", "observed_days"],
        value_vars=["distance_potential", "daily_distance_cdf"],
        var_name="measure",
        value_name="share",
    )
    frame["measure"] = frame.measure.map(
        {
            "distance_potential": "Electric-distance potential",
            "daily_distance_cdf": "Days covered completely",
        }
    )
    p = diagnostic.settings["presentation"]
    chart = (
        alt.Chart(frame)
        .mark_line(strokeWidth=2.5)
        .encode(
            x=alt.X("range_km:Q", title="Electric range (km)"),
            y=alt.Y(
                "share:Q",
                title="Share",
                axis=alt.Axis(format="%"),
                scale=alt.Scale(domain=[0, 1]),
            ),
            color=alt.Color(
                "method:N",
                title="Daily cohort / weighting",
                legend=alt.Legend(orient="bottom", direction="horizontal"),
            ),
            strokeDash=alt.StrokeDash(
                "measure:N",
                title="Measure — line",
                legend=alt.Legend(
                    orient="bottom",
                    direction="horizontal",
                    symbolType="stroke",
                    symbolSize=250,
                ),
            ),
            detail=["method:N", "measure:N"],
            order="range_km:Q",
            tooltip=[
                "method:N",
                "measure:N",
                "range_km:Q",
                alt.Tooltip("share:Q", format=".1%"),
                "observed_days:Q",
            ],
        )
        .properties(width=p["line_width"], height=p["chart_height"])
    )
    return identified_chart(
        chart,
        "travel-survey-electric-distance",
        "Range covers more days than distance: two different denominators",
    )


def activity_chart(diagnostic: TravelSurveyDiagnostic, *, method: str):
    frame = diagnostic.tables["activity"]
    p = diagnostic.settings["presentation"]
    panels = []
    bin_minutes = p["activity_bins_minutes"]
    bin_count = 1440 // bin_minutes
    y_max = float(frame.loc[frame.method.eq(method), "share"].max()) * 1.18
    for activity in p["activity_order"]:
        part = frame.loc[frame.method.eq(method) & frame.activity.eq(activity)]
        windows = diagnostic.tables["windows"].loc[
            diagnostic.tables["windows"].activity.eq(activity)
        ]
        bands = (
            alt.Chart(windows)
            .mark_rect(color="#83b783", opacity=0.22)
            .encode(
                x=alt.X(
                    "start_hour:Q",
                    scale=alt.Scale(domain=[0, 24], nice=False, padding=0),
                ),
                x2="end_hour:Q",
            )
        )
        bars = (
            alt.Chart(part)
            .mark_bar(color="#326b8c", opacity=0.9, orient="vertical")
            .encode(
                x=alt.X(
                    "hour:Q",
                    title="Departure (local hour)",
                    scale=alt.Scale(domain=[0, 24], nice=False, padding=0),
                    axis=alt.Axis(values=list(range(0, 25, 2))),
                ),
                x2="hour_end:Q",
                y=alt.Y(
                    "share:Q",
                    title="Within-proxy trips",
                    axis=alt.Axis(format="%"),
                    scale=alt.Scale(domain=[0, y_max]),
                ),
                y2=alt.datum(0),
                tooltip=[
                    "activity:N",
                    "hour:Q",
                    "hour_end:Q",
                    alt.Tooltip("share:Q", format=".2%"),
                    "observations:Q",
                    "denominator_rows:Q",
                ],
            )
        )
        labels = (
            alt.Chart(windows)
            .mark_text(
                align="left", baseline="top", dx=4, dy=4, color="#285b2c", fontSize=11
            )
            .encode(
                x=alt.X(
                    "start_hour:Q",
                    scale=alt.Scale(domain=[0, 24], nice=False, padding=0),
                ),
                y=alt.value(0),
                text="label:N",
            )
        )
        count = int(part.denominator_rows.iloc[0]) if len(part) else 0
        panels.append(
            (bands + bars + labels).properties(
                width=p["grid_width"],
                height=p["activity_height"],
                title=alt.Title(
                    activity,
                    subtitle=[
                        f"{count:,} captured trips - {bin_count} departure bins of {bin_minutes} minutes",
                        "Shading: nominal weekday main windows; weekends use inactive bands"
                        if activity != "Inactive trips"
                        else "Shading: nominal inactive main window across day types",
                    ],
                    anchor="start",
                ),
            )
        )
    return identified_chart(
        alt.vconcat(*panels, spacing=24).resolve_scale(y="shared"),
        "travel-survey-activities",
        "Interpret departures alongside reviewed RAMP mobility windows",
    )


def clock_heatmap(
    diagnostic: TravelSurveyDiagnostic,
    *,
    method: str,
    days: list[str],
    clock: str = "start",
):
    frame = diagnostic.tables["clock"]
    frame = frame.loc[
        frame.method.eq(method) & frame.day_type.isin(days) & frame.clock.eq(clock)
    ]
    p = diagnostic.settings["presentation"]
    chart = (
        alt.Chart(frame)
        .mark_rect()
        .encode(
            x=alt.X("hour:O", title="Local hour"),
            y=alt.Y("day_type:N", title=None, sort=p["day_types"]),
            color=alt.Color(
                "share:Q",
                title="Within-day trip share",
                scale=alt.Scale(scheme="tealblues"),
                legend=alt.Legend(orient="bottom", direction="horizontal", format="%"),
            ),
            tooltip=[
                "day_type:N",
                "hour:Q",
                alt.Tooltip("share:Q", format=".2%"),
                "observations:Q",
                "denominator_rows:Q",
            ],
        )
        .properties(width=p["heatmap_width"], height=120)
    )
    return identified_chart(
        chart, "travel-survey-clock-heatmap", "Trip timing by day type"
    )


def distance_chart(diagnostic: TravelSurveyDiagnostic, *, method: str):
    frame = diagnostic.tables["distance"]
    frame = frame.loc[frame.method.eq(method)].copy()
    edges = diagnostic.settings["presentation"]["distance_bins_km"]
    frame["bin"] = pd.cut(frame.distance_km, bins=edges, right=False)
    grouped = (
        frame.groupby("bin", observed=False)
        .agg(days=("weight", "size"), weight=("weight", "sum"))
        .reset_index()
    )
    grouped["distance_band"] = grouped.bin.astype(str)
    grouped["share"] = grouped.weight / frame.weight.sum()
    if not np.isclose(grouped.share.sum(), 1):
        raise ValueError(
            "Configured daily-distance bins do not cover all retained observations"
        )
    p = diagnostic.settings["presentation"]
    chart = (
        alt.Chart(grouped.drop(columns="bin"))
        .mark_bar(color="#087e8b")
        .encode(
            x=alt.X(
                "distance_band:O",
                title="Daily retained distance (km)",
                sort=grouped.distance_band.tolist(),
                axis=alt.Axis(labelAngle=-35),
            ),
            y=alt.Y(
                "share:Q",
                title="Share of positive-travel days",
                axis=alt.Axis(format="%"),
            ),
            tooltip=["distance_band:O", "days:Q", alt.Tooltip("share:Q", format=".1%")],
        )
        .properties(width=p["line_width"], height=p["chart_height"])
    )
    return identified_chart(
        chart,
        "travel-survey-daily-distance",
        "Most travelling days are short, with a consequential long-distance tail",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    diagnostic = build_diagnostic(load_config_bundle(args.scenario), write=True)
    print(diagnostic.tables["summary"].to_string(index=False))


if __name__ == "__main__":
    main()
