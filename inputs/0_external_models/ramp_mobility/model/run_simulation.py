"""Preflight or run one legacy survey/charging case; preflight is the default."""

import argparse
from datetime import datetime, timezone
import hashlib
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import random
import re
import shutil
import sys
import tomllib


ROOT = Path(__file__).resolve().parent
MODEL_REVISION = "ontario-2018-corrected-soc-weather-v1"


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_settings():
    return tomllib.loads((ROOT / "reruns.toml").read_text(encoding="utf-8"))


def parser(settings):
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--survey", choices=settings["surveys"], required=True)
    result.add_argument("--mode", choices=settings["strategies"], required=True)
    result.add_argument("--seed", type=int, default=settings["seed"])
    result.add_argument("--tag", default="", help="Optional distinct output name suffix")
    result.add_argument("--plots", action="store_true")
    result.add_argument("--run", action="store_true", help="Start the full-year simulation")
    return result


def output_directory(args, year):
    if args.tag and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", args.tag):
        raise ValueError("Tag must start with a letter/digit and contain only letters, digits, '.', '_' or '-'.")
    name = f"seed-{args.seed}" + (f"-{args.tag}" if args.tag else "")
    return ROOT / "results" / "reruns" / args.survey / args.mode / str(year) / name


def preflight(args, settings):
    """Construct real input objects and weather data without calling a simulation."""
    database = ROOT / settings["surveys"][args.survey]
    case = ROOT / settings["case_directory"]
    runtime = case / "ramp_mobility"
    year = settings["year"]
    if year != 2018:
        raise ValueError("This audited legacy case uses calendar 2018; audit calendar/weather behavior before changing year.")
    if not 0 <= args.seed <= 2**32 - 1:
        raise ValueError("Seed must be between 0 and 2**32 - 1 for NumPy's legacy generator.")
    output = output_directory(args, year)
    if settings.get("profile_storage") != "disk":
        raise ValueError("The audited annual runner requires disk-backed float64 profile storage")
    os.environ["RAMP_DATABASE_DIR"] = str(database)
    os.environ["MPLCONFIGDIR"] = str(ROOT / ".mplconfig")
    if not args.plots:
        os.environ["MPLBACKEND"] = "Agg"
    sys.path.insert(0, str(case))

    import numpy as np
    import pandas as pd

    random.seed(args.seed)
    np.random.seed(args.seed)
    # The input module reads tables and constructs User/Appliance definitions only.
    inputs = importlib.import_module("ramp_mobility.country_input_files.North America.CA")
    # Reload only when this function is reused within one process for another survey.
    if Path(inputs.inputfolder).resolve() != database.resolve():
        inputs = importlib.reload(inputs)
    from ramp_mobility.core_model.initialise import yearly_pattern
    from ramp_mobility.post_process import post_process as pp
    from ramp_mobility import utils

    for day, percentages in inputs.trips.items():
        if not np.isfinite(percentages).all() or (percentages < 0).any():
            raise ValueError(f"Invalid {day} hourly distribution")
        if not np.isclose(percentages.sum(), 1, atol=1e-6):
            raise ValueError(f"{day} trip shares do not sum to one")
    for mapping in (inputs.d_tot, inputs.d_min, inputs.t_func):
        for value in mapping.values():
            numbers = list(value.values()) if isinstance(value, dict) else [value]
            if not np.isfinite(numbers).all() or min(numbers) <= 0:
                raise ValueError("Distance and duration inputs must be finite and positive")
    powers = settings["charger_powers_kw"]
    probabilities = settings["charger_probabilities"]
    if (len(powers) != len(probabilities) or min(powers) <= 0
            or not np.isfinite(powers + probabilities).all()
            or min(probabilities) < 0 or not np.isclose(sum(probabilities), 1)):
        raise ValueError("Invalid charger powers/probabilities")
    for user in inputs.User_list:
        for appliance in user.App_list:
            if appliance.Battery_cap <= 0 or user.num_users < 0:
                raise ValueError("Invalid user/battery definition")

    temperature_file = database / "temp_ninja_pop_1980-2022.csv"
    pattern, dummy_days = yearly_pattern("CA", year)
    temperature = pp.temp_import("CA", year, str(temperature_file))
    start = pd.Timestamp(year=year, month=1, day=1) - pd.Timedelta(days=dummy_days)
    end = pd.Timestamp(year=year + 1, month=1, day=1) + pd.Timedelta(days=dummy_days)
    needed_temperature = temperature.loc[str(start):str(end)]
    if needed_temperature.empty or not np.isfinite(needed_temperature["CA"]).all():
        raise ValueError("Non-finite temperature evidence inside simulation/buffer dates")
    users = sum(user.num_users for user in inputs.User_list)
    scratch_bytes = len(pattern) * 1440 * users * 8
    required_free_bytes = scratch_bytes + 2 * 1024**3
    free_bytes = shutil.disk_usage(ROOT).free
    if free_bytes < required_free_bytes:
        raise ValueError(f"Insufficient disk space: need {required_free_bytes / 1024**3:.2f} GiB free for scratch and outputs")
    mode = settings["strategies"][args.mode]
    eligible_minutes = None
    if args.mode in ("day", "night"):
        minutes = pd.date_range(f"{year}-01-01", periods=1440, freq="min")
        eligible_minutes = len(utils.fixed_charge_window(minutes, mode))
    summary = {
        "survey": args.survey,
        "mode": mode,
        "year": year,
        "seed": args.seed,
        "database": str(database),
        "output": str(output),
        "nominal_users": inputs.tot_users,
        "constructed_users": users,
        "user_classes": len(inputs.User_list),
        "daily_distance_km": {day: float(value) for day, value in inputs.d_tot.items()},
        "dummy_days_each_side": dummy_days,
        "calendar_days_with_buffers": len(pattern),
        "ordinary_charging_minutes_per_day": eligible_minutes,
        "temperature_minutes_loaded": len(temperature),
        "temperature_nulls_outside_simulation": int(temperature["CA"].isna().sum()),
        "model_revision": MODEL_REVISION,
        "profile_storage": "disk-backed float64",
        "scratch_gib": round(scratch_bytes / 1024**3, 3),
        "required_free_disk_gib": round(required_free_bytes / 1024**3, 3),
        "warnings": [
            "Raw UTC export retains legacy padding; downstream checks select the full Toronto calendar.",
        ],
        "evidence_notes": [
            "SOC/grid efficiency and UTC weather lookups corrected; logistic plug-in behavior retained.",
        ],
    }
    if args.survey == "TTS2016":
        summary["evidence_notes"].append("User-accepted assumption: TTS weekday behavior repeated on weekends, with legacy activity probabilities.")
    else:
        summary["evidence_notes"].append("NHTS legacy sample remains unweighted; midpoint hours intentionally represent trip occurrence.")
    return summary, inputs, temperature_file, runtime


def main(argv=None):
    settings = load_settings()
    args = parser(settings).parse_args(argv)
    summary, inputs, temperature_file, runtime = preflight(args, settings)
    print(json.dumps(summary, indent=2))
    if not args.run:
        print("Preflight passed. No simulation started. Add --run to simulate this case.")
        return 0

    output = Path(summary["output"])
    # An existing run, including a failed/partial run, is never overwritten.
    output.mkdir(parents=True, exist_ok=False)
    metadata_file = output / "run_metadata.json"
    sources = sorted(Path(inputs.inputfolder).glob("*.csv"))
    code = sorted(runtime.rglob("*.py"))
    metadata = {
        "status": "started",
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "preflight": summary,
        "model_revision": MODEL_REVISION,
        "settings": settings,
        "input_sha256": {str(p.relative_to(ROOT)): sha256(p) for p in sources},
        "code_sha256": {str(p.relative_to(ROOT)): sha256(p) for p in code},
        "control_sha256": {p.name: sha256(p) for p in (
            ROOT / "run_simulation.py", ROOT / "reruns.toml", ROOT / "uv.lock",
        )},
        "python": sys.version,
        "packages": {name: importlib.metadata.version(name) for name in (
            "numpy", "pandas", "matplotlib", "holidays", "pytz",
        )},
    }
    metadata_file.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    previous_directory = Path.cwd()
    # Keep remaining legacy relative paths confined to the selected case.
    os.chdir(runtime)
    os.environ["RAMP_RESULTS_DIR"] = str(output)
    try:
        from ramp_mobility.ramp_run import main as simulate

        metadata["diagnostics"] = simulate(
            year=settings["year"],
            charging_mode=settings["strategies"][args.mode],
            logistic=settings["logistic"],
            infr_prob=settings["infrastructure_probability"],
            Ch_stations=(settings["charger_powers_kw"], settings["charger_probabilities"]),
            simulation_name="",
            temperature_file=temperature_file,
            plots=args.plots,
            profile_store=output / ".scratch",
        ) or {}
        expected = output / "North America" / "CA" / "Charging Profiles.csv"
        if not expected.is_file():
            raise RuntimeError(f"Missing charging output: {expected}")
        metadata["output_validation"] = validate_charging_output(expected, settings["year"])
        metadata["status"] = "complete"
        metadata["charging_profile_sha256"] = sha256(expected)
        metadata["quality_status"] = (
            "review_required" if metadata["diagnostics"].get("negative_soc_users", 0) else "passed"
        )
    except BaseException as error:
        metadata["status"] = "failed"
        metadata["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        os.chdir(previous_directory)
        metadata["scratch_cleaned"] = cleanup_scratch(output)
        metadata["finished_utc"] = datetime.now(timezone.utc).isoformat()
        metadata_file.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return 0


def cleanup_scratch(output):
    """Delete only known generated maps inside this run after their process/owner closes them."""
    output = Path(output).resolve()
    scratch = (output / ".scratch").resolve()
    if scratch != output / ".scratch" or not scratch.is_relative_to(ROOT.resolve()):
        raise ValueError("Scratch cleanup must stay inside the selected RAMP run")
    if not scratch.exists():
        return True
    try:
        for path in scratch.iterdir():
            if not re.fullmatch(r"user-\d{3}\.npy", path.name) or not path.is_file():
                return False
        for path in scratch.iterdir():
            path.unlink()
        scratch.rmdir()
        return True
    except OSError:
        # A failed child can retain a mapping until exit; the batch parent retries then.
        return False


def validate_charging_output(path, year):
    """Validate physical power samples over the complete Toronto calendar, excluding raw padding."""
    import numpy as np
    import pandas as pd

    profile = pd.read_csv(path, index_col=0)
    if list(profile.columns) != ["Charging Profile"]:
        raise ValueError("Expected one Charging Profile power column")
    if not all(str(value).endswith(("+00:00", "Z")) for value in profile.index):
        raise ValueError("Charging timestamps must explicitly identify UTC")
    timestamps = pd.to_datetime(profile.index, utc=True, errors="raise")
    if (timestamps.has_duplicates or not timestamps.is_monotonic_increasing
            or not (timestamps[1:] - timestamps[:-1] == pd.Timedelta(minutes=1)).all()):
        raise ValueError("Charging timestamps must be unique, ordered and continuous minute samples")
    local = timestamps.tz_convert("America/Toronto")
    start = pd.Timestamp(year=year, month=1, day=1, tz="America/Toronto")
    end = pd.Timestamp(year=year + 1, month=1, day=1, tz="America/Toronto")
    mask = (local >= start) & (local < end)
    selected = pd.to_numeric(profile.iloc[:, 0], errors="raise").to_numpy()[mask]
    expected = int((end - start).total_seconds() / 60)
    if (len(selected) != expected or not np.isfinite(selected).all() or (selected < 0).any()):
        raise ValueError("Charging output must cover the full Toronto year with finite nonnegative power")
    return {
        "calendar_timezone": "America/Toronto", "calendar_year": year,
        "minute_samples": len(selected), "aggregate_grid_energy_kwh": float(selected.sum() / 60),
        "peak_kw": float(selected.max()), "mean_kw": float(selected.mean()),
    }


if __name__ == "__main__":
    raise SystemExit(main())
