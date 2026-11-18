"""Run the six Ontario cases serially in fresh processes; default is preflight."""

import argparse
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import run_simulation as single


ROOT = Path(__file__).resolve().parent
IS_WINDOWS = sys.platform == "win32"
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
CASES = [(survey, mode) for survey in ("NHTS2022", "TTS2016")
         for mode in ("uncontrolled", "night", "day")]


def now():
    return datetime.now(timezone.utc).isoformat()


def safe_tag(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", value):
        raise ValueError("Batch tag must be 1–64 letters/digits/dots/underscores/hyphens and start with a letter/digit")
    return value


def fingerprint(settings, seed):
    files = [ROOT / name for name in ("run_all.py", "run_simulation.py", "reruns.toml", "uv.lock")]
    files += [ROOT / name for name in ("pyproject.toml", ".python-version", "uv.toml")
              if (ROOT / name).is_file()]
    files += sorted((ROOT / settings["case_directory"] / "ramp_mobility").rglob("*.py"))
    for folder in settings["surveys"].values():
        files += sorted((ROOT / folder).glob("*.csv"))
    return {"model_revision": single.MODEL_REVISION, "seed": seed,
            "files": {str(path.relative_to(ROOT)): single.sha256(path) for path in files}}


def save_state(path, state):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def set_execution_state(flags):
    import ctypes

    function = ctypes.windll.kernel32.SetThreadExecutionState
    function.argtypes = [ctypes.c_uint]
    function.restype = ctypes.c_uint
    return function(flags)


@contextmanager
def prevent_sleep():
    previous = None
    if IS_WINDOWS:
        previous = set_execution_state(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
        if not previous:
            raise OSError("Windows rejected the temporary system-awake request")
    try:
        yield
    finally:
        if previous is not None:
            set_execution_state(previous)


def child_command(survey, mode, seed, tag, run):
    command = [sys.executable, "-u", str(ROOT / "run_simulation.py"),
               "--survey", survey, "--mode", mode, "--seed", str(seed), "--tag", tag]
    return command + (["--run"] if run else [])


def run_process(command, log_path=None):
    with (log_path.open("x", encoding="utf-8") if log_path else nullcontext(None)) as stream:
        child = subprocess.Popen(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
                                 creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if IS_WINDOWS else 0)
        try:
            return child.wait()
        except KeyboardInterrupt as error:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
            error.child_returncode = child.returncode
            raise


def case_output(survey, mode, seed, tag, year):
    return single.output_directory(argparse.Namespace(survey=survey, mode=mode, seed=seed, tag=tag), year)


def successful_output(output):
    metadata = json.loads((output / "run_metadata.json").read_text(encoding="utf-8"))
    if metadata.get("status") != "complete" or metadata.get("model_revision") != single.MODEL_REVISION:
        raise ValueError(f"Run did not complete under the selected model revision: {output}")
    profile = output / "North America/CA/Charging Profiles.csv"
    if not profile.is_file() or single.sha256(profile) != metadata.get("charging_profile_sha256"):
        raise ValueError(f"Missing or changed charging profile: {profile}")
    return metadata["charging_profile_sha256"], metadata.get("quality_status", "unknown")


def main(argv=None):
    settings = single.load_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="Start simulations instead of preflights")
    parser.add_argument("--tag", default="", help="Batch/output identity; generated automatically for new runs")
    parser.add_argument("--seed", type=int, default=settings["seed"])
    parser.add_argument("--resume", action="store_true", help="Resume an interrupted/failed batch with the same tag and seed")
    parser.add_argument("--keep-going", action="store_true", help="Continue other cases after a case fails")
    args = parser.parse_args(argv)
    if args.resume and (not args.run or not args.tag):
        parser.error("--resume requires --run and the original --tag")
    if not 0 <= args.seed <= 2**32 - 1:
        parser.error("seed must be between 0 and 2**32 - 1")
    tag = safe_tag(args.tag or (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") if args.run else "preflight"))
    for survey, mode in CASES:
        if survey not in settings["surveys"] or mode not in settings["strategies"]:
            raise ValueError(f"Missing configured case: {survey}/{mode}")
    if not args.run:
        for survey, mode in CASES:
            print(f"Preflight: {survey}/{mode}", flush=True)
            code = run_process(child_command(survey, mode, args.seed, tag, False))
            if code:
                return code
        print("All six preflights passed. No simulations started.", flush=True)
        return 0

    identity = fingerprint(settings, args.seed)
    batch = ROOT / "results/batches" / tag
    state_file = batch / "batch_metadata.json"
    if args.resume:
        state = json.loads(state_file.read_text(encoding="utf-8"))
        if state.get("identity") != identity:
            raise ValueError("Batch code/config/inputs/seed changed; use a new tag rather than resume")
        if state.get("status") == "running":
            raise ValueError("Batch is marked running; stop the original batch before resuming")
        if [(item["survey"], item["mode"]) for item in state["cases"]] != CASES:
            raise ValueError("Saved batch case order differs from this six-case batch")
        # Check every completed result before starting any retry.
        for item in state["cases"]:
            if item["status"] == "complete":
                attempt = item["attempts"][-1]
                output = case_output(item["survey"], item["mode"], args.seed, attempt["tag"], settings["year"])
                digest, _ = successful_output(output)
                if digest != attempt.get("profile_sha256"):
                    raise ValueError(f"Completed batch output changed: {output}")
                if not single.cleanup_scratch(output):
                    raise RuntimeError(f"Could not clean completed case scratch: {output}")
        state["resumed_utc"] = now()
    else:
        for survey, mode in CASES:
            if case_output(survey, mode, args.seed, tag, settings["year"]).exists():
                raise FileExistsError(f"Case output already exists for {survey}/{mode}; choose a new tag")
        batch.mkdir(parents=True, exist_ok=False)
        state = {"tag": tag, "identity": identity, "started_utc": now(),
                 "cases": [{"survey": survey, "mode": mode, "status": "pending", "attempts": []}
                           for survey, mode in CASES]}
    (batch / "logs").mkdir(exist_ok=True)
    state.update(status="running", parent_pid=os.getpid())
    save_state(state_file, state)
    print(f"Batch {tag}; state: {state_file}", flush=True)
    try:
        with prevent_sleep():
            for index, item in enumerate(state["cases"], 1):
                survey, mode = item["survey"], item["mode"]
                if item["status"] == "complete":
                    print(f"[{index}/6] Verified completed case; skipping {survey}/{mode}", flush=True)
                    continue
                if fingerprint(settings, args.seed) != identity:
                    raise ValueError("Code/config/inputs changed during batch; refusing mixed runs")
                for previous in item["attempts"]:
                    old_output = case_output(survey, mode, args.seed, previous["tag"], settings["year"])
                    if not single.cleanup_scratch(old_output):
                        raise RuntimeError(f"Could not clean prior generated scratch: {old_output}")
                number = len(item["attempts"]) + 1
                attempt_tag = tag if number == 1 else f"{tag}-retry-{number}"
                output = case_output(survey, mode, args.seed, attempt_tag, settings["year"])
                while output.exists():
                    number += 1
                    attempt_tag = f"{tag}-retry-{number}"
                    output = case_output(survey, mode, args.seed, attempt_tag, settings["year"])
                log = batch / "logs" / f"{index:02d}-{survey}-{mode}-attempt-{number}.log"
                attempt = {"tag": attempt_tag, "output": str(output), "log": str(log),
                           "started_utc": now(), "status": "running", "returncode": None}
                item["attempts"].append(attempt)
                item["status"] = "running"
                save_state(state_file, state)
                print(f"[{index}/6] Starting {survey}/{mode}; log: {log}", flush=True)
                try:
                    attempt["returncode"] = run_process(child_command(survey, mode, args.seed, attempt_tag, True), log)
                    if attempt["returncode"]:
                        raise RuntimeError(f"Child exited with code {attempt['returncode']}")
                    digest, quality = successful_output(output)
                    attempt.update(status="complete", profile_sha256=digest, quality_status=quality)
                except KeyboardInterrupt as error:
                    attempt.update(status="interrupted", returncode=getattr(error, "child_returncode", None))
                    raise
                except Exception as error:
                    attempt.update(status="failed", error=f"{type(error).__name__}: {error}")
                finally:
                    attempt["scratch_cleaned"] = single.cleanup_scratch(output)
                    attempt["finished_utc"] = now()
                    item["status"] = attempt["status"]
                    save_state(state_file, state)
                print(f"[{index}/6] {item['status']}: {survey}/{mode}", flush=True)
                if not attempt["scratch_cleaned"]:
                    raise RuntimeError(f"Generated scratch could not be cleaned after child exit: {output}")
                if item["status"] != "complete" and not args.keep_going:
                    break
        failed = any(item["status"] != "complete" for item in state["cases"])
        review = any(item["attempts"][-1].get("quality_status") == "review_required"
                     for item in state["cases"] if item["status"] == "complete")
        state["status"] = "failed" if failed else "complete_with_review" if review else "complete"
        return 1 if failed else 0
    except KeyboardInterrupt:
        state["status"] = "interrupted"
        return 130
    except Exception as error:
        state.update(status="failed", error=f"{type(error).__name__}: {error}")
        print(state["error"], flush=True)
        return 1
    finally:
        state["finished_utc"] = now()
        save_state(state_file, state)
        print(f"Batch {state['status']}; details: {state_file}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
