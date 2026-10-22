"""Build observability: elapsed wall time, persistent logs and safe destinations."""

import logging
import re
from dataclasses import replace
from datetime import UTC, datetime

import pytest

import utils
from utils import build_logging
from utils.build_logging import scenario_build_log
from validation.config_models import OutputPaths


def test_timing_records_elapsed_time_and_preserves_result(caplog, monkeypatch):
    ticks = iter((10.0, 12.5))
    monkeypatch.setattr(utils, "perf_counter", lambda: next(ticks))
    logger = logging.getLogger("test.build")

    @utils.log_duration(logger, "Parameter preparation")
    def prepare():
        return ["validated row"]

    with caplog.at_level(logging.INFO):
        assert prepare() == ["validated row"]
    assert caplog.messages == ["Parameter preparation completed in 2.50 s"]


def test_timing_reports_failure_without_success_or_swallowing_error(caplog, monkeypatch):
    ticks = iter((10.0, 13.25))
    monkeypatch.setattr(utils, "perf_counter", lambda: next(ticks))
    error = ValueError("Invalid source")
    with caplog.at_level(logging.INFO), pytest.raises(ValueError) as caught:
        with utils.log_duration(logging.getLogger("test.build"), "Source validation"):
            raise error
    assert caught.value is error
    assert caplog.messages == ["Source validation failed after 3.25 s"]
    assert caplog.records[0].levelno == logging.ERROR


def test_reloading_configuration_keeps_full_mapping_at_debug(caplog):
    with caplog.at_level(logging.INFO):
        utils.load_config_bundle("config/scenarios/default.yaml")
        utils.load_config_bundle("config/scenarios/default.yaml")
    assert not any("Scenario period mapping" in line or "DQ indicators" in line for line in caplog.messages)

    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        utils.load_config_bundle("config/scenarios/default.yaml")
    assert any("Scenario period mapping" in line for line in caplog.messages)
    assert any("DQ indicators" in line for line in caplog.messages)


@pytest.fixture
def log_bundle(tmp_path):
    """Keep every writable path local while retaining the real typed configuration."""
    bundle = utils.load_config_bundle("config/scenarios/default.yaml")
    yaml_content = bundle.scenario_path.read_bytes()
    bundle = replace(
        bundle,
        repo_root=tmp_path,
        paths_path=tmp_path / "config/paths.yaml",
        sources_path=tmp_path / "config/sources.yaml",
        scenario_path=tmp_path / "config/scenarios/default.yaml",
    )
    bundle.scenario_path.parent.mkdir(parents=True)
    bundle.scenario_path.write_bytes(yaml_content)
    return bundle


def _with_outputs(bundle, **updates):
    return replace(
        bundle,
        paths=bundle.paths.model_copy(update={
            "outputs": bundle.paths.outputs.model_copy(update=updates),
        }),
    )


@pytest.mark.parametrize("debug", [False, True])
def test_persistent_log_records_module_severity_and_utc_timestamp(log_bundle, caplog, debug):
    logger = logging.getLogger("test.build.module")
    with caplog.at_level(logging.WARNING):
        root = logging.getLogger()
        original_handlers = list(root.handlers)
        with scenario_build_log(log_bundle, debug=debug) as log_path:
            logger.debug("Detailed source mapping")
            logger.info("Prepared input")
            logger.warning("Optional evidence skipped")
            logger.error("Rejected a parameter row")
        assert root.level == logging.WARNING
        assert root.handlers == original_handlers

    content = log_path.read_text(encoding="utf-8")
    assert "INFO test.build.module: Prepared input" in content
    assert "WARNING test.build.module: Optional evidence skipped" in content
    assert "ERROR test.build.module: Rejected a parameter row" in content
    assert ("Detailed source mapping" in content) == debug
    assert f"scenario={log_bundle.scenario.scenario.name}" in content
    assert f"verbosity={'DEBUG' if debug else 'INFO'}" in content
    assert re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z INFO", content)


def test_each_attempt_has_its_own_log_captures_total_timing_and_closes_handlers(log_bundle, caplog):
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    streams = []
    paths = []
    with caplog.at_level(logging.WARNING):
        for attempt in range(2):
            with scenario_build_log(log_bundle) as log_path:
                paths.append(log_path)
                new_handlers = [handler for handler in root.handlers if handler not in original_handlers]
                assert len(new_handlers) == 1
                streams.append(new_handlers[0].stream)
                with utils.log_duration(logging.getLogger("test.build"), "Total scenario build"):
                    logging.getLogger("test.build").info("Attempt %s", attempt)
            assert root.handlers == original_handlers
            assert root.level == logging.WARNING
    assert all(stream.closed for stream in streams)
    assert len(set(paths)) == 2
    for attempt, path in enumerate(paths):
        content = path.read_text(encoding="utf-8")
        assert content.count("Scenario build started:") == 1
        assert content.count("Scenario build completed:") == 1
        assert content.count("Total scenario build completed in") == 1
        assert content.count(f"Attempt {attempt}") == 1
        assert f"Attempt {1-attempt}" not in content


def test_file_log_threshold_preserves_verbose_caller(log_bundle, caplog):
    with caplog.at_level(logging.DEBUG):
        with scenario_build_log(log_bundle) as log_path:
            assert logging.getLogger().level == logging.DEBUG
            logging.getLogger("test.build").debug("Caller debug details")
        assert logging.getLogger().level == logging.DEBUG
    assert "Caller debug details" in caplog.messages
    assert "Caller debug details" not in log_path.read_text(encoding="utf-8")


def test_log_failure_keeps_traceback_and_restores_logging(log_bundle, caplog):
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    error = ValueError("Source coverage is incomplete")
    with caplog.at_level(logging.WARNING):
        with pytest.raises(ValueError) as caught:
            with scenario_build_log(log_bundle) as log_path:
                handler = next(handler for handler in root.handlers if handler not in original_handlers)
                stream = handler.stream
                raise error
        assert caught.value is error
        assert root.level == logging.WARNING
        assert root.handlers == original_handlers
    assert stream.closed
    content = log_path.read_text(encoding="utf-8")
    assert "Scenario build failed:" in content
    assert "Traceback (most recent call last):" in content
    assert "test_log_failure_keeps_traceback_and_restores_logging" in content
    assert "ValueError: Source coverage is incomplete" in content
    assert "Scenario build completed:" not in content


@pytest.mark.parametrize("destination", ["outputs/escaped.log", "outputs/logs/../escaped.log", "outputs/logs"])
def test_log_rejects_destinations_outside_log_directory_before_writing(log_bundle, destination):
    bundle = _with_outputs(log_bundle, build_log=destination)
    with pytest.raises(ValueError, match="must be a file within"):
        with scenario_build_log(bundle):
            pytest.fail("Unsafe log context was entered")
    assert not (bundle.repo_root / "outputs").exists()


@pytest.mark.parametrize("collision", [
    "database", "validation", "setup", "reference", "scenario", "paths", "sources", "parameter",
])
def test_log_rejects_artifact_and_configuration_collisions_before_append(log_bundle, collision):
    root = log_bundle.repo_root
    paths = {
        "database": root / log_bundle.paths.outputs.sqlite / log_bundle.scenario.outputs.sqlite_name,
        "validation": root / log_bundle.scenario.outputs.validation_report,
        "setup": root / log_bundle.scenario.outputs.setup_log,
        "reference": root / log_bundle.scenario.comparison.reference_sqlite,
        "scenario": log_bundle.scenario_path,
        "paths": log_bundle.paths_path,
        "sources": log_bundle.sources_path,
        "parameter": root / log_bundle.paths.config.parameters / "rules.yaml",
    }
    target = paths[collision]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("Original artifact\n", encoding="utf-8")
    bundle = _with_outputs(log_bundle, build_log=str(target))
    bundle = replace(bundle, paths=bundle.paths.model_copy(update={
        "outputs": bundle.paths.outputs.model_copy(update={"logs": "."}),
    }))
    with pytest.raises(ValueError, match="must differ from|must not replace"):
        with scenario_build_log(bundle):
            pytest.fail("Colliding log context was entered")
    assert target.read_text(encoding="utf-8") == "Original artifact\n"


def test_log_rejects_hardlink_to_output_report(log_bundle):
    log_bundle = _with_outputs(log_bundle, build_log="outputs/logs/hardlink.log")
    report = log_bundle.repo_root / log_bundle.scenario.outputs.validation_report
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("Original report\n", encoding="utf-8")
    log_path = log_bundle.repo_root / "outputs/logs/hardlink.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        log_path.hardlink_to(report)
    except OSError as error:
        pytest.skip(f"Hardlinks unavailable on this filesystem: {error}")
    with pytest.raises(ValueError, match="must differ from validation report"):
        with scenario_build_log(log_bundle):
            pytest.fail("Colliding log context was entered")
    assert report.read_text(encoding="utf-8") == "Original report\n"


@pytest.mark.parametrize("yaml_name, scenario_name", [
    ("default", "baseline"), ("legacy_comparison", "legacy_comparison"),
    ("notebook_diagnostics", "notebook_diagnostics"),
])
def test_every_bundled_scenario_inherits_run_and_yaml_names(log_bundle, yaml_name, scenario_name):
    actual = utils.load_config_bundle(f"config/scenarios/{yaml_name}.yaml")
    selected = replace(log_bundle, scenario=actual.scenario,
                       scenario_path=log_bundle.scenario_path.with_name(f"{yaml_name}.yaml"))
    selected.scenario_path.write_bytes(actual.scenario_path.read_bytes())
    with scenario_build_log(selected) as path:
        logging.getLogger("test.build").info("Scenario-specific event")
    assert re.fullmatch(rf"{scenario_name}_{yaml_name}_\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}-\d{{2}}-\d{{6}}Z\.log", path.name)
    content = path.read_text(encoding="utf-8")
    assert f"yaml={selected.scenario_path.resolve()}" in content
    assert f"yaml_sha256={utils.file_sha256(selected.scenario_path)}" in content


def test_copied_yaml_and_changed_scenario_get_distinct_safe_logs(log_bundle):
    paths = []
    for name, filename in [("baseline", "default.yaml"), ("baseline", "copy.yaml"),
                           ("Ontario: trial", "copy.yaml")]:
        scenario = log_bundle.scenario.model_copy(update={
            "scenario": log_bundle.scenario.scenario.model_copy(update={"name": name}),
        })
        selected = replace(log_bundle, scenario=scenario,
                           scenario_path=log_bundle.scenario_path.with_name(filename))
        selected.scenario_path.write_text(f"# {name}\n", encoding="utf-8")
        with scenario_build_log(selected) as path:
            paths.append(path)
    assert all(p.name.startswith(prefix) for p, prefix in zip(paths, [
        "baseline_default_", "baseline_copy_", "Ontario_trial_copy_",
    ], strict=True))
    assert len(set(paths)) == 3


def test_revised_yaml_content_is_recorded_on_each_attempt(log_bundle):
    hashes = []
    paths = []
    for version in ("first", "revised"):
        log_bundle.scenario_path.write_text(f"# {version}\n", encoding="utf-8")
        hashes.append(utils.file_sha256(log_bundle.scenario_path))
        with scenario_build_log(log_bundle) as path:
            paths.append(path)
    assert hashes[0] != hashes[1]
    assert all(f"yaml_sha256={digest}" in path.read_text(encoding="utf-8")
               for digest, path in zip(hashes, paths, strict=True))


def test_filename_uses_utc_date_time_and_never_appends_to_a_collision(log_bundle, monkeypatch):
    class FixedClock:
        @staticmethod
        def now(tz):
            assert tz is UTC
            return datetime(2026, 10, 22, 17, 30, 0, 123456, tzinfo=UTC)

    monkeypatch.setattr(build_logging, "datetime", FixedClock)
    with scenario_build_log(log_bundle) as path:
        pass
    assert path.name == "baseline_default_2026-10-22_17-30-00-123456Z.log"
    original = path.read_bytes()
    with pytest.raises(FileExistsError):
        with scenario_build_log(log_bundle):
            pytest.fail("A timestamp collision must not overwrite or append")
    assert path.read_bytes() == original


@pytest.mark.parametrize("template", [
    "outputs/logs/baseline.log", "outputs/logs/{scenario_name}.log",
    "outputs/logs/{scenario_name}_{scenario_yaml}.log",
    "outputs/logs/{scenario_name}__{unknown}.log",
    "outputs/logs/{scenario_name!r}__{scenario_yaml}.log",
])
def test_build_log_template_requires_both_identities(log_bundle, template):
    payload = log_bundle.paths.outputs.model_dump()
    payload["build_log"] = template
    with pytest.raises(ValueError, match="template must contain"):
        OutputPaths.model_validate(payload)
