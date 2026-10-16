"""Build observability: elapsed wall time and opt-in configuration detail."""

import logging

import pytest

import utils


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
