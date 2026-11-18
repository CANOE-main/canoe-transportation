from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest
from canoe_schema.v4_0 import CapacityToActivity, CostVariable, DataSet, Efficiency, ExistingCapacity, Region, Technology, TechnologyLabel

from build_transport import TransportContribution, insert_transport_contribution
from validation.insertion import cleanup_transport_parameter_batches, validate_transport_parameter_support
from validation.schema_contract import create_v4_schema


def row(region: str = "ON", tech: str = "T_EX", vintage: int = 2023):
    return SimpleNamespace(region=region, tech=tech, vintage=vintage, capacity=1.0)


def test_support_rejects_historical_orphans_including_entirely_absent_technology() -> None:
    report = validate_transport_parameter_support({
        "existing_capacity": [row("QC")], "efficiency": [row()],
        "cost_variable": [row(tech="T_MISSING_EX")],
    }, existing_vintages=[2023])
    assert report["ok"] is False
    assert report["checks"]["efficiency_historical_capacity"]["unsupported_keys"] == 1
    assert report["checks"]["cost_variable_historical_capacity"]["unsupported_keys"] == 1
    assert report["checks"]["cost_variable_efficiency"]["unsupported_keys"] == 1


def test_future_costs_need_efficiency_but_future_efficiency_needs_no_existing_stock() -> None:
    future = row(tech="T_NEW", vintage=2025)
    batches = {"existing_capacity": [], "efficiency": [future], "cost_invest": [future]}
    assert validate_transport_parameter_support(batches, existing_vintages=[2023])["ok"]
    batches["efficiency"] = []
    report = validate_transport_parameter_support(batches, existing_vintages=[2023])
    assert not report["ok"]
    assert report["checks"]["cost_invest_efficiency"]["unsupported_keys"] == 1


def test_new_factors_require_exact_efficiency_vintage_without_applying_old_period_sql() -> None:
    batches = {"efficiency": [row(tech="T_NEW", vintage=2025)],
               "limit_annual_capacity_factor": [
                   SimpleNamespace(region="ON", tech_or_group="T_NEW", vintage=2030),
                   SimpleNamespace(region="ON", tech_or_group="T_EX", vintage=2025),
                   SimpleNamespace(region="ON", tech_or_group="a_group", vintage=2025),
               ]}
    report = validate_transport_parameter_support(batches, existing_vintages=[2023],
                                                  new_technologies=["T_NEW"])
    assert report["checks"]["new_capacity_factor_efficiency"]["unsupported_keys"] == 1
    assert report["checks"]["new_capacity_factor_efficiency"]["checked_keys"] == 1


def test_cleanup_catches_historical_keys_without_any_capacity_entry() -> None:
    orphan = row(tech="entirely_missing")
    cleaned, removed = cleanup_transport_parameter_batches({
        "existing_capacity": [], "efficiency": [orphan], "cost_variable": [orphan],
    }, existing_periods=[2023], first_model_period=2025, epsilon=0.001)
    assert cleaned["efficiency"] == cleaned["cost_variable"] == []
    assert {item["reason"] for item in removed} == {"missing_active_existing_capacity"}


def test_partial_development_layers_are_reported_and_unchecked_when_omitted() -> None:
    report = validate_transport_parameter_support({"cost_variable": [row()]},
                                                  existing_vintages=[2023])
    assert report["ok"] is True
    assert report["checks"] == {}
    assert report["prepared_tables"] == ["cost_variable"]


def test_charger_conversion_support_rejects_missing_new_and_existing_rows():
    batches = {"efficiency": [row(tech="charger_EX"), row(tech="charger_N", vintage=2025)],
               "capacity_to_activity": []}
    report = validate_transport_parameter_support(
        batches, existing_vintages=[2023],
        capacity_to_activity_technologies=["charger_EX", "charger_N"],
    )
    assert not report["ok"]
    assert report["checks"]["capacity_to_activity_coverage"]["unsupported_keys"] == 2
    batches["capacity_to_activity"] = [
        CapacityToActivity(region="ON", tech=tech, c2a=31.536, data_id="conversion")
        for tech in ("charger_EX", "charger_N")
    ]
    assert validate_transport_parameter_support(
        batches, existing_vintages=[2023],
        capacity_to_activity_technologies=["charger_EX", "charger_N"],
    )["ok"]


def test_charger_conversion_is_rechecked_before_any_caller_owned_write():
    contribution = TransportContribution(
        dataset=DataSet(data_id="fixture", label="fixture"), labels_by_table={}, load_results=[],
        rows_by_table={"technology": [Technology(tech="charger_N", category="charger", flag="p", data_id="fixture")]},
        efficiency_rows=[Efficiency(region="ON", tech="charger_N", vintage=2025,
                                   input_comm="electricity", output_comm="charged", efficiency=0.95,
                                   data_id="fixture")],
        capacity_to_activity_rows=[], capacity_to_activity_categories=("charger",),
        prepared_support_tables=("efficiency", "capacity_to_activity"),
    )
    with sqlite3.connect(":memory:") as connection:
        with pytest.raises(ValueError, match="capacity_to_activity_coverage"):
            insert_transport_contribution(connection, contribution)
        assert connection.execute("SELECT name FROM sqlite_master").fetchall() == []


@pytest.mark.parametrize("value", [None, 0, -1, float("inf")])
def test_conversion_support_rejects_implicit_or_invalid_values(value):
    conversion = CapacityToActivity(region="ON", tech="charger_N", c2a=value, data_id="conversion")
    report = validate_transport_parameter_support(
        {"capacity_to_activity": [conversion]}, existing_vintages=[],
    )
    assert not report["ok"]
    assert report["checks"]["capacity_to_activity_values"]["invalid_rows"] == 1


def test_charger_conversion_registers_internal_provenance_in_caller_transaction():
    technologies = ["T_LDV_CHRG_EX", "T_LDV_CHRG_N", "T_MHDV_CHRG_EX", "T_MHDV_CHRG_N"]
    contribution = TransportContribution(
        dataset=DataSet(data_id="fixture", label="fixture"), scenario_name="conversion_test",
        charger_assumption_dataset=DataSet(data_id="conversion", label="physical conversion"),
        labels_by_table={"technology_label": [TechnologyLabel(tech=tech) for tech in technologies]},
        rows_by_table={"technology": [
            Technology(tech=tech, category="charger", flag="p", data_id="fixture")
            for tech in technologies
        ]}, load_results=[],
        capacity_to_activity_rows=[
            CapacityToActivity(region="ON", tech=tech, c2a=31.536, units="PJ/GW", data_id="conversion")
            for tech in technologies
        ], capacity_to_activity_categories=("charger",),
        prepared_support_tables=("capacity_to_activity",),
    )
    with sqlite3.connect(":memory:", isolation_level=None) as connection:
        create_v4_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(*Region(region="ON").to_insert_sql(parameterized=True))
        insert_transport_contribution(connection, contribution)
        actual = connection.execute(
            "SELECT c.tech, c.c2a, c.units, c.data_source, c.dq_cred FROM capacity_to_activity c "
            "JOIN data_set d USING (data_id) ORDER BY c.tech"
        ).fetchall()
        assert actual == [(tech, 31.536, "PJ/GW", None, None) for tech in sorted(technologies)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        connection.rollback()
        assert connection.execute("SELECT COUNT(*) FROM capacity_to_activity").fetchone() == (0,)


@pytest.mark.parametrize("capacity", [0, -1, float("inf")])
def test_support_rejects_invalid_capacity_without_a_silent_drop(capacity: float) -> None:
    capacity_row = row()
    capacity_row.capacity = capacity
    report = validate_transport_parameter_support({"existing_capacity": [capacity_row]},
                                                  existing_vintages=[2023])
    assert not report["ok"]
    assert report["checks"]["existing_capacity_positive"]["invalid_rows"] == 1


def test_insertion_rechecks_mutable_rows_before_any_database_write() -> None:
    key = {"region": "ON", "tech": "T_EX", "vintage": 2023, "data_id": "fixture"}
    contribution = TransportContribution(
        dataset=DataSet(data_id="fixture", label="fixture"),
        labels_by_table={}, rows_by_table={}, load_results=[],
        parameter_rows=[ExistingCapacity(**key, capacity=1, units="k vehicles")],
        efficiency_rows=[Efficiency(**key, input_comm="fuel", output_comm="service", efficiency=1)],
        cost_variable_rows=[CostVariable(**key, period=2025, cost=1)],
        existing_vintages=(2023,),
        prepared_support_tables=("existing_capacity", "efficiency", "cost_variable"),
    )
    contribution.parameter_rows.clear()
    # No schema is needed: the support error must occur before even registering data.
    with sqlite3.connect(":memory:") as connection:
        with pytest.raises(ValueError, match="historical_capacity"):
            insert_transport_contribution(connection, contribution)
        assert connection.execute("SELECT name FROM sqlite_master").fetchall() == []


def test_insertion_rejects_uninsertable_age_evidence_before_any_database_write() -> None:
    contribution = TransportContribution(
        dataset=DataSet(data_id="fixture", label="fixture"),
        labels_by_table={}, rows_by_table={}, load_results=[],
        road_utilization_audit={"vkt_schedules": True},
    )
    with sqlite3.connect(":memory:") as connection:
        with pytest.raises(ValueError, match="vkt_schedules requires dual-index LACF insertion"):
            insert_transport_contribution(connection, contribution)
        assert connection.execute("SELECT name FROM sqlite_master").fetchall() == []


def _curve_batches():
    return {
        "lifetime_tech": [],
        "lifetime_process": [SimpleNamespace(region="ON", tech="T_EX", vintage=2020, lifetime=3)],
        "lifetime_survival_curve": [
            SimpleNamespace(region="ON", tech="T_EX", vintage=2020, period=2020 + age, fraction=value)
            for age, value in enumerate([1, 0.8, 0.4, 0])
        ],
    }


def test_curve_support_requires_explicit_matching_lifetime_and_dense_nodes():
    batches = _curve_batches()
    assert validate_transport_parameter_support(batches, existing_vintages=[2020])["ok"]
    batches["lifetime_process"] = []
    assert not validate_transport_parameter_support(batches, existing_vintages=[2020])["ok"]
    batches["lifetime_tech"] = [SimpleNamespace(region="ON", tech="T_EX", lifetime=3)]
    assert validate_transport_parameter_support(batches, existing_vintages=[2020])["ok"]
    batches["lifetime_survival_curve"].pop(1)
    assert not validate_transport_parameter_support(batches, existing_vintages=[2020])["ok"]


@pytest.mark.parametrize("fraction", [0, 0.00009, 1.1, float("nan")])
def test_curve_support_rejects_interior_values_temoa_cannot_use(fraction):
    batches = _curve_batches()
    batches["lifetime_survival_curve"][2].fraction = fraction
    report = validate_transport_parameter_support(batches, existing_vintages=[2020])
    assert not report["ok"]
    assert report["checks"]["survival_curve_lifetime_contract"]["invalid_keys"] == 1


def test_curve_support_is_rechecked_before_caller_database_writes():
    from canoe_schema.v4_0 import LifetimeProcess, LifetimeSurvivalCurve

    key = {"region": "ON", "tech": "T_EX", "vintage": 2020, "data_id": "fixture"}
    contribution = TransportContribution(
        dataset=DataSet(data_id="fixture", label="fixture"),
        labels_by_table={}, rows_by_table={}, load_results=[],
        lifetime_process_rows=[LifetimeProcess(**key, lifetime=2)],
        lifetime_survival_curve_rows=[
            LifetimeSurvivalCurve(**key, period=2020, fraction=1),
            LifetimeSurvivalCurve(**key, period=2021, fraction=0.5),
            LifetimeSurvivalCurve(**key, period=2022, fraction=0),
        ], prepared_support_tables=("lifetime_process", "lifetime_survival_curve"),
    )
    contribution.lifetime_process_rows.clear()
    with sqlite3.connect(":memory:") as connection:
        with pytest.raises(ValueError, match="survival_curve_lifetime_contract"):
            insert_transport_contribution(connection, contribution)
        assert connection.execute("SELECT name FROM sqlite_master").fetchall() == []


def test_explicit_lifetime_coverage_catches_removed_process_and_curve_pair():
    active = row(vintage=2020)
    report = validate_transport_parameter_support(
        {"existing_capacity": [active], "lifetime_tech": [], "lifetime_process": [],
         "lifetime_survival_curve": []}, existing_vintages=[2020],
        expected_survival_curve_keys=[("ON", "T_EX", 2020)],
    )
    assert not report["ok"]
    assert report["checks"]["active_process_lifetime_coverage"]["unsupported_keys"] == 1
    assert report["checks"]["prepared_survival_curve_coverage"]["unsupported_keys"] == 1


def test_historical_curves_and_process_lifetimes_require_published_capacity_when_prepared():
    batches = _curve_batches()
    batches["existing_capacity"] = []
    report = validate_transport_parameter_support(batches, existing_vintages=[2020])
    assert not report["ok"]
    for table in ("lifetime_process", "lifetime_survival_curve"):
        assert report["checks"][f"{table}_historical_capacity"]["examples"] == [("ON", "T_EX", 2020)]
    batches["existing_capacity"] = [row(vintage=2020)]
    assert validate_transport_parameter_support(batches, existing_vintages=[2020])["ok"]


def test_lifetime_scope_excludes_unlimited_blending_without_exempting_vehicles():
    batches = {"efficiency": [row(tech="vehicle"), row(tech="unlimited_blend")],
               "lifetime_tech": [SimpleNamespace(region="ON", tech="vehicle", lifetime=16)]}
    assert validate_transport_parameter_support(
        batches, existing_vintages=[2023], lifetime_technologies=["vehicle"],
    )["ok"]
    batches["lifetime_tech"] = []
    report = validate_transport_parameter_support(
        batches, existing_vintages=[2023], lifetime_technologies=["vehicle"],
    )
    assert report["checks"]["active_process_lifetime_coverage"]["unsupported_keys"] == 1


def test_fixed_lifetime_needs_a_regional_technology_in_complete_contribution():
    fixed = SimpleNamespace(region="ON", tech="T_EX", lifetime=16)
    batches = {"lifetime_tech": [fixed], "existing_capacity": [], "efficiency": []}
    report = validate_transport_parameter_support(batches, existing_vintages=[2020])
    assert not report["ok"]
    assert report["checks"]["fixed_lifetime_regional_support"]["examples"] == [("ON", "T_EX")]
    batches["efficiency"] = [row(vintage=2025)]
    assert validate_transport_parameter_support(batches, existing_vintages=[2020])["ok"]


def _charger_group_contribution(singleton_group="charger_group"):
    from canoe_schema.v4_0 import LimitCapacityShare, TechGroup, TechGroupMember
    from build_transport import _charger_contract_fingerprint

    groups = [TechGroup(group_name=name, data_id="charger-structure") for name in (singleton_group, "pool")]
    members = [TechGroupMember(group_name=name, tech="charger", data_id="charger-structure")
               for name in (singleton_group, "pool")]
    shares = [LimitCapacityShare(region="ON", period=2025, sub_group=singleton_group, super_group="pool",
                                 operator="e", share=1, data_id="charger-structure")]
    return TransportContribution(
        dataset=DataSet(data_id="fixture", label="fixture"), scenario_name="charger-groups",
        charger_assumption_dataset=DataSet(data_id="charger-structure", label="single-member group identity"),
        labels_by_table={"technology_label": [TechnologyLabel(tech="charger")]}, load_results=[],
        rows_by_table={"technology": [Technology(tech="charger", category="charger", flag="p", data_id="fixture")]},
        charger_group_rows=groups, charger_group_member_rows=members, charger_capacity_share_rows=shares,
        charger_contract_fingerprint=_charger_contract_fingerprint(groups, members, shares),
    )


@pytest.mark.parametrize("batch", ["charger_group_rows", "charger_group_member_rows", "charger_capacity_share_rows"])
def test_mutated_charger_mix_is_rejected_before_any_caller_write(batch):
    contribution = _charger_group_contribution()
    getattr(contribution, batch).pop()
    with sqlite3.connect(":memory:") as connection:
        with pytest.raises(ValueError, match="charger groups or capacity shares changed"):
            insert_transport_contribution(connection, contribution)
        assert connection.execute("SELECT name FROM sqlite_master").fetchall() == []


def test_charger_groups_and_period_shares_register_in_caller_transaction():
    from canoe_schema.v4_0 import TimePeriod

    with sqlite3.connect(":memory:", isolation_level=None) as connection:
        create_v4_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(*Region(region="ON").to_insert_sql(parameterized=True))
        connection.execute(*TimePeriod(period=2025, flag="f").to_insert_sql(parameterized=True))
        inserted = insert_transport_contribution(connection, _charger_group_contribution())
        assert len(inserted["tech_group"]) == len(inserted["tech_group_member"]) == 2
        assert connection.execute(
            "SELECT region,period,sub_group,super_group,operator,share FROM limit_capacity_share"
        ).fetchall() == [("ON", 2025, "charger_group", "pool", "e", 1)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.in_transaction
        connection.rollback()
        assert connection.execute("SELECT COUNT(*) FROM tech_group").fetchone() == (0,)


def test_charger_group_name_conflicts_are_rejected_at_insertion():
    with sqlite3.connect(":memory:") as connection:
        with pytest.raises(ValueError, match="group names conflict"):
            insert_transport_contribution(connection, _charger_group_contribution(singleton_group="charger"))
        assert connection.execute("SELECT name FROM sqlite_master").fetchall() == []


def test_charger_mix_cannot_drop_its_preparation_fingerprint():
    from dataclasses import replace

    contribution = replace(_charger_group_contribution(), charger_contract_fingerprint=None)
    with sqlite3.connect(":memory:") as connection:
        with pytest.raises(ValueError, match="lacks its contract fingerprint"):
            insert_transport_contribution(connection, contribution)
        assert connection.execute("SELECT name FROM sqlite_master").fetchall() == []
