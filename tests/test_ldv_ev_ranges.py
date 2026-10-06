from dataclasses import replace
import sqlite3

import pandas as pd
import pytest
from canoe_schema.v4_0 import (
    DataSet,
    Region,
    TimePeriod,
    TechnologyLabel,
    TechGroupLabel,
    TechGroup,
    TechGroupMember,
)
from pydantic import ValidationError

from build_transport import (
    prepare_transport_contribution,
    insert_transport_contribution,
)
from fetching.epa_omega_baseline import (
    build_request,
    fetch_baseline,
    normalize_baseline,
    prepare_baseline_extract,
    module_rules as omega_rules,
    required_inputs,
    validate_extract,
)
from parameterization.ldv_ev_ranges import (
    RangeRepresentationBlocked,
    build_range_evidence,
    bucket_range_evidence,
    module_rules,
    prepare_range_rows,
    prepare_representative_parameters,
    range_bucket,
    validate_share_evidence,
)
from utils import load_config_bundle, resolve_input_path
from validation.config_models import ScenarioConfig
from validation.insertion import insert_models
from validation.provenance import resolve_provenance
from validation.schema_contract import create_v4_schema


@pytest.fixture
def bundle():
    b = load_config_bundle("config/scenarios/legacy_reproduction.yaml")
    payload = b.scenario.model_dump()
    payload["geography"]["regions"] = ["ON"]
    payload["charging_profiles"]["travel_behavior_source"] = "none"
    payload["embodied_emissions"] = False
    return replace(b, scenario=ScenarioConfig.model_validate(payload))


def with_mode(bundle, mode):
    payload = bundle.scenario.model_dump()
    payload["BEV_PHEV_range_representation"]["mode"] = mode
    return replace(bundle, scenario=ScenarioConfig.model_validate(payload))


@pytest.fixture
def evidence(bundle):
    rows = []
    for market in ["car", "truck"]:
        for p, values in [
            ("BEV", [(140, 100), (400, 300)]),
            ("PHEV", [(30, 80), (40, 20)]),
        ]:
            for miles, count in values:
                rows.append(
                    dict(
                        source_row_id=len(rows),
                        model_year=2022,
                        reg_class_id=market,
                        manufacturer_id="ABC",
                        vehicle_name=f"model{len(rows)}",
                        context_size_class="Midsize",
                        powertrain_type=p,
                        onroad_charge_depleting_range_mi=miles,
                        battery_kwh=20,
                        sales=count,
                    )
                )
    records = normalize_baseline(pd.DataFrame(rows), rules=omega_rules(bundle))
    result = bucket_range_evidence(records, rules=module_rules(bundle))
    result.provenance_contexts.append(
        resolve_provenance(
            bundle.sources,
            source_key="epa_omega_baseline",
            component_key="baseline_sales_ranges",
            transformation="Fixture baseline sales",
            transformation_version="1",
        )
    )
    return result


@pytest.mark.parametrize(
    "p,miles,expected",
    [
        ("BEV", 150, "bev_150_miles"),
        ("BEV", 150.01, "bev_200_miles"),
        ("BEV", 250, "bev_200_miles"),
        ("BEV", 250.01, "bev_300_miles"),
        ("BEV", 350, "bev_300_miles"),
        ("BEV", 350.01, "bev_400_miles"),
        ("PHEV", 34.999, "phev_35_miles"),
        ("PHEV", 35, "phev_50_miles"),
    ],
)
def test_legacy_buckets_are_distinct_from_rating_bins(bundle, p, miles, expected):
    assert range_bucket(p, miles, module_rules(bundle)) == expected


def test_duplicate_row_identity_and_invalid_sales_are_rejected(bundle, evidence):
    raw = evidence.records.copy()
    with pytest.raises(ValueError, match="identity"):
        normalize_baseline(pd.concat([raw, raw.iloc[[0]]]), rules=omega_rules(bundle))
    for value in [-1, 0.5, float("nan")]:
        invalid = raw.copy()
        invalid["sales"] = invalid.sales.astype(float)
        invalid.loc[0, "sales"] = value
        with pytest.raises(ValueError, match="sales"):
            normalize_baseline(invalid, rules=omega_rules(bundle))


def test_unresolved_sales_remain_in_full_denominator_and_block_share_mode(
    bundle, evidence
):
    records = evidence.records.copy()
    records.loc[0, "range_status"] = "invalid_cd_range"
    result = bucket_range_evidence(records, rules=module_rules(bundle))
    result.provenance_contexts.extend(evidence.provenance_contexts)
    assert result.audit["sales"] == evidence.audit["sales"]
    assert result.audit["unresolved_sales"] == 100
    buckets = result.bucket_totals.query("market_class=='car' and powertrain=='BEV'")
    assert buckets.share_of_all_sales.sum() == pytest.approx(0.75)
    with pytest.raises(RangeRepresentationBlocked, match="100 baseline sales"):
        validate_share_evidence(result, rules=module_rules(bundle))


def test_none_never_acquires_range_sources_and_preserves_templates(bundle, monkeypatch):
    def fail(*a, **kw):
        raise AssertionError("range acquisition in none")

    monkeypatch.setattr("parameterization.ldv_ev_ranges.build_range_evidence", fail)
    result = prepare_range_rows(bundle, publish=False)
    assert not result.share_rows and not result.group_rows and not result.member_rows
    assert result.structural_dataset is None


def test_constraints_use_new_class_powertrain_denominators_ge_and_model_vintages(
    bundle, evidence
):
    b = with_mode(bundle, "new_capacity_shares")
    result = prepare_range_rows(b, evidence=evidence, publish=False)
    assert len(result.share_rows) == 18 * len(b.scenario.periods.model)
    assert {r.operator.value for r in result.share_rows} == {"ge"}
    assert {r.vintage for r in result.share_rows} == set(b.scenario.periods.model)
    tech = pd.read_csv(resolve_input_path(b, "template", "technology.csv")).set_index(
        "tech"
    )
    for group in {r.super_group for r in result.share_rows}:
        members = [r.tech for r in result.member_rows if r.group_name == group]
        assert all(t.endswith("_N") for t in members)
        assert len(set(tech.loc[members].category)) == 1
        assert set(tech.loc[members].category) <= {
            "cars",
            "passenger_light_trucks",
            "freight_light_trucks",
        }
        assert len({tech.loc[t].sub_category.split("_")[0] for t in members}) == 1
        first_vintage = [
            r.share
            for r in result.share_rows
            if r.super_group == group and r.vintage == b.scenario.periods.model[0]
        ]
        assert sum(first_vintage) == pytest.approx(1)


def test_actual_baseline_resolves_all_sales_and_supports_minimum_constraints(
    bundle, monkeypatch
):
    monkeypatch.setattr(
        "socket.socket", lambda *a, **kw: pytest.fail("network in OMEGA evidence")
    )
    result = build_range_evidence(bundle)
    assert result.audit["records"] == 78
    assert result.audit["sales"] == 855557
    assert result.audit["unresolved_sales"] == 0
    assert result.audit["accepted_for_parameters"]
    totals = (
        result.records.groupby(["market_class", "powertrain"]).sales.sum().to_dict()
    )
    assert totals == {
        ("car", "BEV"): 458662,
        ("car", "PHEV"): 49292,
        ("light_truck", "BEV"): 211289,
        ("light_truck", "PHEV"): 136314,
    }
    assert result.bucket_totals.groupby(
        ["market_class", "powertrain"]
    ).share_of_all_sales.sum().tolist() == pytest.approx([1] * 4)
    zeros = result.bucket_totals.query(
        "market_class=='light_truck' and powertrain=='BEV' and sales==0"
    )
    assert set(zeros.bucket) == {"bev_150_miles", "bev_400_miles"}
    prepared = prepare_range_rows(
        with_mode(bundle, "new_capacity_shares"), evidence=result, publish=False
    )
    assert len(prepared.share_rows) == 90
    assert {r.data_source for r in prepared.share_rows} == {"T33"}
    assert all(
        r.dq_cred is not None
        and r.dq_geog is not None
        and r.dq_struc is not None
        and r.dq_tech is not None
        and r.dq_time is not None
        for r in prepared.share_rows
    )


def test_representative_mode_requires_supplied_parameter_prerequisites(
    bundle, evidence
):
    ranges = prepare_range_rows(
        with_mode(bundle, "representative_archetype"), evidence=evidence, publish=False
    )
    assert not ranges.share_rows and not ranges.group_rows and not ranges.member_rows
    from validation.schema_contract import TransportationTechnology

    technologies = pd.read_csv(resolve_input_path(bundle, "template", "technology.csv"))
    categories = module_rules(bundle).category_to_market
    # Structure alone is insufficient; the pure API never rebuilds missing parameter layers.
    rows = [
        TransportationTechnology(
            tech=r.tech,
            category=r.category,
            sub_category=r.sub_category,
            flag="p",
            data_id="internal",
        )
        for r in technologies.itertuples()
        if r.category in categories
    ]
    with pytest.raises(
        RangeRepresentationBlocked, match="supplied capacity_to_activity"
    ):
        prepare_representative_parameters(
            with_mode(bundle, "representative_archetype"),
            range_preparation=ranges,
            batches={"technology": rows, "commodity": []},
            provenance_contexts=[],
            publish=False,
        )


def contribution(connection, bundle, evidence=None):
    return prepare_transport_contribution(
        connection,
        bundle=bundle,
        template_dir=resolve_input_path(bundle, "template"),
        include_existing_capacity=False,
        include_demand=False,
        include_road_utilization=False,
        include_lifetimes=False,
        include_efficiencies=False,
        include_costs=False,
        include_emission_embodied=False,
        range_evidence=evidence,
    )


def test_caller_rollback_reinsertion_mode_switch_and_unrelated_groups(bundle, evidence):
    connection = sqlite3.connect(":memory:", isolation_level=None)
    create_v4_schema(connection)
    connection.execute("BEGIN")
    insert_models(connection, [Region(region="ON")])
    insert_models(
        connection,
        [TimePeriod(period=v, flag="f") for v in bundle.scenario.periods.model],
    )
    insert_models(connection, [DataSet(data_id="other", label="other sector")])
    insert_models(
        connection,
        [
            TechnologyLabel(tech="OTHER"),
        ],
    )
    insert_models(connection, [TechGroupLabel(group_name="other_group")])
    insert_models(
        connection,
        [TechGroup(group_name="other_group", data_id="other", notes="caller-owned")],
    )
    insert_models(
        connection,
        [TechGroupMember(group_name="other_group", tech="OTHER", data_id="other")],
    )
    c = contribution(connection, with_mode(bundle, "new_capacity_shares"), evidence)
    inserted = insert_transport_contribution(connection, c)
    assert len(inserted["limit_new_capacity_share"]) == 90
    assert connection.in_transaction
    assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    insert_transport_contribution(connection, c, conflict="ignore_identical")
    assert (
        connection.execute("SELECT COUNT(*) FROM limit_new_capacity_share").fetchone()[
            0
        ]
        == 90
    )
    none = contribution(connection, bundle)
    insert_transport_contribution(connection, none, conflict="ignore_identical")
    assert (
        connection.execute("SELECT COUNT(*) FROM limit_new_capacity_share").fetchone()[
            0
        ]
        == 0
    )
    assert connection.execute("SELECT COUNT(*) FROM tech_group").fetchone()[0] == 1
    assert connection.execute(
        "SELECT notes FROM tech_group WHERE group_name='other_group'"
    ).fetchone() == ("caller-owned",)
    assert connection.execute(
        "SELECT tech FROM tech_group_member WHERE group_name='other_group'"
    ).fetchone() == ("OTHER",)
    assert (
        connection.execute("SELECT COUNT(*) FROM existing_capacity").fetchone()[0] == 0
    )
    assert len(c.rows_by_table["technology"]) == len(none.rows_by_table["technology"])
    connection.rollback()
    assert connection.execute("SELECT COUNT(*) FROM technology").fetchone()[0] == 0
    connection.close()


def test_typed_modes_reject_unknown_selection(bundle):
    payload = bundle.scenario.model_dump()
    payload["BEV_PHEV_range_representation"]["mode"] = "average_everything"
    with pytest.raises(ValidationError):
        ScenarioConfig.model_validate(payload)


def test_compact_registered_extract_matches_legacy_notebook_and_market(bundle):
    identity = prepare_baseline_extract(bundle)
    assert (
        identity["matches_legacy_notebook_source"]
        and identity["matches_compact_extract_bytes"]
    )
    assert identity["parent_rows"] == 783 and identity["extract_rows"] == 78
    assert identity["sales_by_powertrain"] == {"BEV": 669951, "PHEV": 185606}
    assert build_request(bundle).expected_bytes == 13977
    assert bundle.sources.sources["epa_omega_baseline"].status == "active"
    assert bundle.sources.sources["epa_automotive_trends"].status == "inactive"
    assert required_inputs(bundle) == []
    assert required_inputs(with_mode(bundle, "new_capacity_shares")) == [
        build_request(bundle).extract_path
    ]


def test_normal_etl_requires_only_compact_extract_and_never_parent_or_fuel_catalogue(
    bundle, tmp_path
):
    from validation.config_models import SourcesConfig

    sources = bundle.sources.model_dump()
    adapter = sources["sources"]["epa_omega_baseline"]["components"][
        "baseline_sales_ranges"
    ]["adapter"]
    adapter["parent_path"] = str(tmp_path / "no_parent.csv")
    adapter["notebook_path"] = str(tmp_path / "no_notebook.ipynb")
    b = replace(bundle, sources=SourcesConfig.model_validate(sources))
    assert len(fetch_baseline(b, publish=False)) == 78


def test_compact_pin_rejects_changed_snapshot_before_normalization(bundle, tmp_path):
    request = build_request(bundle)
    bad = tmp_path / "modified.csv"
    bad.write_bytes(request.extract_path.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="immutable snapshot"):
        validate_extract(request.model_copy(update={"extract_path": bad}))


def test_source_year_and_normalization_contract_are_checked_before_io(bundle, tmp_path):
    from validation.config_models import SourcesConfig

    sources = bundle.sources.model_dump()
    adapter = sources["sources"]["epa_omega_baseline"]["components"][
        "baseline_sales_ranges"
    ]["adapter"]
    adapter["model_year"] = 2024
    adapter["extract_path"] = str(tmp_path / "missing.csv")
    b = replace(bundle, sources=SourcesConfig.model_validate(sources))
    with pytest.raises(ValueError, match="model years disagree"):
        fetch_baseline(b, publish=False)
    from fetching.epa_omega_baseline import OmegaNormalizationRules

    rules = omega_rules(bundle).model_dump()
    rules["columns"] = ["sales"]
    with pytest.raises(ValidationError, match="native plug-in"):
        OmegaNormalizationRules.model_validate(rules)


def test_range_modes_reject_supplied_evidence_from_a_different_source(bundle, evidence):
    wrong = resolve_provenance(
        bundle.sources,
        source_key="fueleconomy_gov_vehicle_data",
        component_key="range_evidence",
        transformation="Unaccepted evidence",
        transformation_version="1",
    )
    other = replace(evidence, provenance_contexts=[wrong])
    with pytest.raises(RangeRepresentationBlocked, match="registered OMEGA"):
        prepare_range_rows(
            with_mode(bundle, "new_capacity_shares"), evidence=other, publish=False
        )


def test_scenario_has_only_range_mode_and_rejects_obsolete_gates(bundle):
    assert set(bundle.scenario.BEV_PHEV_range_representation.model_dump()) == {"mode"}
    payload = bundle.scenario.model_dump()
    payload["BEV_PHEV_range_representation"]["evidence"] = "epa_fueleconomy_2024"
    with pytest.raises(ValidationError, match="Extra inputs"):
        ScenarioConfig.model_validate(payload)


@pytest.fixture
def representative_prerequisites(bundle, evidence):
    from canoe_schema import v4_0 as schema
    from validation.schema_contract import TransportationTechnology

    b = with_mode(bundle, "representative_archetype")
    context = evidence.provenance_contexts[0]
    fields = context.parameter_fields()
    internal = DataSet(data_id="fixture_structure", label="Internal fixture structure")
    technologies = pd.read_csv(
        resolve_input_path(b, "template", "technology.csv")
    ).astype(object)
    commodities = pd.read_csv(
        resolve_input_path(b, "template", "commodity.csv")
    ).astype(object)
    rows = {
        "technology": [
            TransportationTechnology(
                **{k: v for k, v in r.items() if v is not None},
                data_id=internal.data_id,
            )
            for r in technologies.where(technologies.notna(), None).to_dict("records")
        ],
        "commodity": [
            schema.Commodity(
                **{k: v for k, v in r.items() if v is not None},
                data_id=internal.data_id,
            )
            for r in commodities.where(commodities.notna(), None).to_dict("records")
        ],
        **{
            t: []
            for t in (
                "existing_capacity",
                "capacity_to_activity",
                "limit_annual_capacity_factor",
                "lifetime_tech",
                "lifetime_survival_curve",
                "efficiency",
                "limit_tech_input_split",
                "cost_invest",
                "cost_fixed",
                "cost_variable",
                "emission_embodied",
            )
        },
    }
    rules = module_rules(b)
    outputs = {
        "cars": "T_D_pkm_ldv_c",
        "passenger_light_trucks": "T_D_pkm_ldv_t",
        "freight_light_trucks": "T_D_tkm_ldv_t",
    }
    for powertrain, buckets in rules.buckets.items():
        for category in rules.category_to_market:
            for i, bucket in enumerate(buckets):
                tech = next(
                    r.tech
                    for r in rows["technology"]
                    if r.category == category
                    and r.sub_category == bucket.sub_category
                    and r.tech.endswith("_N")
                )
                rows["capacity_to_activity"].append(
                    schema.CapacityToActivity(
                        region="ON",
                        tech=tech,
                        c2a=1,
                        units="bn service/k vehicles",
                        data_id=internal.data_id,
                    )
                )
                for vintage in b.scenario.periods.model:
                    rows["limit_annual_capacity_factor"].append(
                        schema.LimitAnnualCapacityFactor(
                            region="ON",
                            tech_or_group=tech,
                            vintage=vintage,
                            output_comm=outputs[category],
                            operator="e",
                            factor=0.1,
                            **fields,
                        )
                    )
                    input_comm = (
                        ("T_gsl_elc_phev35" if i == 0 else "T_gsl_elc_phev50")
                        if powertrain == "PHEV"
                        else "T_elc_ldv_chrg"
                    )
                    # Per-vintage changes make arithmetic efficiency and a single exact fuel split invalid.
                    efficiency = 2 + i + (vintage - 2025) / 100
                    rows["efficiency"].append(
                        schema.Efficiency(
                            region="ON",
                            tech=tech,
                            vintage=vintage,
                            input_comm=input_comm,
                            output_comm=outputs[category],
                            efficiency=efficiency,
                            units="bn service/PJ",
                            **fields,
                        )
                    )
                    rows["cost_invest"].append(
                        schema.CostInvest(
                            region="ON",
                            tech=tech,
                            vintage=vintage,
                            cost=10 + i * 2,
                            units="$M2020CAD/k vehicles",
                            **fields,
                        )
                    )
                    rows["emission_embodied"].append(
                        schema.EmissionEmbodied(
                            region="ON",
                            tech=tech,
                            vintage=vintage,
                            emis_comm="co2",
                            value=100 + i * 10,
                            units="kt/k vehicles",
                            **fields,
                        )
                    )
                    for period in b.scenario.periods.model:
                        if period >= vintage:
                            rows["lifetime_survival_curve"].append(
                                schema.LifetimeSurvivalCurve(
                                    region="ON",
                                    period=period,
                                    tech=tech,
                                    vintage=vintage,
                                    fraction=1 if period == vintage else 0.8,
                                    **fields,
                                )
                            )
                            rows["cost_variable"].append(
                                schema.CostVariable(
                                    region="ON",
                                    period=period,
                                    tech=tech,
                                    vintage=vintage,
                                    cost=20 + i * 3,
                                    units="$M2020CAD/bn service",
                                    **fields,
                                )
                            )
    for n, fraction in ((35, 0.3), (50, 0.6)):
        for vintage in b.scenario.periods.model:
            for input_comm in ("T_gsl", "T_elc_ldv_chrg"):
                rows["efficiency"].append(
                    schema.Efficiency(
                        region="ON",
                        vintage=vintage,
                        tech=f"T_BLND_GSL_ELC_PHEV{n}",
                        input_comm=input_comm,
                        output_comm=f"T_gsl_elc_phev{n}",
                        efficiency=1,
                        units="PJ/PJ",
                        data_id=internal.data_id,
                    )
                )
                rows["limit_tech_input_split"].append(
                    schema.LimitTechInputSplit(
                        region="ON",
                        period=vintage,
                        tech=f"T_BLND_GSL_ELC_PHEV{n}",
                        input_comm=input_comm,
                        operator="e",
                        proportion=fraction
                        if input_comm == "T_elc_ldv_chrg"
                        else 1 - fraction,
                        **fields,
                    )
                )
    stock_tech = next(
        r.tech
        for r in rows["technology"]
        if r.category == "cars" and r.tech.endswith("_EX")
    )
    rows["existing_capacity"].append(
        schema.ExistingCapacity(
            region="ON",
            tech=stock_tech,
            vintage=2020,
            capacity=7,
            units="k vehicles",
            **fields,
        )
    )
    rows["efficiency"].append(
        schema.Efficiency(
            region="ON",
            tech="T_MDV_T_BEV_N",
            vintage=2025,
            input_comm="T_elc_mhdv_chrg",
            output_comm="T_D_tkm_mdv_t",
            efficiency=3,
            units="bn service/PJ",
            **fields,
        )
    )
    ranges = prepare_range_rows(b, evidence=evidence, publish=False)
    return b, dict(
        range_preparation=ranges,
        batches=rows,
        provenance_contexts=[context],
        internal_datasets=(internal,),
        publish=False,
    )


def test_representative_rules_conserve_parameters_and_preserve_stock_and_mhdv(
    representative_prerequisites,
):
    b, kwargs = representative_prerequisites
    result = prepare_representative_parameters(b, **kwargs)
    expected_efficiency = 1 / (0.8 / 2 + 0.2 / 3)
    row = next(
        r
        for r in result.batches["efficiency"]
        if r.tech == "T_LDV_C_GSL_PHEV_REP_N" and r.vintage == 2025
    )
    assert row.efficiency == pytest.approx(expected_efficiency)
    assert row.efficiency != pytest.approx(0.8 * 2 + 0.2 * 3)
    cost = next(
        r.cost
        for r in result.batches["cost_invest"]
        if r.tech == row.tech and r.vintage == 2025
    )
    assert cost == pytest.approx(0.8 * 10 + 0.2 * 12)
    expected_fractions = [
        (0.8 * 0.3 / (2 + d) + 0.2 * 0.6 / (3 + d)) / (0.8 / (2 + d) + 0.2 / (3 + d))
        for d in (0, 0.05, 0.1, 0.15, 0.2)
    ]
    splits = [
        r
        for r in result.batches["limit_tech_input_split"]
        if r.tech == "T_BLND_GSL_ELC_PHEV_C_REP" and r.input_comm == "T_elc_ldv_chrg"
    ]
    assert len(splits) == 5 and all(
        r.proportion == pytest.approx(sum(expected_fractions) / 5) for r in splits
    )
    assert result.audit["max_conservation_error"] < 1e-12
    assert result.audit["max_phev_component_relative_difference"] > 0
    assert result.batches["existing_capacity"] == kwargs["batches"]["existing_capacity"]
    assert [r for r in result.batches["efficiency"] if r.tech == "T_MDV_T_BEV_N"] == [
        r for r in kwargs["batches"]["efficiency"] if r.tech == "T_MDV_T_BEV_N"
    ]
    from parameterization.ldv_ev_ranges import validate_representative_preparation

    result.batches["cost_invest"].pop()
    with pytest.raises(ValueError, match="changed after conservation"):
        validate_representative_preparation(result)


@pytest.mark.parametrize(
    "table,field,value,message",
    [
        ("capacity_to_activity", "c2a", 2, "requires identical"),
        ("limit_annual_capacity_factor", "factor", 0.2, "requires identical"),
        ("lifetime_survival_curve", "fraction", 0.7, "change the fuel/service mix"),
        ("cost_invest", "units", "$M2015CAD/k vehicles", "units/relationships differ"),
        ("efficiency", "dq_cred", 1, "source/DQ fields disagree"),
    ],
)
def test_representative_rejects_unsupported_aggregation_before_return(
    representative_prerequisites, table, field, value, message
):
    b, kwargs = representative_prerequisites
    kwargs["batches"][table][0] = kwargs["batches"][table][0].model_copy(
        update={field: value}
    )
    with pytest.raises(ValueError, match=message):
        prepare_representative_parameters(b, **kwargs)


@pytest.mark.parametrize("caller_object", ["parameter", "group", "constraint"])
def test_representation_rejects_caller_owned_variant_uses_before_writes(bundle, evidence, caller_object):
    from contextlib import closing
    from canoe_schema.v4_0 import CapacityToActivity, LimitNewCapacityShare
    from validation.insertion import replace_owned_ldv_representation
    ranges = prepare_range_rows(with_mode(bundle, "representative_archetype"), evidence=evidence, publish=False)
    variant = ranges.audit["scope_variant_technologies"][0]
    with closing(sqlite3.connect(":memory:", isolation_level=None)) as c:
        create_v4_schema(c)
        insert_models(c, [Region(region="ON"), Region(region="AB")])
        insert_models(c, [TimePeriod(period=2025, flag="f")])
        insert_models(c, [TechnologyLabel(tech=variant)])
        insert_models(c, [DataSet(data_id="other", label="Caller source")])
        # Another region's use of the same shared label is always preserved.
        insert_models(c, [CapacityToActivity(region="AB", tech=variant, c2a=3, data_id="other")])
        if caller_object == "parameter":
            insert_models(c, [CapacityToActivity(region="ON", tech=variant, c2a=2, data_id="other")])
        else:
            group = ranges.audit["scope_super_groups"][0]
            group_data_id = "other"
            if caller_object == "constraint":
                group_data_id = "canoe-transport-range-groups:fixture"
                insert_models(c, [DataSet(data_id=group_data_id, label="Internal transport range group structure")])
            insert_models(c, [TechGroupLabel(group_name=group)])
            insert_models(c, [TechGroup(group_name=group, data_id=group_data_id)])
            insert_models(c, [TechGroupMember(group_name=group, tech=variant, data_id=group_data_id)])
            if caller_object == "constraint":
                insert_models(c, [LimitNewCapacityShare(region="ON", vintage=2025, sub_group=group, super_group=group, operator="ge", share=.1, data_id="other")])
        changes = c.total_changes
        with pytest.raises(ValueError, match="caller-owned|Caller-owned"):
            replace_owned_ldv_representation(c, ranges=ranges, batches={}, contexts=[], internal_datasets=[])
        assert c.total_changes == changes
        assert c.execute("SELECT c2a FROM capacity_to_activity WHERE region='AB'").fetchone() == (3,)


def test_real_offline_standalone_caller_rollback_and_mode_switches(
    bundle, tmp_path, monkeypatch
):
    """Actual pinned sources, complete parameter families, and the shared assembly seam."""
    import hashlib
    import json
    import socket
    from contextlib import closing
    import build_transport
    from canoe_schema.v4_0 import (
        CapacityFactorTech,
        LimitNewCapacityShare,
        TimeSeason,
        TimeOfDay,
    )
    from parameterization.ldv_charging_profiles import prepare_charging_profile_rows

    bundle = load_config_bundle("config/scenarios/legacy_reproduction.yaml")
    payload = bundle.scenario.model_dump()
    payload["charging_profiles"]["travel_behavior_source"] = "nhts"
    # Use the complete configured vehicle-cycle layer as well as actual range evidence.
    payload["embodied_emissions"] = True
    b = replace(bundle, scenario=ScenarioConfig.model_validate(payload))
    b = with_mode(b, "new_capacity_shares")

    def no_network(*args, **kwargs):
        pytest.fail("network access during registered-source offline build")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)
    captured = []
    original_prepare = build_transport.prepare_transport_contribution

    def capture(*args, **kwargs):
        result = original_prepare(*args, **kwargs)
        captured.append(result)
        return result

    monkeypatch.setattr(build_transport, "prepare_transport_contribution", capture)
    database = tmp_path / "omega_new_capacity_shares.sqlite"
    report = build_transport.bootstrap_database(
        bundle=b,
        template_dir=resolve_input_path(b, "template"),
        database_path=database,
    )
    assert report["ok"] and report["validation"]["foreign_key_violations"] == 0
    prepared = captured.pop()
    assert len(prepared.range_representation.share_rows) == 900
    assert len(prepared.charging_profiles.rows) == 175200
    tables = [
        "technology",
        "commodity",
        "existing_capacity",
        "demand",
        "capacity_to_activity",
        "limit_annual_capacity_factor",
        "lifetime_tech",
        "lifetime_survival_curve",
        "efficiency",
        "limit_tech_input_split",
        "cost_invest",
        "cost_fixed",
        "cost_variable",
        "emission_embodied",
    ]

    def digest(c, table):
        rows = c.execute(f'SELECT * FROM "{table}"').fetchall()
        return hashlib.sha256(
            json.dumps(sorted(rows, key=repr), separators=(",", ":")).encode()
        ).hexdigest()

    with closing(sqlite3.connect(database)) as standalone:
        assert standalone.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert standalone.execute(
            "SELECT DISTINCT data_source FROM limit_new_capacity_share"
        ).fetchall() == [("T33",)]
        assert standalone.execute("SELECT COUNT(*) FROM tech_group").fetchone() == (24,)
        assert standalone.execute(
            "SELECT COUNT(*) FROM tech_group_member"
        ).fetchone() == (36,)
        hashes = {t: digest(standalone, t) for t in tables}
        regions = [
            Region(region=r) for (r,) in standalone.execute("SELECT region FROM region")
        ]
        periods = [
            TimePeriod(period=p, flag=flag, sequence=seq)
            for seq, p, flag in standalone.execute(
                "SELECT sequence,period,flag FROM time_period"
            )
        ]

    # Exercise upstream temporal inheritance without repeating 175,200-row inserts
    # for every representation switch. Standalone above proves the full hourly grid;
    # this physical-hour projection retains its annual energy on caller-owned axes.
    hourly_profiles = prepared.charging_profiles
    annual_seasons = [TimeSeason(sequence=1, season="year", segment_fraction=1)]
    annual_times = [TimeOfDay(sequence=1, tod="all", hours=24)]
    annual = prepare_charging_profile_rows(
        b,
        technologies=pd.DataFrame(
            [r.model_dump() for r in prepared.rows_by_table["technology"]]
        ),
        seasons=annual_seasons,
        times_of_day=annual_times,
        time_mapping=pd.DataFrame(
            {"hour_index": range(8760), "season": "year", "tod": "all"}
        ),
        publish=False,
    )
    profile_ids = {x.data_id for x in prepared.charging_profiles.provenance_contexts}
    prepared = replace(
        prepared,
        charging_profiles=annual,
        provenance_contexts=[
            x for x in prepared.provenance_contexts if x.data_id not in profile_ids
        ]
        + annual.provenance_contexts,
    )
    assert len(annual.rows) == 20
    with closing(sqlite3.connect(":memory:", isolation_level=None)) as caller:
        create_v4_schema(caller)
        insert_models(caller, regions)
        insert_models(caller, periods)
        seasons = [
            s.model_copy(update={"notes": "caller-owned axis"})
            for s in annual_seasons
        ]
        insert_models(caller, seasons)
        insert_models(caller, annual_times)
        insert_models(caller, [DataSet(data_id="other", label="caller sector")])
        insert_models(caller, [TechnologyLabel(tech="OTHER")])
        insert_models(caller, [TechGroupLabel(group_name="other_group")])
        insert_models(caller, [TechGroup(group_name="other_group", data_id="other")])
        insert_models(
            caller,
            [TechGroupMember(group_name="other_group", tech="OTHER", data_id="other")],
        )
        insert_models(
            caller,
            [
                LimitNewCapacityShare(
                    region="ON",
                    vintage=2025,
                    sub_group="other_group",
                    super_group="other_group",
                    operator="ge",
                    share=0.2,
                    data_id="other",
                )
            ],
        )
        insert_models(
            caller,
            [
                CapacityFactorTech(
                    region="ON",
                    tech="OTHER",
                    season="year",
                    tod="all",
                    factor=0.2,
                    data_id="other",
                )
            ],
        )
        axes = caller.execute("SELECT * FROM time_season").fetchall()
        caller.execute("BEGIN")
        build_transport.insert_transport_contribution(caller, prepared)
        assert caller.in_transaction
        assert caller.execute("PRAGMA foreign_key_check").fetchall() == []
        assert {t: digest(caller, t) for t in tables} == hashes
        build_transport.insert_transport_contribution(
            caller, prepared, conflict="ignore_identical"
        )
        assert caller.execute(
            "SELECT COUNT(*) FROM limit_new_capacity_share"
        ).fetchone() == (901,)
        assert caller.execute(
            "SELECT COUNT(*) FROM capacity_factor_tech"
        ).fetchone() == (21,)

        none_bundle = with_mode(b, "none")
        none_range = prepare_range_rows(none_bundle, publish=False)
        range_ids = {
            x.data_id for x in prepared.range_representation.provenance_contexts
        }
        none = replace(
            prepared,
            range_representation=none_range,
            provenance_contexts=[
                x for x in prepared.provenance_contexts if x.data_id not in range_ids
            ],
        )
        build_transport.insert_transport_contribution(
            caller, none, conflict="ignore_identical"
        )
        assert caller.execute(
            "SELECT COUNT(*) FROM limit_new_capacity_share"
        ).fetchone() == (1,)
        assert caller.execute("SELECT COUNT(*) FROM tech_group").fetchone() == (1,)
        assert {t: digest(caller, t) for t in tables} == hashes
        disabled = replace(
            none_bundle,
            scenario=none_bundle.scenario.model_copy(
                update={
                    "charging_profiles": none_bundle.scenario.charging_profiles.model_copy(
                        update={"travel_behavior_source": "none"}
                    )
                }
            ),
        )
        none = replace(
            none,
            charging_profiles=prepare_charging_profile_rows(disabled, publish=False),
        )
        build_transport.insert_transport_contribution(
            caller, none, conflict="ignore_identical"
        )
        assert caller.execute(
            "SELECT COUNT(*) FROM capacity_factor_tech"
        ).fetchone() == (1,)
        assert {t: digest(caller, t) for t in tables} == hashes
        build_transport.insert_transport_contribution(
            caller, prepared, conflict="ignore_identical"
        )
        assert caller.execute(
            "SELECT COUNT(*) FROM limit_new_capacity_share"
        ).fetchone() == (901,)
        assert caller.execute("SELECT * FROM time_season").fetchall() == axes
        # Reuse the complete prepared parameter prerequisites; no second acquisition/build.
        rep_bundle = with_mode(b, "representative_archetype")
        rep_seed = replace(
            prepared, range_representation=prepare_range_rows(rep_bundle, publish=False)
        )
        rep = build_transport._apply_ldv_range_representation(rep_bundle, rep_seed)
        audit = rep.range_representation.representative.audit
        assert audit["run_validated"] and audit["max_conservation_error"] < 1e-12
        assert len(audit["phev_component_energy"]) == 150
        assert rep.parameter_rows == prepared.parameter_rows
        assert rep.charging_profiles == prepared.charging_profiles
        variants = set(audit["scope_variants"])
        assert len(variants) == 18
        assert not any(r.tech in variants for r in rep.rows_by_table["technology"])
        for table in tables:
            if table in {"technology", "commodity", "existing_capacity", "demand"}:
                continue
            original = build_transport._representative_batches(prepared)[table]
            transformed = build_transport._representative_batches(rep)[table]
            column = (
                "tech_or_group" if table == "limit_annual_capacity_factor" else "tech"
            )
            assert [r for r in original if getattr(r, column) not in variants] == [
                r
                for r in transformed
                if getattr(r, column) not in variants
                and "_REP" not in getattr(r, column)
            ]
        build_transport.insert_transport_contribution(
            caller, rep, conflict="ignore_identical"
        )
        assert (
            caller.in_transaction
            and caller.execute("PRAGMA foreign_key_check").fetchall() == []
        )
        assert caller.execute(
            "SELECT COUNT(*) FROM limit_new_capacity_share"
        ).fetchone() == (1,)
        assert caller.execute(
            "SELECT COUNT(DISTINCT tech) FROM cost_invest WHERE tech LIKE '%_REP_N'"
        ).fetchone() == (6,)
        assert caller.execute(
            "SELECT COUNT(*) FROM cost_invest WHERE tech IN ("
            + ",".join("?" for _ in variants)
            + ")",
            tuple(variants),
        ).fetchone() == (0,)
        rep_hashes = {t: digest(caller, t) for t in tables}
        # Standalone uses the very same prepared, real-source contribution and schema insertion.
        rep_database = tmp_path / "omega_representative.sqlite"
        full_rep = replace(rep, charging_profiles=hourly_profiles,
            provenance_contexts=[*rep.provenance_contexts, *hourly_profiles.provenance_contexts])
        monkeypatch.setattr(build_transport, "prepare_transport_contribution", lambda *a, **kw: full_rep)
        rep_report = build_transport.bootstrap_database(
            bundle=rep_bundle,
            template_dir=resolve_input_path(rep_bundle, "template"),
            database_path=rep_database,
        )
        assert (
            rep_report["ok"] and rep_report["validation"]["foreign_key_violations"] == 0
        )
        with closing(sqlite3.connect(rep_database)) as standalone:
            assert {t: digest(standalone, t) for t in tables} == rep_hashes
            assert standalone.execute(
                "SELECT COUNT(*) FROM limit_new_capacity_share"
            ).fetchone() == (0,)
        build_transport.insert_transport_contribution(
            caller, rep, conflict="ignore_identical"
        )
        assert {t: digest(caller, t) for t in tables} == rep_hashes
        build_transport.insert_transport_contribution(
            caller, prepared, conflict="ignore_identical"
        )
        assert {t: digest(caller, t) for t in tables} == hashes
        assert caller.execute(
            "SELECT COUNT(*) FROM efficiency WHERE tech LIKE '%_REP%'"
        ).fetchone() == (0,)
        assert caller.execute("SELECT * FROM time_season").fetchall() == axes
        caller.rollback()
        assert caller.execute("SELECT COUNT(*) FROM technology").fetchone() == (0,)
        assert caller.execute(
            "SELECT COUNT(*) FROM limit_new_capacity_share"
        ).fetchone() == (1,)
        assert caller.execute(
            "SELECT COUNT(*) FROM capacity_factor_tech"
        ).fetchone() == (1,)
        assert caller.execute("SELECT * FROM time_season").fetchall() == axes

    # Failed standalone replacement must preserve the previously published complete DB.
    published_hash = hashlib.sha256(database.read_bytes()).hexdigest()
    bad_range = replace(
        prepared.range_representation,
        share_rows=prepared.range_representation.share_rows[:-1],
    )
    monkeypatch.setattr(
        build_transport,
        "prepare_transport_contribution",
        lambda *a, **kw: replace(prepared, range_representation=bad_range, charging_profiles=hourly_profiles,
            provenance_contexts=[*prepared.provenance_contexts, *hourly_profiles.provenance_contexts]),
    )
    with pytest.raises(ValueError, match="changed after validated preparation"):
        build_transport.bootstrap_database(
            bundle=b,
            template_dir=resolve_input_path(b, "template"),
            database_path=database,
            overwrite=True,
        )
    assert hashlib.sha256(database.read_bytes()).hexdigest() == published_hash
    assert not list(tmp_path.glob("*.tmp"))
