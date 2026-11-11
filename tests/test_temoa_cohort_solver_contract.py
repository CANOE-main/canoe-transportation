"""Opt-in real Temoa acceptance and HiGHS micro-fixtures, not a full solve.

Run in a Temoa environment with TEMOA_SOURCE_ROOT set to its verified checkout.
The expected source identity is upstream main cb2f51503ddb4dc6607c73635098d34c5e2d1283.
Set CANOE_CURVE_DATABASE to check every published curve in a read-only SQLite
connection. Without it, only synthetic solver fixtures run. Nothing is written
to a source checkout, compiled database, or parameter artifact.
"""

import ast
from collections import defaultdict
import hashlib
from math import isfinite
import os
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from parameterization.road_cohort_survival import build_composite_cohorts


def _verified_temoa_source():
    location = os.environ.get("TEMOA_SOURCE_ROOT")
    if not location:
        pytest.skip("Set TEMOA_SOURCE_ROOT and use its Pyomo/HiGHS environment for the upstream contract test")
    source_root = Path(location)
    for relative, expected in (
        ("temoa/components/technology.py", "9957db97966398ad7d24dd9d274c9e6cb7d08c6e"),
        ("temoa/components/time.py", "769d8825e156957bdd87d30a67a567f5206eb6cf"),
    ):
        source_bytes = (source_root / relative).read_bytes().replace(b"\r\n", b"\n")
        blob = b"blob " + str(len(source_bytes)).encode() + b"\0" + source_bytes
        assert hashlib.sha1(blob).hexdigest() == expected
    source = (source_root / "temoa/components/capacity.py").read_text(encoding="utf-8")
    function = next(node for node in ast.parse(source).body
                    if isinstance(node, ast.FunctionDef) and node.name == "adjusted_capacity_constraint")
    assert hashlib.sha256(ast.get_source_segment(source, function).encode()).hexdigest() == (
        "01571de773be3860e2ea59706727888aa80dc13360775aa2eeb21ed0a6d52528")
    sys.path.insert(0, str(source_root))
    from temoa.components import technology, time

    assert Path(technology.__file__).resolve() == (source_root / "temoa/components/technology.py").resolve()
    assert Path(time.__file__).resolve() == (source_root / "temoa/components/time.py").resolve()
    return source_root


@pytest.mark.parametrize("mode,period,expected", [
    ("curve", 2020, 62.0), ("fixed", 2025, 20.0), ("trimmed_tail", 2012, 10000.1),
])
def test_actual_temoa_annual_capacity_constraints_solve(mode, period, expected):
    _verified_temoa_source()
    from pyomo.environ import ConcreteModel, Constraint, NonNegativeReals, Objective, SolverFactory, Var, maximize, value
    from temoa.components.capacity import adjusted_capacity_constraint, capacity_annual_constraint
    from temoa.components.technology import create_survival_curve, get_default_survival, param_process_life_fraction_rule

    class Sparse(dict):
        def sparse_keys(self):
            return self.keys()

    class Survival(Sparse):
        def __missing__(self, key):
            return get_default_survival(model, *key)

    schedule = pd.DataFrame({"road_class": "cars", "age": range(11),
                             "survival_probability": [1, .9, .8, .7, .6, .5, .4, .3, .2, .1, 0]})
    annual = pd.DataFrame({"region": "ON", "road_class": "cars", "tech": "car_EX",
                          "vintage_year": [2011, 2015], "vintage": 2010,
                          "calibrated_acquisitions_k_vehicles": [100., 200.]})
    if mode == "trimmed_tail":
        schedule = pd.DataFrame({"road_class": "cars", "age": range(3),
                                 "survival_probability": [1., .5, 0.]})
        annual["vintage_year"] = [2011, 2012]
        annual["calibrated_acquisitions_k_vehicles"] = [99999., 1.]
    cohort = build_composite_cohorts(
        annual, schedule, reference_year=int(annual.vintage_year.max()),
        first_model_period=period)
    if mode == "trimmed_tail":
        assert cohort.curves.period.max() == 2013
        assert cohort.tail_losses.removed_k_vehicles.sum() == 0.5
    curve = mode != "fixed"
    r, t, v = "ON", "car_EX", 2010
    amount = cohort.capacity.capacity.item() if curve else 100.0
    life = cohort.lifetimes.lifetime.item() if curve else 16
    pm = ConcreteModel()
    pm.cap = Var(domain=NonNegativeReals)
    pm.activity = Var(domain=NonNegativeReals)
    fractions = Survival({(r, int(row.period), t, v): float(row.fraction)
                          for row in cohort.curves.itertuples()}) if curve else Survival()
    model = SimpleNamespace(
        efficiency=Sparse(), existing_capacity=Sparse({(r, t, v): amount}),
        lifetime_process={(r, t, v): life}, is_survival_curve_process={},
        survival_curve_periods={}, lifetime_survival_curve=fractions,
        time_exist=[v], time_optimize=[period], tech_retirement=set(), period_length={period: 5},
        retired_existing_capacity=defaultdict(float), v_capacity={(r, period, t, v): pm.cap},
        capacity_to_activity={(r, t): 1}, process_inputs={(r, period, t, v): ["fuel"]},
        process_outputs_by_input={(r, period, t, v, "fuel"): ["service"]},
        v_flow_out_annual={(r, period, "fuel", t, v, "service"): pm.activity},
    )
    create_survival_curve(model)
    model.process_life_frac = {(r, period, t, v): param_process_life_fraction_rule(model, r, period, t, v)}
    pm.capacity = Constraint(expr=adjusted_capacity_constraint(model, r, period, t, v))
    pm.production = Constraint(expr=capacity_annual_constraint(model, r, period, t, v))
    pm.objective = Objective(expr=pm.activity, sense=maximize)
    result = SolverFactory("appsi_highs").solve(pm)
    assert str(result.solver.termination_condition) == "optimal"
    assert value(pm.cap) == pytest.approx(expected)
    assert value(pm.activity) == pytest.approx(expected)


def test_published_database_curves_with_actual_temoa(record_property):
    database_path = os.environ.get("CANOE_CURVE_DATABASE")
    if not database_path:
        pytest.skip("Set CANOE_CURVE_DATABASE to opt into read-only published-curve acceptance")
    _verified_temoa_source()
    from temoa.components.technology import (
        create_survival_curve, get_default_survival,
        param_process_life_fraction_rule, populate_core_dictionaries,
    )
    from temoa.components.time import init_set_time_optimize, param_period_length

    class Sparse(dict):
        def sparse_keys(self):
            return self.keys()

    class Survival(Sparse):
        def __missing__(self, key):
            return get_default_survival(model, *key)

    class OrderedPeriods(list):
        def last(self):
            return self[-1]

    path = Path(database_path).resolve(strict=True)
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
        connection.execute("PRAGMA query_only=ON")
        annual_rows = connection.execute(
            "SELECT region, period, tech, vintage, fraction FROM lifetime_survival_curve"
        ).fetchall()
        lifetimes = {(r, t, v): lifetime for r, t, v, lifetime in connection.execute(
            "SELECT region, tech, vintage, lifetime FROM lifetime_process")}
        efficiencies = connection.execute(
            "SELECT region, input_comm, tech, vintage, output_comm, efficiency FROM efficiency"
        ).fetchall()
        capacities = Sparse({(r, t, v): capacity for r, t, v, capacity in connection.execute(
            "SELECT region, tech, vintage, capacity FROM existing_capacity")})
        future = [row[0] for row in connection.execute(
            "SELECT period FROM time_period WHERE flag='f' ORDER BY period")]
        historical = OrderedPeriods(row[0] for row in connection.execute(
            "SELECT period FROM time_period WHERE flag='e' ORDER BY period"))
    assert annual_rows, "The selected database contains no annual survival curves"
    assert len(future) >= 2, "Temoa requires future period starts plus a final horizon boundary"
    curve_keys = {(r, t, v) for r, _, t, v, _ in annual_rows}
    assert curve_keys <= lifetimes.keys()
    fractions = Survival({(r, p, t, v): value for r, p, t, v, value in annual_rows})
    assert len(fractions) == len(annual_rows)
    # Check every emitted curve; build active periods from actual nonzero
    # efficiency edges through Temoa's own temporal/process indexing functions.
    model = SimpleNamespace(
        efficiency=Sparse({(r, i, t, v, o): value for r, i, t, v, o, value in efficiencies
                           if (r, t, v) in curve_keys}),
        existing_capacity=capacities, lifetime_process=lifetimes,
        lifetime_survival_curve=fractions, is_survival_curve_process={}, survival_curve_periods={},
        time_future=future, time_exist=historical, vintage_exist=historical, tech_uncap=set(),
        used_techs=set(), process_inputs={}, process_outputs={},
        commodity_down_stream_process={}, commodity_up_stream_process={},
        process_outputs_by_input={}, process_inputs_by_output={}, process_vintages={}, process_periods={},
    )
    model.time_optimize = init_set_time_optimize(model)
    model.period_length = {period: param_period_length(model, period) for period in model.time_optimize}
    create_survival_curve(model)
    assert set(model.survival_curve_periods) == curve_keys
    assert len(fractions) == len(annual_rows), "Published annual curves should need no interpolation"
    populate_core_dictionaries(model)
    checked_pairs = 0
    partial_retirement_pairs = 0
    for (region, tech, vintage), periods in model.process_periods.items():
        assert (region, tech, vintage) in curve_keys
        for period in periods:
            fraction = param_process_life_fraction_rule(model, region, period, tech, vintage)
            assert isfinite(fraction) and 0 < fraction <= 1, (region, period, tech, vintage, fraction)
            checked_pairs += 1
            if vintage + lifetimes[region, tech, vintage] < period + model.period_length[period]:
                partial_retirement_pairs += 1
    assert checked_pairs > 0
    for name, value in {
        "annual_curve_rows": len(annual_rows), "curve_processes": len(curve_keys),
        "active_curve_processes": len(model.process_periods),
        "optimized_periods": model.time_optimize,
        "checked_process_period_pairs": checked_pairs,
        "partial_retirement_pairs": partial_retirement_pairs,
    }.items():
        record_property(name, value)
        print(f"{name}={value}")
