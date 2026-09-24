import sqlite3

from validation.legacy_compare import compare_legacy_costs


def test_cost_parity_reports_exact_key_differences(tmp_path) -> None:
    candidate = tmp_path / "candidate.sqlite"
    reference = tmp_path / "reference.sqlite"
    for path, invest, variable in (
        (candidate, 12.0, 3.0), (reference, 10.0, 3.0)
    ):
        with sqlite3.connect(path) as connection:
            invest_table = "cost_invest" if path == candidate else "CostInvest"
            variable_table = "cost_variable" if path == candidate else "CostVariable"
            connection.execute(
                f"CREATE TABLE {invest_table}(region TEXT, tech TEXT, vintage INTEGER, cost REAL)"
            )
            connection.execute(
                f"CREATE TABLE {variable_table}(region TEXT, tech TEXT, period INTEGER, vintage INTEGER, cost REAL)"
            )
            connection.execute(
                f"INSERT INTO {invest_table} VALUES ('ON','T_A_N',2025,?)", (invest,)
            )
            connection.execute(
                f"INSERT INTO {variable_table} VALUES ('ON','T_A_N',2025,2025,?)",
                (variable,),
            )
    result = compare_legacy_costs(
        candidate, reference, absolute_tolerance=1e-9, relative_tolerance=1e-6
    )
    assert result["invest"]["value_differences"] == 1
    assert result["invest"]["median_candidate_to_legacy_ratio"] == 1.2
    assert result["variable"]["equal_within_diagnostic_tolerance"] == 1
