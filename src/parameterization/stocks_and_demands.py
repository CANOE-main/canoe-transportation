"""Derive Ontario LDV existing-stock age cohorts from mapped Report A stock."""

import argparse
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from utils import (
    ConfigBundle,
    load_config_bundle,
    load_harmonization_rules,
    resolve_artifact_path,
    write_dataframe_atomic,
)


ROAD_RULE_KEY = "road_aggregation"
STOCK_RULE_KEY = "stocks_and_demands"
LIFETIME_RULE_KEY = "lifetimes_survival"


def module_rules(bundle: ConfigBundle) -> dict[str, Any]:
    """Load Ontario stock-and-demand rules."""
    return load_harmonization_rules(bundle, STOCK_RULE_KEY)


def median_lifetime_map(medians: pd.DataFrame) -> dict[str, float]:
    """Return unique CEUD-class median-equivalent ages."""
    selected = medians.loc[medians["target_system"].eq("nrcan_ceud")].copy()
    if selected.empty:
        raise ValueError("No NRCan CEUD median-equivalent lifetimes are available")
    duplicates = selected.duplicated("target_class", keep=False)
    if duplicates.any():
        raise ValueError("Duplicate NRCan CEUD median-equivalent lifetime rows")
    if selected["median_equivalent_age"].isna().any():
        missing = selected.loc[
            selected["median_equivalent_age"].isna(),
            "target_class",
        ].tolist()
        raise ValueError(f"Missing median-equivalent ages for: {missing}")
    return {
        str(row.target_class): float(row.median_equivalent_age)
        for row in selected.itertuples(index=False)
    }


def derive_ldv_age_distributions(
    mapped_stock: pd.DataFrame,
    *,
    survival_curves: bool,
    maximum_age: int,
    median_lifetimes: dict[str, float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the scenario-owned cutoff and report all excluded stock."""
    required = {
        "report_year",
        "MODEL_YEAR",
        "FIT_ACTIVE",
        "nrcan_ceud_class",
        "nlr_atb_class",
        "mapping_accepted",
    }
    missing = sorted(required - set(mapped_stock.columns))
    if missing:
        raise ValueError(
            "Mapped Ontario stock missing columns: " + ", ".join(missing)
        )
    accepted = mapped_stock.loc[mapped_stock["mapping_accepted"]].copy()
    accepted["report_year"] = pd.to_numeric(
        accepted["report_year"],
        errors="raise",
    ).astype(int)
    accepted["MODEL_YEAR"] = pd.to_numeric(
        accepted["MODEL_YEAR"],
        errors="coerce",
    ).astype("Int64")
    accepted["FIT_ACTIVE"] = pd.to_numeric(
        accepted["FIT_ACTIVE"],
        errors="coerce",
    )
    accepted["age"] = accepted["report_year"] - accepted["MODEL_YEAR"]
    accepted["pre_2000"] = accepted["MODEL_YEAR"].lt(2000)
    accepted["negative_age"] = accepted["age"].lt(0)
    if survival_curves:
        accepted["cutoff_age"] = float(maximum_age)
        accepted["cutoff_basis"] = "scenario_survival_curve_max_age"
    else:
        accepted["cutoff_age"] = accepted["nrcan_ceud_class"].map(
            median_lifetimes
        )
        missing_medians = sorted(
            accepted.loc[
                accepted["cutoff_age"].isna(),
                "nrcan_ceud_class",
            ]
            .dropna()
            .astype(str)
            .unique()
        )
        if missing_medians:
            raise ValueError(
                "Missing median-equivalent lifetime for mapped classes: "
                + ", ".join(missing_medians)
            )
        accepted["cutoff_basis"] = "source_derived_median_equivalent_age"
    accepted["over_cutoff"] = accepted["age"].gt(accepted["cutoff_age"])
    accepted["retained"] = (
        ~accepted["negative_age"]
        & ~accepted["over_cutoff"]
        & accepted["age"].notna()
    )

    findings: list[dict[str, Any]] = []
    for ceud_class, rows in accepted.groupby("nrcan_ceud_class", dropna=False):
        retained = rows.loc[rows["retained"]]
        findings.append(
            {
                "report_year": int(rows["report_year"].max()),
                "nrcan_ceud_class": ceud_class,
                "cutoff_basis": rows["cutoff_basis"].iloc[0],
                "cutoff_age": rows["cutoff_age"].iloc[0],
                "source_rows": len(rows),
                "source_fit_active_stock": rows["FIT_ACTIVE"].sum(),
                "negative_age_rows": int(rows["negative_age"].sum()),
                "negative_age_fit_active_stock": rows.loc[
                    rows["negative_age"],
                    "FIT_ACTIVE",
                ].sum(),
                "pre_2000_rows": int(rows["pre_2000"].sum()),
                "pre_2000_fit_active_stock": rows.loc[
                    rows["pre_2000"],
                    "FIT_ACTIVE",
                ].sum(),
                "over_cutoff_rows": int(rows["over_cutoff"].sum()),
                "over_cutoff_fit_active_stock": rows.loc[
                    rows["over_cutoff"],
                    "FIT_ACTIVE",
                ].sum(),
                "retained_rows": len(retained),
                "retained_fit_active_stock": retained["FIT_ACTIVE"].sum(),
            }
        )

    retained = accepted.loc[accepted["retained"]].copy()
    age_distribution = (
        retained.groupby(
            [
                "report_year",
                "nrcan_ceud_class",
                "nlr_atb_class",
                "MODEL_YEAR",
                "age",
                "cutoff_basis",
                "cutoff_age",
            ],
            as_index=False,
            dropna=False,
        )["FIT_ACTIVE"]
        .sum(min_count=1)
        .rename(
            columns={
                "MODEL_YEAR": "model_year",
                "FIT_ACTIVE": "fit_active_stock",
            }
        )
    )
    totals = age_distribution.groupby(
        ["report_year", "nrcan_ceud_class", "nlr_atb_class"]
    )["fit_active_stock"].transform("sum")
    age_distribution["age_distribution"] = (
        age_distribution["fit_active_stock"] / totals
    )
    return (
        age_distribution.sort_values(
            ["report_year", "nrcan_ceud_class", "nlr_atb_class", "age"],
            kind="stable",
        ).reset_index(drop=True),
        pd.DataFrame(findings),
    )


def build_existing_stock_age_artifacts(scenario_path: str | Path) -> Path:
    """Publish scenario-dependent Ontario LDV existing-stock age cohorts."""
    bundle = load_config_bundle(scenario_path)
    rules = module_rules(bundle)["ontario_report_a"]
    road_rules = load_harmonization_rules(bundle, ROAD_RULE_KEY)
    lifetime_rules = load_harmonization_rules(bundle, LIFETIME_RULE_KEY)
    output_dir = resolve_artifact_path(bundle, "stocks_and_demands")
    mapped = pd.read_csv(
        resolve_artifact_path(bundle, "road_aggregation")
        / road_rules["mapped_current_stock_file"],
        low_memory=False,
    )
    medians = pd.read_csv(
        resolve_artifact_path(bundle, "lifetimes_survival")
        / lifetime_rules["median_lifetimes_file"]
    )
    age_distribution, findings = derive_ldv_age_distributions(
        mapped,
        survival_curves=bundle.scenario.switches.survival_curves,
        maximum_age=bundle.scenario.switches.survival_curve_max_age,
        median_lifetimes=median_lifetime_map(medians),
    )
    write_dataframe_atomic(
        age_distribution,
        output_dir / str(rules["age_distribution_file"]),
    )
    write_dataframe_atomic(
        findings,
        resolve_artifact_path(bundle, "lifetime_validation")
        / str(rules["truncation_findings_file"]),
    )
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        default="config/scenarios/legacy_reproduction.yaml",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()
    output_dir = build_existing_stock_age_artifacts(args.scenario)
    logging.info("Wrote Ontario LDV age-cohort artifacts to %s", output_dir)


if __name__ == "__main__":
    main()
