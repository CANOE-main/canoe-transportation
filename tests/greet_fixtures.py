"""Small synthetic GREET dependencies independent of generated production inputs.

These labels exercise archetype resolution only. They are not ATB observations or
evidence for GREET calculation accuracy, emissions values or model calibration.
"""

from pathlib import Path
import shutil

import pandas as pd

from utils import ConfigBundle, load_config_bundle, load_harmonization_rules, resolve_input_path


ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "config/scenarios/default.yaml"
ATB_DETAILS = (
    "Gasoline Plug-in Hybrid Electric Vehicle (35-mile electric range)",
    "Gasoline Plug-in Hybrid Electric Vehicle (50-mile electric range)",
    "Battery Electric Vehicle (150-mile range)",
    "Battery Electric Vehicle (200-mile range)",
    "Battery Electric Vehicle (300-mile range)",
    "Battery Electric Vehicle (400-mile range)",
)


def isolated_greet_config(directory: Path) -> ConfigBundle:
    """Copy versioned contracts and supply only literal fixture archetype labels."""
    shutil.copytree(ROOT / "config", directory / "config")
    shutil.copytree(ROOT / "inputs/0_canoe_template", directory / "inputs/0_canoe_template")
    bundle = load_config_bundle(SCENARIO, repo_root=directory)
    rules = load_harmonization_rules(bundle, "nlr_atb_autonomie")
    path = resolve_input_path(
        bundle, "interim", rules["interim_subdir"],
        rules["components"]["vehicles"]["output_file"],
    )
    assert path.is_relative_to(directory)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "source_id": ["nlr_atb_transportation"] * len(ATB_DETAILS),
        "vehicle_weight_category": ["Light Duty"] * len(ATB_DETAILS),
        "vehicle_detail": ATB_DETAILS,
    }).to_csv(path, index=False, lineterminator="\n")
    return bundle
