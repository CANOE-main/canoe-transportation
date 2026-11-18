"""The built distribution exposes the caller-owned API without source-path help."""

from pathlib import Path
import shutil
import subprocess
import sys
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[1]


def test_wheel_exposes_builder_and_explicit_configuration_root(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    shutil.copytree(ROOT / "src", project / "src", ignore=shutil.ignore_patterns("__pycache__", "*.egg-info"))
    for filename in ("pyproject.toml", "README.md"):
        shutil.copyfile(ROOT / filename, project / filename)
    wheels = tmp_path / "wheels"
    built = subprocess.run(
        ["uv", "build", "--offline", "--wheel", "--out-dir", str(wheels)],
        cwd=project, capture_output=True, text=True,
    )
    assert built.returncode == 0, built.stderr
    wheel = next(wheels.glob("*.whl"))
    installed = tmp_path / "installed"
    with ZipFile(wheel) as archive:
        # Check the payload, including new runtime/diagnostic modules, rather than
        # allowing an editable checkout to hide an omitted package file.
        expected = [project / "src/build_transport.py"]
        for package in ("fetching", "parameterization", "utils", "validation"):
            expected.extend((project / "src" / package).rglob("*.py"))
        for source in expected:
            member = source.relative_to(project / "src").as_posix()
            assert archive.read(member) == source.read_bytes(), member
        assert not any(name.startswith(("config/", "inputs/")) for name in archive.namelist())
        archive.extractall(installed)

    artifacts = tmp_path / "artifacts"
    shutil.copytree(ROOT / "config", artifacts / "config")
    shutil.copyfile(ROOT / "pyproject.toml", artifacts / "pyproject.toml")
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    # -I ignores PYTHONPATH/cwd. Remove an editable checkout's .pth entry too;
    # every backend module must then originate in the actual wheel payload.
    code = """
import importlib
from pathlib import Path
import sys
installed, checkout, artifacts = map(Path, sys.argv[1:])
sys.path = [entry for entry in sys.path if Path(entry).resolve() not in {checkout, checkout / 'src'}]
sys.path.insert(0, str(installed))
modules = [importlib.import_module(name) for name in ('build_transport', 'fetching', 'parameterization', 'utils', 'validation')]
assert all(Path(module.__file__).is_relative_to(installed) for module in modules)
for name in ('parameterization.charger_representation', 'validation.charger_feasibility'):
    assert Path(importlib.import_module(name).__file__).is_relative_to(installed)
assert 'scipy' not in sys.modules  # optional LP support must not become a runtime import requirement
builder, _, _, utilities, _ = modules
assert all(callable(getattr(builder, name)) for name in ('prepare_scenario_inputs', 'prepare_transport_contribution', 'insert_transport_contribution'))
bundle = utilities.load_config_bundle('config/scenarios/default.yaml', repo_root=artifacts)
assert bundle.repo_root == artifacts
assert utilities.resolve_input_path(bundle, 'cache').is_relative_to(artifacts)
assert not (artifacts / 'src').exists()
print('installed API and explicit configuration root passed')
"""
    imported = subprocess.run(
        [sys.executable, "-I", "-c", code, str(installed), str(ROOT), str(artifacts)],
        cwd=unrelated, capture_output=True, text=True,
    )
    assert imported.returncode == 0, imported.stderr
    assert "installed API and explicit configuration root passed" in imported.stdout
