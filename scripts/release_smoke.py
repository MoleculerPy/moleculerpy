#!/usr/bin/env python3
"""Ecosystem release smoke checks for MoleculerPy packages.

Builds each package, validates metadata with twine, and verifies that a clean
virtualenv can install the built wheel and import the package.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import venv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PackageCheck:
    """Release smoke configuration for one package."""

    name: str
    path: Path
    import_name: str
    version_expr: str
    local_dependencies: tuple[str, ...] = ()


ROOT = Path(__file__).resolve().parents[2]
PYTHON = Path(sys.executable)

PACKAGES = [
    PackageCheck(
        name="moleculerpy",
        path=ROOT / "moleculerpy",
        import_name="moleculerpy",
        version_expr="moleculerpy.__version__",
    ),
    PackageCheck(
        name="moleculerpy-channels",
        path=ROOT / "moleculerpy-channels",
        import_name="moleculerpy_channels",
        version_expr="moleculerpy_channels.__version__",
        local_dependencies=("moleculerpy",),
    ),
    PackageCheck(
        name="moleculerpy-repl",
        path=ROOT / "moleculerpy-repl",
        import_name="moleculerpy_repl",
        version_expr="moleculerpy_repl.__version__",
        local_dependencies=("moleculerpy",),
    ),
]


def run(command: list[str], *, cwd: Path) -> None:
    """Run a command and fail loudly on the first non-zero exit."""
    print(f"\n[{cwd.name}] $ {' '.join(command)}")
    subprocess.run(command, cwd=cwd, check=True)


def clean_build_artifacts(package: PackageCheck) -> None:
    """Remove stale build outputs so the smoke run is deterministic."""
    for artifact_name in ("build", "dist"):
        artifact_path = package.path / artifact_name
        if artifact_path.exists():
            shutil.rmtree(artifact_path)


def find_wheel(package: PackageCheck) -> Path:
    """Return the single built wheel for a package."""
    wheels = sorted((package.path / "dist").glob("*.whl"))
    if not wheels:
        raise FileNotFoundError(f"No wheel found for {package.name}")
    if len(wheels) > 1:
        raise RuntimeError(f"Expected one wheel for {package.name}, found {len(wheels)}")
    return wheels[0]


def dist_files(package: PackageCheck) -> list[Path]:
    """Return all built distribution files for a package."""
    return sorted((package.path / "dist").glob("*"))


def smoke_install(package: PackageCheck, wheels_by_name: dict[str, Path]) -> None:
    """Install the wheel in a clean virtualenv and import it."""
    wheel_path = find_wheel(package)
    with tempfile.TemporaryDirectory(prefix=f"{package.name}-smoke-") as tmp_dir:
        venv_path = Path(tmp_dir) / "venv"
        venv.EnvBuilder(with_pip=True).create(venv_path)
        pip_path = venv_path / "bin" / "pip"
        python_path = venv_path / "bin" / "python"

        for dependency_name in package.local_dependencies:
            dependency_wheel = wheels_by_name[dependency_name]
            run(
                [str(pip_path), "install", "--no-deps", str(dependency_wheel)],
                cwd=package.path,
            )

        run([str(pip_path), "install", str(wheel_path)], cwd=package.path)
        run(
            [
                str(python_path),
                "-c",
                (
                    f"import {package.import_name}; "
                    f"print({package.version_expr})"
                ),
            ],
            cwd=package.path,
        )


def has_git_repo(path: Path) -> bool:
    """Return True when the directory is a standalone git repository."""
    return (path / ".git").exists()


def main() -> int:
    """Run smoke checks for all ecosystem packages."""
    failures: list[str] = []
    wheels_by_name: dict[str, Path] = {}

    print(f"Workspace root: {ROOT}")
    print(f"Python: {PYTHON}")

    for package in PACKAGES:
        print(f"\n=== {package.name} ===")

        if not has_git_repo(package.path):
            print(f"[warn] {package.path} is not a standalone git repository")

        try:
            clean_build_artifacts(package)
            run([str(PYTHON), "-m", "build", "--no-isolation"], cwd=package.path)
            distributions = dist_files(package)
            if not distributions:
                raise FileNotFoundError(f"No dist files found for {package.name}")
            run(
                [str(PYTHON), "-m", "twine", "check", *[str(path) for path in distributions]],
                cwd=package.path,
            )
            wheels_by_name[package.name] = find_wheel(package)
            smoke_install(package, wheels_by_name)
        except Exception as exc:  # pragma: no cover - script-style error handling
            failures.append(f"{package.name}: {exc}")
            print(f"[fail] {package.name}: {exc}")
        else:
            print(f"[ok] {package.name}")

    if failures:
        print("\nRelease smoke failed:")
        for failure in failures:
            print(f" - {failure}")
        return 1

    print("\nAll release smoke checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
