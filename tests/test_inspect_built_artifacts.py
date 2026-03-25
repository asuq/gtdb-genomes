"""Tests for built-artifact inspection helpers."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import zipfile

import pytest


def load_artifact_inspector():
    """Load the built-artifact inspector module from the local `bin` path."""

    inspector_path = (
        Path(__file__).resolve().parents[1]
        / "bin"
        / "inspect_built_artifacts.py"
    )
    module_spec = importlib.util.spec_from_file_location(
        "inspect_built_artifacts",
        inspector_path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(f"Could not load artifact inspector from {inspector_path}")
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def write_duplicate_record_wheel(wheel_path: Path) -> None:
    """Write one synthetic wheel whose `RECORD` repeats the same payload path."""

    init_payload = "__version__ = '0.2.0'\n"
    with zipfile.ZipFile(wheel_path, "w") as handle:
        handle.writestr("gtdb_genomes/__init__.py", init_payload)
        handle.writestr(
            "gtdb_genomes-0.2.0.dist-info/WHEEL",
            (
                "Wheel-Version: 1.0\n"
                "Generator: test\n"
                "Root-Is-Purelib: true\n"
                "Tag: py3-none-any\n"
            ),
        )
        handle.writestr(
            "gtdb_genomes-0.2.0.dist-info/METADATA",
            "Metadata-Version: 2.4\nName: gtdb-genomes\nVersion: 0.2.0\n",
        )
        record_hash = load_artifact_inspector().build_record_hash(
            init_payload.encode("utf-8"),
        )
        record_text = "\n".join(
            [
                f"gtdb_genomes/__init__.py,{record_hash},{len(init_payload)}",
                f"gtdb_genomes/__init__.py,{record_hash},{len(init_payload)}",
                "gtdb_genomes-0.2.0.dist-info/WHEEL,,",
                "gtdb_genomes-0.2.0.dist-info/METADATA,,",
                "gtdb_genomes-0.2.0.dist-info/RECORD,,",
                "",
            ],
        )
        handle.writestr(
            "gtdb_genomes-0.2.0.dist-info/RECORD",
            record_text,
        )


def test_validate_wheel_record_rejects_duplicate_rows(
    tmp_path: Path,
) -> None:
    """The inspector should reject duplicate wheel `RECORD` rows explicitly."""

    wheel_path = tmp_path / "gtdb_genomes-0.2.0-py3-none-any.whl"
    write_duplicate_record_wheel(wheel_path)

    inspector = load_artifact_inspector()
    with pytest.raises(ValueError, match="duplicate rows for gtdb_genomes/__init__.py"):
        inspector.validate_wheel_record(wheel_path)
