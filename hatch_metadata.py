"""Custom Hatch metadata helpers for external runtime requirements."""

from __future__ import annotations


EXTERNAL_RUNTIME_REQUIREMENTS = (
    "ncbi-datasets-cli (>=18.4.0,<18.22.0)",
    "unzip (>=6.0,<7.0)",
)


def get_external_runtime_requirements() -> tuple[str, ...]:
    """Return the external runtime requirements advertised in built metadata."""

    return EXTERNAL_RUNTIME_REQUIREMENTS
