"""Tests for logging helpers and secret redaction."""

from __future__ import annotations

from pathlib import Path

from gtdb_genomes.logging_utils import (
    close_logger,
    configure_logging,
    redact_command,
    redact_text,
)


def test_redaction_helpers_hide_secrets() -> None:
    """Redaction helpers should remove secrets from text and commands."""

    assert redact_text("token secret value", ["secret"]) == "token [REDACTED] value"
    assert redact_command(
        ["datasets", "--api-key", "secret"],
        ["secret"],
    ) == "datasets --api-key [REDACTED]"


def test_configure_logging_writes_debug_log_for_real_runs(tmp_path: Path) -> None:
    """Debug logging should create a file handler for non-dry runs."""

    logger, debug_log_path = configure_logging(
        debug=True,
        dry_run=False,
        output_root=tmp_path,
    )
    logger.debug("debug message")
    close_logger(logger)

    assert debug_log_path == tmp_path / "debug.log"
    assert debug_log_path.read_text().strip().endswith("DEBUG debug message")


def test_configure_logging_skips_debug_file_for_dry_run(tmp_path: Path) -> None:
    """Dry-run debug logging should remain console-only."""

    logger, debug_log_path = configure_logging(
        debug=True,
        dry_run=True,
        output_root=tmp_path,
    )
    logger.debug("console only")
    close_logger(logger)

    assert debug_log_path is None
    assert not (tmp_path / "debug.log").exists()
