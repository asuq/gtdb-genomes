"""Logging helpers for gtdb-genomes."""

from __future__ import annotations

from collections.abc import Iterable
import logging
from logging.handlers import MemoryHandler
from pathlib import Path
import re
import shlex


LOGGER_NAME = "gtdb_genomes"
DEBUG_LOG_BUFFER_CAPACITY = 10_000
REDACTION_TOKEN = "[REDACTED]"
GENERIC_SECRET_PATTERNS = (
    re.compile(
        r"(?P<prefix>\bNCBI_API_KEY=)(?P<quote>['\"]?)(?P<value>[^\s'\";]+)(?P=quote)",
    ),
    re.compile(
        r"(?P<prefix>--(?:ncbi-)?api-key=)(?P<quote>['\"]?)(?P<value>[^\s'\";]+)(?P=quote)",
    ),
    re.compile(
        r"(?P<prefix>--(?:ncbi-)?api-key\s+)(?P<quote>['\"]?)(?P<value>[^\s'\";]+)(?P=quote)",
    ),
    re.compile(
        r"(?P<prefix>\b(?:x-)?api-key\b\s*:\s*)(?P<quote>['\"]?)(?P<value>[^\s'\",;]+)(?P=quote)",
        re.IGNORECASE,
    ),
    re.compile(
        r'(?P<prefix>"(?:ncbi_api_key|api_key|api-key)"\s*:\s*")(?P<value>[^"]*)(?P<suffix>")',
        re.IGNORECASE,
    ),
    re.compile(
        r"(?P<prefix>'(?:ncbi_api_key|api_key|api-key)'\s*:\s*')(?P<value>[^']*)(?P<suffix>')",
        re.IGNORECASE,
    ),
)


def normalise_secrets(secrets: Iterable[str | None]) -> tuple[str, ...]:
    """Return the non-empty secrets that should be redacted from logs."""

    return tuple(secret for secret in secrets if secret)


def redact_known_secret_patterns(text: str) -> str:
    """Redact recognised API-key spellings from one text value."""

    redacted_text = text
    for pattern in GENERIC_SECRET_PATTERNS:
        redacted_text = pattern.sub(
            lambda match: (
                f"{match.group('prefix')}"
                f"{match.groupdict().get('quote', '')}"
                f"{REDACTION_TOKEN}"
                f"{match.groupdict().get('suffix', match.groupdict().get('quote', ''))}"
            ),
            redacted_text,
        )
    return redacted_text


def redact_text(text: str, secrets: Iterable[str | None]) -> str:
    """Redact all known secrets from one text value."""

    redacted_text = redact_known_secret_patterns(text)
    for secret in normalise_secrets(secrets):
        redacted_text = redacted_text.replace(secret, REDACTION_TOKEN)
    return redacted_text


def format_command(command: list[str]) -> str:
    """Format a subprocess argv list for human-readable logging."""

    return shlex.join(command)


def redact_command(
    command: list[str],
    secrets: Iterable[str | None],
) -> str:
    """Render a shell-safe command string with secrets redacted."""

    return redact_text(format_command(command), secrets)


class RedactingFormatter(logging.Formatter):
    """Logging formatter that sanitises recognised secrets in rendered output."""

    def __init__(self, fmt: str, *, secrets: Iterable[str | None]) -> None:
        """Initialise the formatter with one sanitised secret set."""

        super().__init__(fmt)
        self._secrets = normalise_secrets(secrets)

    def format(self, record: logging.LogRecord) -> str:
        """Format one log record and redact secret material."""

        return redact_text(super().format(record), self._secrets)


def get_logger() -> logging.Logger:
    """Return the package logger."""

    return logging.getLogger(LOGGER_NAME)


def configure_console_logging(
    debug: bool = False,
    *,
    secrets: Iterable[str | None] = (),
) -> logging.Logger:
    """Configure console logging for the current process."""

    logger = get_logger()
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    close_logger(logger)
    logger.propagate = False

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG if debug else logging.INFO)
    handler.setFormatter(
        RedactingFormatter("%(levelname)s %(message)s", secrets=secrets),
    )
    logger.addHandler(handler)
    return logger


def configure_logging(
    debug: bool = False,
    dry_run: bool = False,
    output_root: Path | None = None,
    *,
    secrets: Iterable[str | None] = (),
) -> tuple[logging.Logger, Path | None]:
    """Configure console logging and, when allowed, the debug log file."""

    logger = configure_console_logging(debug=debug, secrets=secrets)
    debug_log_path: Path | None = None
    if debug and not dry_run and output_root is None:
        buffer_handler = MemoryHandler(
            capacity=DEBUG_LOG_BUFFER_CAPACITY,
            flushLevel=logging.CRITICAL + 1,
        )
        buffer_handler.setLevel(logging.DEBUG)
        buffer_handler.setFormatter(
            RedactingFormatter(
                "%(asctime)s %(levelname)s %(message)s",
                secrets=secrets,
            ),
        )
        logger.addHandler(buffer_handler)
    elif debug and not dry_run and output_root is not None:
        debug_log_path = output_root / "debug.log"
        debug_log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(debug_log_path, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            RedactingFormatter(
                "%(asctime)s %(levelname)s %(message)s",
                secrets=secrets,
            ),
        )
        logger.addHandler(file_handler)
    return logger, debug_log_path


def attach_debug_log_handler(
    logger: logging.Logger,
    output_root: Path,
    *,
    secrets: Iterable[str | None] = (),
) -> Path | None:
    """Attach the real-run debug log handler and flush any buffered records."""

    if logger.level > logging.DEBUG:
        return None
    debug_log_path = output_root / "debug.log"
    debug_log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(debug_log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        RedactingFormatter(
            "%(asctime)s %(levelname)s %(message)s",
            secrets=secrets,
        ),
    )
    logger.addHandler(file_handler)
    for handler in tuple(logger.handlers):
        if not isinstance(handler, MemoryHandler):
            continue
        handler.setTarget(file_handler)
        handler.flush()
        handler.close()
        logger.removeHandler(handler)
    return debug_log_path


def close_logger(logger: logging.Logger) -> None:
    """Close and detach all handlers from the package logger."""

    for handler in tuple(logger.handlers):
        handler.close()
        logger.removeHandler(handler)
