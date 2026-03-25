"""Output-writing helpers for the GTDB workflow."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
import logging
from pathlib import Path
import re
import sys
from typing import TYPE_CHECKING, Any, TextIO, TypedDict

import polars as pl
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from gtdb_genomes.download import (
    DEFAULT_REQUESTED_DOWNLOAD_METHOD,
)
from gtdb_genomes.layout import (
    RunDirectories,
    copy_accession_payload,
    get_accession_output_directory,
    get_duplicate_accessions,
    move_accession_payload,
    write_root_manifests,
    write_taxon_accessions,
)
from gtdb_genomes.logging_utils import attach_debug_log_handler, redact_text
from gtdb_genomes.metadata import SUPPRESSED_ASSEMBLY_NOTE
from gtdb_genomes.provenance import build_runtime_provenance
from gtdb_genomes.run_identity import (
    build_accession_decision_sha256,
    build_deterministic_run_id,
)
from gtdb_genomes.selection import build_taxon_slug_map
from gtdb_genomes.workflow_execution import (
    AccessionExecution,
    DownloadExecutionResult,
)

if TYPE_CHECKING:
    from gtdb_genomes.cli import CliArgs
    from gtdb_genomes.release_resolver import ReleaseResolution
    from gtdb_genomes.workflow_planning import SuppressedAccessionNote


# Logger and output-root helpers.


class RunSummaryRow(TypedDict):
    """Structured data for `run_summary.log`."""

    run_id: str
    accession_decision_sha256: str
    started_at: str
    finished_at: str
    requested_release: str
    resolved_release: str
    download_method_requested: str
    download_method_used: str
    threads_requested: int
    download_concurrency_used: int
    rehydrate_workers_used: int
    include: str
    prefer_genbank: str
    version_latest: str
    package_version: str
    git_revision: str
    datasets_version: str
    unzip_version: str
    release_manifest_sha256: str
    bacterial_taxonomy_sha256: str
    archaeal_taxonomy_sha256: str
    debug_enabled: str
    requested_taxa_count: int
    matched_rows: int
    unique_gtdb_accessions: int
    successful_accessions: int
    failed_accessions: int
    output_dir: str
    exit_code: int


class TaxonSummaryRow(TypedDict):
    """Structured row for `taxon_summary.tsv`."""

    requested_taxon: str
    unique_gtdb_accessions: int
    successful_accessions: int
    failed_accessions: int
    duplicate_copies_written: int
    output_dir: str


class EnrichedOutputRow(TypedDict):
    """Structured workflow row enriched with execution state."""

    requested_taxon: str
    taxon_slug: str
    resolved_release: str
    taxonomy_file: str
    lineage: str
    gtdb_accession: str
    ncbi_accession: str
    selected_accession: str
    download_request_accession: str
    final_accession: str
    conversion_status: str
    output_relpath: str
    download_status: str
    duplicate_across_taxa: bool


class AccessionMapRow(TypedDict):
    """Structured row for the condensed `accession_map.tsv`."""

    final_accession: str
    requested_taxa: str
    gtdb_accessions: str
    selected_accessions: str
    download_request_accessions: str
    conversion_status: str
    download_status: str
    output_relpaths: str
    duplicate_across_taxa: str


class PerTaxonOutputRow(TypedDict):
    """Structured row for `taxon_accessions.tsv`."""

    final_accession: str
    requested_taxon: str
    lineage: str
    gtdb_accession: str
    ncbi_accession: str
    selected_accession: str
    download_request_accession: str
    conversion_status: str
    output_relpath: str
    download_status: str
    duplicate_across_taxa: str


class FailureManifestRow(TypedDict):
    """Structured row for `download_failures.tsv`."""

    accession: str
    requested_taxa: str
    gtdb_accessions: str
    suppressed: str
    stage: str
    error_type: str
    reason: str
    status: str


class DuplicatedGenomeRow(TypedDict):
    """Structured row for `duplicated_genomes.tsv`."""

    final_accession: str
    requested_taxa: str
    taxa_count: int
    output_relpaths: str


@dataclass(frozen=True, slots=True)
class OutputTransferOperation:
    """One planned filesystem transfer for one taxon-accession output."""

    requested_taxon: str
    taxon_slug: str
    final_accession: str
    source_directory: Path
    destination_directory: Path
    move_eligible: bool
    duplicate_copy: bool


@dataclass(slots=True)
class TaxonTransferBatch:
    """One ordered batch of output transfers for one requested taxon."""

    requested_taxon: str
    taxon_slug: str
    taxon_index: int
    requested_taxa_total: int
    operations: list[OutputTransferOperation]


def configure_output_logger(
    args: CliArgs,
    logger: logging.Logger,
    run_directories: RunDirectories,
) -> logging.Logger:
    """Attach the output-root debug log handler for real runs when needed."""

    if args.debug:
        attach_debug_log_handler(
            logger,
            run_directories.output_root,
            secrets=(args.ncbi_api_key,),
        )
    return logger


# Manifest row builders.


DATASETS_HELP_FOOTER = (
    "Use datasets download genome accession <command> --help "
    "for detailed help about a command."
)
DATASETS_NO_MATCH_MESSAGE = (
    "Error: There are no genome assemblies that match your query. "
    "Please try again using different search criteria."
)
WHITESPACE_RUN_PATTERN = re.compile(r"\s+")
DIGEST_DISPLAY_PREFIX_LENGTH = 12


def normalise_failure_manifest_reason(reason: str) -> str:
    """Return one compact single-line manifest reason string."""

    normalised_reason = WHITESPACE_RUN_PATTERN.sub(" ", reason.strip())
    if normalised_reason.endswith(DATASETS_HELP_FOOTER):
        normalised_reason = normalised_reason.removesuffix(
            DATASETS_HELP_FOOTER,
        ).rstrip()
    return normalised_reason


def build_failure_manifest_reason(
    *,
    error_message: str,
    error_type: str,
    suppressed: bool,
) -> str:
    """Return one manifest-friendly terminal failure reason."""

    reason = normalise_failure_manifest_reason(error_message)
    if suppressed:
        if error_type == "subprocess" and reason == DATASETS_NO_MATCH_MESSAGE:
            return SUPPRESSED_ASSEMBLY_NOTE
        if reason:
            return f"{reason} {SUPPRESSED_ASSEMBLY_NOTE}"
        return SUPPRESSED_ASSEMBLY_NOTE
    return reason


def build_taxon_summary_rows(
    accession_rows: list[EnrichedOutputRow],
    duplicate_counts: dict[str, int],
    run_directories: RunDirectories,
    requested_taxa: tuple[str, ...],
    taxon_slug_map: dict[str, str],
) -> list[TaxonSummaryRow]:
    """Build `taxon_summary.tsv` rows from accession-level output rows."""

    grouped_rows: dict[str, list[EnrichedOutputRow]] = defaultdict(list)
    for row in accession_rows:
        grouped_rows[row["requested_taxon"]].append(row)

    summary_rows: list[TaxonSummaryRow] = []
    for requested_taxon in requested_taxa:
        rows = grouped_rows.get(requested_taxon, [])
        taxon_slug = taxon_slug_map[requested_taxon]
        summary_rows.append(
            {
                "requested_taxon": requested_taxon,
                "unique_gtdb_accessions": len(
                    {row["gtdb_accession"] for row in rows},
                ),
                "successful_accessions": len(
                    {
                        row["final_accession"]
                        for row in rows
                        if row["download_status"] != "failed"
                        and row["final_accession"]
                    },
                ),
                "failed_accessions": len(
                    {
                        row["gtdb_accession"]
                        for row in rows
                        if row["download_status"] == "failed"
                    },
                ),
                "duplicate_copies_written": duplicate_counts.get(requested_taxon, 0),
                "output_dir": str(run_directories.taxa_root / taxon_slug),
            },
        )
    return summary_rows


def build_run_summary_row(
    args: CliArgs,
    resolution: ReleaseResolution,
    method_used: str,
    download_concurrency_used: int,
    rehydrate_workers_used: int,
    matched_rows: int,
    accession_rows: list[dict[str, Any]],
    output_root: Path | None,
    exit_code: int,
    started_at: str,
    finished_at: str,
) -> RunSummaryRow:
    """Build the single `run_summary.log` data record."""

    provenance = build_runtime_provenance(
        release_manifest_sha256=resolution.release_manifest_sha256,
        bacterial_taxonomy_sha256=resolution.bacterial_taxonomy_sha256,
        archaeal_taxonomy_sha256=resolution.archaeal_taxonomy_sha256,
    )
    accession_decision_sha256 = build_accession_decision_sha256(accession_rows)
    return {
        "run_id": build_deterministic_run_id(
            requested_release=args.gtdb_release,
            resolved_release=resolution.resolved_release,
            requested_taxa=args.gtdb_taxa,
            include=args.include,
            prefer_genbank=args.prefer_genbank,
            version_latest=args.version_latest,
            provenance=provenance,
            accession_decision_sha256=accession_decision_sha256,
        ),
        "accession_decision_sha256": accession_decision_sha256,
        "started_at": started_at,
        "finished_at": finished_at,
        "requested_release": args.gtdb_release,
        "resolved_release": resolution.resolved_release,
        "download_method_requested": DEFAULT_REQUESTED_DOWNLOAD_METHOD,
        "download_method_used": method_used,
        "threads_requested": args.threads,
        "download_concurrency_used": download_concurrency_used,
        "rehydrate_workers_used": rehydrate_workers_used,
        "include": args.include,
        "prefer_genbank": str(args.prefer_genbank).lower(),
        "version_latest": str(args.version_latest).lower(),
        "package_version": provenance.package_version,
        "git_revision": provenance.git_revision,
        "datasets_version": provenance.datasets_version,
        "unzip_version": provenance.unzip_version,
        "release_manifest_sha256": provenance.release_manifest_sha256,
        "bacterial_taxonomy_sha256": (
            provenance.bacterial_taxonomy_sha256 or ""
        ),
        "archaeal_taxonomy_sha256": (
            provenance.archaeal_taxonomy_sha256 or ""
        ),
        "debug_enabled": str(args.debug).lower(),
        "requested_taxa_count": len(args.gtdb_taxa),
        "matched_rows": matched_rows,
        "unique_gtdb_accessions": len(
            {row["gtdb_accession"] for row in accession_rows},
        ),
        "successful_accessions": len(
            {
                row["final_accession"]
                for row in accession_rows
                if row["download_status"] != "failed" and row["final_accession"]
            },
        ),
        "failed_accessions": len(
            {
                row["gtdb_accession"]
                for row in accession_rows
                if row["download_status"] == "failed"
            },
        ),
        "output_dir": "" if output_root is None else str(output_root),
        "exit_code": exit_code,
    }


def render_run_summary_log(run_summary: RunSummaryRow) -> str:
    """Render one human-readable `run_summary.log` file."""

    sections = (
        ("Run Identity", ("run_id", "accession_decision_sha256", "started_at", "finished_at")),
        (
            "Release And Provenance",
            (
                "requested_release",
                "resolved_release",
                "download_method_requested",
                "download_method_used",
                "threads_requested",
                "download_concurrency_used",
                "rehydrate_workers_used",
                "include",
                "prefer_genbank",
                "version_latest",
                "package_version",
                "git_revision",
                "datasets_version",
                "unzip_version",
                "release_manifest_sha256",
                "bacterial_taxonomy_sha256",
                "archaeal_taxonomy_sha256",
                "debug_enabled",
            ),
        ),
        (
            "Counts",
            (
                "requested_taxa_count",
                "matched_rows",
                "unique_gtdb_accessions",
                "successful_accessions",
                "failed_accessions",
            ),
        ),
        ("Paths And Exit", ("output_dir", "exit_code")),
    )
    lines: list[str] = []
    for section_title, section_keys in sections:
        lines.append(section_title)
        for key in section_keys:
            lines.append(
                f"{key}: {format_run_summary_value(key, run_summary[key])}",
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def format_run_summary_value(key: str, value: object) -> object:
    """Return one human-readable run-summary value."""

    if key not in {"run_id", "accession_decision_sha256"}:
        return value
    value_text = str(value)
    if len(value_text) <= DIGEST_DISPLAY_PREFIX_LENGTH:
        return value_text
    return f"{value_text[:DIGEST_DISPLAY_PREFIX_LENGTH]}..."


def join_sorted_values(values: list[str]) -> str:
    """Return deterministic semicolon-joined unique non-empty values."""

    return ";".join(sorted({value.strip() for value in values if value.strip()}))


def join_unique_row_values(
    rows: list[EnrichedOutputRow],
    field_name: str,
) -> str:
    """Collapse one row field into a deterministic semicolon-joined value."""

    return join_sorted_values(
        [str(row.get(field_name, "")).strip() for row in rows],
    )


def build_accession_map_rows(
    enriched_rows: list[EnrichedOutputRow],
) -> list[AccessionMapRow]:
    """Build the condensed run-level `accession_map.tsv` rows."""

    grouped_rows: dict[str, list[EnrichedOutputRow]] = defaultdict(list)
    for row in enriched_rows:
        group_key = row["final_accession"] or row["ncbi_accession"]
        grouped_rows[group_key].append(row)

    accession_rows: list[AccessionMapRow] = []
    for group_key in sorted(grouped_rows):
        rows = grouped_rows[group_key]
        accession_rows.append(
            {
                "final_accession": join_unique_row_values(rows, "final_accession"),
                "requested_taxa": join_unique_row_values(rows, "requested_taxon"),
                "gtdb_accessions": join_unique_row_values(rows, "gtdb_accession"),
                "selected_accessions": join_unique_row_values(rows, "selected_accession"),
                "download_request_accessions": join_unique_row_values(
                    rows,
                    "download_request_accession",
                ),
                "conversion_status": join_unique_row_values(rows, "conversion_status"),
                "download_status": join_unique_row_values(rows, "download_status"),
                "output_relpaths": join_unique_row_values(rows, "output_relpath"),
                "duplicate_across_taxa": str(
                    any(row["duplicate_across_taxa"] for row in rows),
                ).lower(),
            },
        )
    return accession_rows


def build_duplicated_genome_rows(
    enriched_rows: list[EnrichedOutputRow],
) -> list[DuplicatedGenomeRow]:
    """Build one row per duplicated realised accession."""

    grouped_rows: dict[str, list[EnrichedOutputRow]] = defaultdict(list)
    for row in enriched_rows:
        if row["duplicate_across_taxa"] and row["final_accession"]:
            grouped_rows[row["final_accession"]].append(row)

    duplicate_rows: list[DuplicatedGenomeRow] = []
    for final_accession in sorted(grouped_rows):
        rows = grouped_rows[final_accession]
        requested_taxa = join_unique_row_values(rows, "requested_taxon")
        duplicate_rows.append(
            {
                "final_accession": final_accession,
                "requested_taxa": requested_taxa,
                "taxa_count": len(requested_taxa.split(";")) if requested_taxa else 0,
                "output_relpaths": join_unique_row_values(rows, "output_relpath"),
            },
        )
    return duplicate_rows


def build_failure_rows(
    enriched_rows: list[EnrichedOutputRow],
    executions: dict[str, AccessionExecution],
    secrets: tuple[str, ...],
    suppressed_notes: dict[str, SuppressedAccessionNote] | None = None,
) -> list[FailureManifestRow]:
    """Build terminal-failure `download_failures.tsv` rows."""

    failure_rows: list[FailureManifestRow] = []
    suppressed_accessions = {} if suppressed_notes is None else suppressed_notes

    rows_by_accession: dict[str, list[EnrichedOutputRow]] = defaultdict(list)
    for row in enriched_rows:
        rows_by_accession[row["ncbi_accession"]].append(row)

    for accession, rows in sorted(rows_by_accession.items()):
        execution = require_execution_record(executions, accession)
        if execution.download_status != "failed" or not execution.failures:
            continue
        failure = execution.failures[-1]
        suppressed = accession in suppressed_accessions
        reason = build_failure_manifest_reason(
            error_message=failure.error_message,
            error_type=failure.error_type,
            suppressed=suppressed,
        )
        failure_rows.append(
            {
                "accession": (
                    execution.final_accession
                    or execution.request_accession_used
                    or failure.attempted_accession
                    or accession
                ),
                "requested_taxa": join_unique_row_values(rows, "requested_taxon"),
                "gtdb_accessions": join_unique_row_values(rows, "gtdb_accession"),
                "suppressed": str(suppressed).lower(),
                "stage": failure.stage,
                "error_type": failure.error_type,
                "reason": redact_text(reason, secrets),
                "status": failure.final_status,
            },
        )
    return sorted(failure_rows, key=lambda row: row["accession"])


# Output materialisation and exit handling.


def require_execution_record(
    executions: dict[str, AccessionExecution],
    accession: str,
) -> AccessionExecution:
    """Return one execution record or raise a clear internal-contract error."""

    execution = executions.get(accession)
    if execution is not None:
        return execution
    raise RuntimeError(
        f"Internal error: missing execution record for accession {accession}",
    )


def build_enriched_output_rows(
    resolved_release: str,
    mapped_frame: pl.DataFrame,
    executions: dict[str, AccessionExecution],
) -> list[EnrichedOutputRow]:
    """Build enriched manifest rows without materialising any payloads."""

    enriched_rows: list[EnrichedOutputRow] = []
    for row in mapped_frame.rows(named=True):
        execution = require_execution_record(executions, row["ncbi_accession"])
        selected_accession = row["final_accession"]
        final_accession = execution.final_accession or ""
        enriched_rows.append(
            {
                "requested_taxon": row["requested_taxon"],
                "taxon_slug": row["taxon_slug"],
                "resolved_release": resolved_release,
                "taxonomy_file": row["taxonomy_file"],
                "lineage": row["lineage"],
                "gtdb_accession": row["gtdb_accession"],
                "ncbi_accession": row["ncbi_accession"],
                "selected_accession": selected_accession,
                "download_request_accession": execution.request_accession_used,
                "final_accession": final_accession,
                "conversion_status": execution.conversion_status,
                "output_relpath": "",
                "download_status": execution.download_status,
                "duplicate_across_taxa": False,
            },
        )

    duplicate_accessions = get_duplicate_accessions(enriched_rows)
    for row in enriched_rows:
        row["duplicate_across_taxa"] = row["final_accession"] in duplicate_accessions

    return enriched_rows


def build_transfer_batches(
    enriched_rows: list[EnrichedOutputRow],
    executions: dict[str, AccessionExecution],
    run_directories: RunDirectories,
    requested_taxa: tuple[str, ...],
    *,
    keep_temp: bool,
) -> tuple[
    list[TaxonTransferBatch],
    dict[str, int],
]:
    """Build one ordered output-transfer plan for all successful accessions."""

    taxon_slug_map = build_taxon_slug_map(requested_taxa)
    taxon_order = {
        taxon_slug_map[requested_taxon]: index
        for index, requested_taxon in enumerate(requested_taxa)
    }
    source_by_accession: dict[str, Path] = {}
    accessions_by_taxon: dict[str, set[str]] = defaultdict(set)

    for row in enriched_rows:
        if row["download_status"] == "failed" or not row["final_accession"]:
            continue
        accessions_by_taxon[row["taxon_slug"]].add(row["final_accession"])
        payload_directory = require_execution_record(
            executions,
            row["ncbi_accession"],
        ).payload_directory
        if payload_directory is None:
            raise RuntimeError(
                "Internal error: successful accessions must have payloads",
            )
        source_by_accession.setdefault(row["final_accession"], payload_directory)

    taxon_slugs_by_accession: dict[str, list[str]] = defaultdict(list)
    for taxon_slug, accessions in accessions_by_taxon.items():
        for accession in accessions:
            taxon_slugs_by_accession[accession].append(taxon_slug)

    owner_by_accession = {
        accession: sorted(
            taxon_slugs,
            key=lambda taxon_slug: taxon_order[taxon_slug],
        )[-1]
        for accession, taxon_slugs in taxon_slugs_by_accession.items()
    }
    duplicate_accessions = {
        row["final_accession"]
        for row in enriched_rows
        if row["duplicate_across_taxa"] and row["final_accession"]
    }
    duplicate_counts: dict[str, int] = defaultdict(int)
    transfer_batches: list[TaxonTransferBatch] = []

    for taxon_index, requested_taxon in enumerate(requested_taxa, start=1):
        taxon_slug = taxon_slug_map[requested_taxon]
        shared_accessions = sorted(
            accession
            for accession in accessions_by_taxon.get(taxon_slug, set())
            if accession in duplicate_accessions
        )
        unique_accessions = sorted(
            accession
            for accession in accessions_by_taxon.get(taxon_slug, set())
            if accession not in duplicate_accessions
        )
        operations: list[OutputTransferOperation] = []
        for accession in (*shared_accessions, *unique_accessions):
            owner_taxon = owner_by_accession[accession]
            duplicate_copy = accession in duplicate_accessions and taxon_slug != owner_taxon
            move_eligible = not keep_temp and (
                accession not in duplicate_accessions or taxon_slug == owner_taxon
            )
            operations.append(
                OutputTransferOperation(
                    requested_taxon=requested_taxon,
                    taxon_slug=taxon_slug,
                    final_accession=accession,
                    source_directory=source_by_accession[accession],
                    destination_directory=get_accession_output_directory(
                        run_directories,
                        taxon_slug,
                        accession,
                    ),
                    move_eligible=move_eligible,
                    duplicate_copy=duplicate_copy,
                ),
            )
            if duplicate_copy:
                duplicate_counts[requested_taxon] += 1
        transfer_batches.append(
            TaxonTransferBatch(
                requested_taxon=requested_taxon,
                taxon_slug=taxon_slug,
                taxon_index=taxon_index,
                requested_taxa_total=len(requested_taxa),
                operations=operations,
            ),
        )

    return transfer_batches, duplicate_counts


def create_taxon_progress_bar(
    batch: TaxonTransferBatch,
    *,
    stream: TextIO | None = None,
) -> tqdm:
    """Create one progress bar for one taxon output-materialisation batch."""

    return tqdm(
        total=len(batch.operations),
        desc=(
            f"taxa {batch.taxon_index}/{batch.requested_taxa_total} "
            f"{batch.taxon_slug}"
        ),
        file=stream or sys.stderr,
        leave=True,
        ascii=True,
        dynamic_ncols=False,
        bar_format=(
            "{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} {postfix}"
        ),
    )


def execute_transfer_batches(
    enriched_rows: list[EnrichedOutputRow],
    transfer_batches: list[TaxonTransferBatch],
    run_directories: RunDirectories,
    logger: logging.Logger,
) -> tuple[dict[str, list[PerTaxonOutputRow]], dict[tuple[str, str], str]]:
    """Execute one planned set of output transfers and build taxon rows."""

    output_relpaths: dict[tuple[str, str], str] = {}
    remaining_move_eligible_by_source: dict[Path, int] = defaultdict(int)
    for batch in transfer_batches:
        for operation in batch.operations:
            if operation.move_eligible:
                remaining_move_eligible_by_source[operation.source_directory] += 1

    with logging_redirect_tqdm(loggers=[logger]):
        for batch in transfer_batches:
            with create_taxon_progress_bar(batch) as progress_bar:
                for operation in batch.operations:
                    action = "copy"
                    if (
                        operation.move_eligible
                        and remaining_move_eligible_by_source[
                            operation.source_directory
                        ]
                        == 1
                    ):
                        action = "move"
                    if action == "move":
                        move_accession_payload(
                            operation.source_directory,
                            operation.destination_directory,
                        )
                    else:
                        copy_accession_payload(
                            operation.source_directory,
                            operation.destination_directory,
                        )
                    if operation.move_eligible:
                        remaining_move_eligible_by_source[
                            operation.source_directory
                        ] -= 1
                    output_relpaths[
                        (operation.taxon_slug, operation.final_accession)
                    ] = str(
                        operation.destination_directory.relative_to(
                            run_directories.output_root,
                        ),
                    )
                    if operation.duplicate_copy:
                        logger.debug(
                            "Copied duplicate genome %s into taxon %s",
                            operation.final_accession,
                            operation.taxon_slug,
                        )
                    progress_bar.update(1)
                    progress_bar.set_postfix_str(
                        (
                            f"finished={operation.final_accession} "
                            f"action={action}"
                        ),
                        refresh=True,
                    )

    for row in enriched_rows:
        if row["download_status"] == "failed" or not row["final_accession"]:
            continue
        row["output_relpath"] = output_relpaths[
            (row["taxon_slug"], row["final_accession"])
        ]

    per_taxon_rows: dict[str, list[PerTaxonOutputRow]] = defaultdict(list)
    for row in enriched_rows:
        per_taxon_rows[row["taxon_slug"]].append(
            {
                "final_accession": row["final_accession"],
                "requested_taxon": row["requested_taxon"],
                "lineage": row["lineage"],
                "gtdb_accession": row["gtdb_accession"],
                "ncbi_accession": row["ncbi_accession"],
                "selected_accession": row["selected_accession"],
                "download_request_accession": row["download_request_accession"],
                "conversion_status": row["conversion_status"],
                "output_relpath": row["output_relpath"],
                "download_status": row["download_status"],
                "duplicate_across_taxa": str(row["duplicate_across_taxa"]).lower(),
            },
        )
    return per_taxon_rows, output_relpaths


def resolve_exit_code(
    enriched_rows: list[EnrichedOutputRow],
) -> tuple[int, int, int]:
    """Return success count, failure count, and workflow exit code."""

    successful_count = len(
        {
            row["final_accession"]
            for row in enriched_rows
            if row["download_status"] != "failed" and row["final_accession"]
        },
    )
    failed_count = len(
        {
            row["gtdb_accession"]
            for row in enriched_rows
            if row["download_status"] == "failed"
        },
    )
    if failed_count == 0:
        return successful_count, failed_count, 0
    if successful_count > 0:
        return successful_count, failed_count, 6
    return successful_count, failed_count, 7


def materialise_real_run_outputs(
    args: CliArgs,
    logger: logging.Logger,
    run_directories: RunDirectories,
    started_at: str,
    resolution: ReleaseResolution,
    mapped_frame: pl.DataFrame,
    execution_result: DownloadExecutionResult,
    unsupported_executions: dict[str, AccessionExecution],
    secrets: tuple[str, ...],
    suppressed_notes: dict[str, SuppressedAccessionNote] | None = None,
) -> int:
    """Copy payloads, write manifests, and return the final exit code."""

    executions = {
        **execution_result.executions,
        **unsupported_executions,
    }
    logger.info("Writing output manifests to %s", run_directories.output_root)
    enriched_rows = build_enriched_output_rows(
        resolution.resolved_release,
        mapped_frame,
        executions,
    )
    transfer_batches, duplicate_counts = build_transfer_batches(
        enriched_rows,
        executions,
        run_directories,
        args.gtdb_taxa,
        keep_temp=args.keep_temp,
    )
    per_taxon_rows, _ = execute_transfer_batches(
        enriched_rows,
        transfer_batches,
        run_directories,
        logger,
    )
    taxon_slug_map = build_taxon_slug_map(args.gtdb_taxa)
    for requested_taxon in args.gtdb_taxa:
        per_taxon_rows.setdefault(taxon_slug_map[requested_taxon], [])
    failure_rows = build_failure_rows(
        enriched_rows,
        executions,
        secrets,
        suppressed_notes=suppressed_notes,
    )
    taxon_summary_rows = build_taxon_summary_rows(
        enriched_rows,
        duplicate_counts,
        run_directories,
        args.gtdb_taxa,
        taxon_slug_map,
    )
    successful_count, failed_count, exit_code = resolve_exit_code(enriched_rows)
    run_summary_text = render_run_summary_log(
        build_run_summary_row(
            args,
            resolution,
            execution_result.method_used,
            execution_result.download_concurrency_used,
            execution_result.rehydrate_workers_used,
            mapped_frame.height,
            enriched_rows,
            run_directories.output_root,
            exit_code,
            started_at,
            datetime.now(UTC).isoformat(),
        ),
    )
    write_root_manifests(
        run_directories,
        run_summary_text,
        taxon_summary_rows,
        build_accession_map_rows(enriched_rows),
        failure_rows,
        build_duplicated_genome_rows(enriched_rows),
    )
    for requested_taxon in args.gtdb_taxa:
        taxon_slug = taxon_slug_map[requested_taxon]
        write_taxon_accessions(run_directories, taxon_slug, per_taxon_rows[taxon_slug])
    logger.info(
        "Run finished: successful_accessions=%d failed_accessions=%d exit_code=%d",
        successful_count,
        failed_count,
        exit_code,
    )
    return exit_code
