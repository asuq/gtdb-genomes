"""Resolve NCBI eligibility once, before any genome payload is downloaded."""

from __future__ import annotations

from dataclasses import replace
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from gtdb_genomes.download import CommandFailureRecord, write_accession_input_file
from gtdb_genomes.metadata import run_summary_lookup_with_retries
from gtdb_genomes.workflow_execution_models import AccessionExecution, AccessionPlan
from gtdb_genomes.workflow_planning import create_staging_directory

if TYPE_CHECKING:
    from gtdb_genomes.cli import CliArgs


def resolve_eligible_plans(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    logger: logging.Logger,
) -> tuple[tuple[AccessionPlan, ...], dict[str, AccessionExecution]]:
    """Return pinned eligible plans and terminal exclusions/metadata failures."""

    if not args.ncbi_filters or not plans:
        return plans, {}
    requests = tuple(dict.fromkeys(
        request
        for plan in plans
        for request in (
            (plan.download_request_accession, plan.original_accession)
            if plan.conversion_status == "paired_to_gca"
            else (plan.download_request_accession,)
        )
    ))
    logger.info("Checking NCBI eligibility for %d candidate accession(s)", len(requests))
    with create_staging_directory("gtdb_genomes_eligibility_") as directory:
        input_path = Path(directory) / "accessions.txt"
        baseline = run_summary_lookup_with_retries(
            requests, write_accession_input_file(input_path, requests),
            ncbi_api_key=args.ncbi_api_key, primary_only=True,
        )
        resolved = {
            request: next(iter(accessions))
            for request, accessions in baseline.summary_map.items()
        }
        exact_accessions = tuple(dict.fromkeys(resolved.values()))
        eligible: set[str] = set()
        if exact_accessions:
            filtered = run_summary_lookup_with_retries(
                exact_accessions,
                write_accession_input_file(input_path, exact_accessions),
                ncbi_api_key=args.ncbi_api_key,
                filter_args=args.ncbi_filters,
                primary_only=True,
            )
            eligible = set(filtered.status_map)

    retained: list[AccessionPlan] = []
    terminal: dict[str, AccessionExecution] = {}
    criteria = json.dumps(args.ncbi_filters, ensure_ascii=True)
    for plan in plans:
        target = resolved.get(plan.download_request_accession)
        if target is not None and target in eligible:
            retained.append(replace(
                plan,
                download_request_accession=target,
                fallback_allowed=plan.original_accession in eligible,
            ))
            continue
        failures: tuple[CommandFailureRecord, ...] = ()
        if target is None:
            failures = (CommandFailureRecord(
                stage="metadata_lookup", attempt_index=1, max_attempts=1,
                error_type="filter_metadata_missing",
                error_message=(
                    "Cannot evaluate NCBI criteria: no primary metadata record for "
                    f"{plan.download_request_accession}"
                ),
                final_status="metadata_unavailable",
                attempted_accession=plan.download_request_accession,
            ),)
        terminal[plan.original_accession] = AccessionExecution(
            original_accession=plan.original_accession,
            final_accession=None,
            conversion_status=plan.conversion_status,
            download_status="failed" if failures else "excluded",
            download_batch="", payload_directory=None, failures=failures,
            request_accession_used=target or plan.download_request_accession,
            exclusion_reason="" if failures else f"Excluded by NCBI criteria: {criteria}",
        )
    logger.info(
        "NCBI eligibility: eligible=%d excluded=%d metadata_unavailable=%d",
        len(retained),
        sum(execution.download_status == "excluded" for execution in terminal.values()),
        sum(execution.download_status == "failed" for execution in terminal.values()),
    )
    return tuple(retained), terminal
