"""Contract-level edge-case tests for the integrated workflow."""

from __future__ import annotations

import io
import logging
from pathlib import Path

import polars as pl
import pytest

from gtdb_genomes.cli import CliArgs, main
from gtdb_genomes.download import (
    CommandFailureRecord,
    DownloadMethodDecision,
    PreviewError,
    RetryableCommandResult,
)
from gtdb_genomes.layout import LayoutError, initialise_run_directories
from gtdb_genomes.metadata import SummaryLookupResult
from gtdb_genomes.preflight import PreflightError
from gtdb_genomes.release_resolver import resolve_release
from gtdb_genomes.taxonomy import load_release_taxonomy
from gtdb_genomes.workflow import (
    AccessionExecution,
    AccessionPlan,
    DownloadExecutionResult,
    ResolvedPayloadDirectory,
    build_unsupported_uba_warning,
    build_failure_rows,
    create_staging_directory,
    execute_batch_dehydrate_plans,
    execute_direct_accession_plans,
    extract_download_payload,
    plan_supported_downloads,
)


def build_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a minimal taxonomy frame for workflow tests."""

    return pl.DataFrame(
        {
            "gtdb_accession": ["RS_GCF_000001.1"],
            "lineage": [lineage],
            "ncbi_accession": ["GCF_000001.1"],
            "taxonomy_file": ["bac120_taxonomy_r95.tsv"],
        },
    )


def build_mixed_uba_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a taxonomy frame with one supported and one legacy UBA accession."""

    return pl.DataFrame(
        {
            "gtdb_accession": ["RS_GCF_000001.1", "UBA11131"],
            "lineage": [lineage, lineage],
            "ncbi_accession": ["GCF_000001.1", "UBA11131"],
            "taxonomy_file": [
                "bac120_taxonomy_r80.tsv",
                "bac120_taxonomy_r80.tsv",
            ],
        },
    )


def build_multi_accession_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a taxonomy frame with repeated and distinct supported accessions."""

    return pl.DataFrame(
        {
            "gtdb_accession": [
                "RS_GCF_000001.1",
                "RS_GCF_000001.1_copy",
                "RS_GCF_000002.1",
            ],
            "lineage": [lineage, lineage, lineage],
            "ncbi_accession": [
                "GCF_000001.1",
                "GCF_000001.1",
                "GCF_000002.1",
            ],
            "taxonomy_file": [
                "bac120_taxonomy_r202.tsv",
                "bac120_taxonomy_r202.tsv",
                "bac120_taxonomy_r202.tsv",
            ],
        },
    )


def build_uba_only_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a taxonomy frame containing only unsupported UBA accessions."""

    return pl.DataFrame(
        {
            "gtdb_accession": ["UBA11131"],
            "lineage": [lineage],
            "ncbi_accession": ["UBA11131"],
            "taxonomy_file": ["bac120_taxonomy_r80.tsv"],
        },
    )


def build_shared_preferred_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a taxonomy frame whose rows share one preferred accession."""

    return pl.DataFrame(
        {
            "gtdb_accession": ["RS_GCF_001881595.2", "GB_GCA_001881595.3"],
            "lineage": [lineage, lineage],
            "ncbi_accession": ["GCF_001881595.2", "GCA_001881595.3"],
            "taxonomy_file": ["bac120_taxonomy_r80.tsv", "bac120_taxonomy_r80.tsv"],
        },
    )


def install_capture_logger(
    monkeypatch: pytest.MonkeyPatch,
) -> io.StringIO:
    """Patch workflow logging to capture warning text for assertions."""

    stream = io.StringIO()

    def fake_configure_logging(
        debug: bool = False,
        dry_run: bool = False,
        output_root: Path | None = None,
    ) -> tuple[logging.Logger, Path | None]:
        """Return a predictable test logger backed by one string buffer."""

        del dry_run, output_root
        logger = logging.getLogger(f"test-workflow-{id(stream)}")
        logger.handlers.clear()
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
        logger.propagate = False
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        logger.addHandler(handler)
        return logger, None

    def fake_close_logger(logger: logging.Logger) -> None:
        """Flush and detach handlers without closing the shared string buffer."""

        for handler in tuple(logger.handlers):
            handler.flush()
            logger.removeHandler(handler)

    monkeypatch.setattr(
        "gtdb_genomes.workflow.configure_logging",
        fake_configure_logging,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.close_logger",
        fake_close_logger,
    )
    return stream


def parse_tsv(path: Path) -> tuple[list[str], list[list[str]]]:
    """Return the header and rows from a TSV output file."""

    lines = path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split("\t")
    rows = [line.split("\t") for line in lines[1:]]
    return header, rows


def build_cli_args(output_dir: Path) -> CliArgs:
    """Build a minimal CLI argument object for workflow unit tests."""

    return CliArgs(
        gtdb_release="80",
        gtdb_taxa=("s__Escherichia coli",),
        outdir=output_dir,
        prefer_genbank=True,
        version_fixed=False,
        download_method="auto",
        threads=4,
        ncbi_api_key=None,
        include="genome",
        debug=False,
        keep_temp=False,
        dry_run=False,
    )


def test_zero_match_run_writes_header_only_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Zero matches should create the documented output tree and exit 4."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: (_ for _ in ()).throw(
            AssertionError("preflight should not run"),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame("d__Bacteria;p__Firmicutes;g__Bacillus"),
    )

    output_dir = tmp_path / "zero-match"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 4
    assert (output_dir / "run_summary.tsv").exists()
    assert (output_dir / "accession_map.tsv").read_text().splitlines() == [
        "requested_taxon\ttaxon_slug\tresolved_release\ttaxonomy_file\tlineage\tgtdb_accession\tfinal_accession\taccession_type_original\taccession_type_final\tconversion_status\tdownload_method_used\tdownload_batch\toutput_relpath\tdownload_status",
    ]
    assert (
        output_dir / "taxa" / "g__Escherichia" / "taxon_accessions.tsv"
    ).exists()


def test_mixed_uba_dry_run_warns_once_and_skips_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Mixed supported and UBA dry-runs should warn once and exit cleanly."""

    warning_stream = install_capture_logger(monkeypatch)
    required_tools_calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: required_tools_calls.append(tuple(required_tools)),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_mixed_uba_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("metadata lookup should not run"),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    output_dir = tmp_path / "mixed-uba-dry-run"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()
    assert required_tools_calls == [("unzip",), ("datasets",)]
    warning_text = warning_stream.getvalue()
    assert warning_text.count("unsupported legacy GTDB UBA accessions") == 1
    assert "PRJNA417962" in warning_text
    assert "GCF_000001.1" not in warning_text


def test_uba_only_dry_run_warns_once_and_skips_ncbi_calls(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """UBA-only dry-runs should warn and avoid metadata or preview calls."""

    warning_stream = install_capture_logger(monkeypatch)
    required_tools_calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: required_tools_calls.append(tuple(required_tools)),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_uba_only_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("metadata lookup should not run"),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("preview should not run"),
        ),
    )

    output_dir = tmp_path / "uba-only-dry-run"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()
    assert required_tools_calls == [("unzip",)]
    warning_text = warning_stream.getvalue()
    assert warning_text.count("unsupported legacy GTDB UBA accessions") == 1
    assert "PRJNA417962" in warning_text


def test_zero_match_dry_run_missing_unzip_returns_exit_five(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Dry-runs should fail early when `unzip` is unavailable."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame("d__Bacteria;p__Firmicutes;g__Bacillus"),
    )

    def raise_preflight_error(required_tools: tuple[str, ...]) -> None:
        """Fail on the new early dry-run unzip check."""

        assert required_tools == ("unzip",)
        raise PreflightError("Missing required external tools: unzip")

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        raise_preflight_error,
    )

    output_dir = tmp_path / "zero-match-dry-run-preflight"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--dry-run",
        ],
    )

    assert exit_code == 5
    assert not output_dir.exists()


def test_supported_prefer_genbank_dry_run_missing_tools_returns_exit_five(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Supported prefer-GenBank dry-runs should still enforce datasets."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )

    required_tools_calls: list[tuple[str, ...]] = []

    def raise_preflight_error(required_tools: tuple[str, ...]) -> None:
        """Fail when the workflow reaches the supported dry-run datasets check."""

        required_tools_calls.append(required_tools)
        if required_tools == ("unzip",):
            return None
        assert required_tools == ("datasets",)
        raise PreflightError("Missing required external tools: datasets")

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        raise_preflight_error,
    )

    output_dir = tmp_path / "prefer-genbank-dry-run-preflight"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--prefer-genbank",
            "--dry-run",
        ],
    )

    assert exit_code == 5
    assert not output_dir.exists()
    assert required_tools_calls == [("unzip",), ("datasets",)]


def test_supported_real_run_missing_tools_returns_exit_five(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Supported non-dry runs should still enforce datasets and unzip."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )

    def raise_preflight_error(required_tools: tuple[str, ...]) -> None:
        """Fail when the workflow reaches the supported real-run path."""

        assert required_tools == ("datasets", "unzip")
        raise PreflightError("Missing required external tools: datasets, unzip")

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        raise_preflight_error,
    )

    output_dir = tmp_path / "real-run-preflight"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 5
    assert not output_dir.exists()


def test_build_unsupported_uba_warning_mentions_examples_and_bioproject() -> None:
    """The UBA warning builder should produce deterministic user guidance."""

    warning_text = build_unsupported_uba_warning(
        pl.DataFrame(
            {
                "requested_taxon": [
                    "g__Escherichia",
                    "g__Escherichia",
                    "s__Escherichia coli",
                ],
                "ncbi_accession": ["UBA11131", "UBA11131", "UBA22222"],
            },
        ),
    )

    assert "Skipping 2 unsupported legacy GTDB UBA accessions" in warning_text
    assert "g__Escherichia;s__Escherichia coli" in warning_text
    assert "UBA11131, UBA22222" in warning_text
    assert "PRJNA417962" in warning_text


def test_extract_download_payload_reports_layout_stage_for_archive_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Archive extraction failures should be labelled as layout failures."""

    run_directories = initialise_run_directories(tmp_path / "layout-error")
    monkeypatch.setattr(
        "gtdb_genomes.workflow.extract_archive",
        lambda archive_path, extraction_root: (_ for _ in ()).throw(
            LayoutError("archive extraction failed"),
        ),
    )

    payload_directory, failures = extract_download_payload(
        "GCF_000001.1",
        tmp_path / "archive.zip",
        run_directories,
    )

    assert payload_directory is None
    assert len(failures) == 1
    assert failures[0].stage == "layout"


def test_extract_download_payload_resolves_realised_version_from_stem_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Stem requests should resolve the realised version from the payload path."""

    run_directories = initialise_run_directories(tmp_path / "layout-stem-resolution")

    def fake_extract_archive(archive_path: Path, extraction_root: Path) -> Path:
        """Create one extracted payload directory for the test."""

        del archive_path
        payload_directory = (
            extraction_root / "ncbi_dataset" / "data" / "GCA_000001.7"
        )
        payload_directory.mkdir(parents=True, exist_ok=True)
        return extraction_root

    monkeypatch.setattr(
        "gtdb_genomes.workflow.extract_archive",
        fake_extract_archive,
    )

    payload_directory, failures = extract_download_payload(
        "GCA_000001",
        tmp_path / "archive.zip",
        run_directories,
    )

    assert failures == ()
    assert payload_directory == ResolvedPayloadDirectory(
        final_accession="GCA_000001.7",
        directory=(
            run_directories.extracted_root
            / "GCA_000001"
            / "ncbi_dataset"
            / "data"
            / "GCA_000001.7"
        ),
    )


def test_extract_download_payload_ignores_nested_accession_named_directories(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Only top-level payload directories should be considered as assemblies."""

    run_directories = initialise_run_directories(tmp_path / "layout-nested-accession")

    def fake_extract_archive(archive_path: Path, extraction_root: Path) -> Path:
        """Create one real payload and one nested accession-like directory."""

        del archive_path
        top_level_payload = extraction_root / "ncbi_dataset" / "data" / "GCA_000001.7"
        nested_payload = (
            top_level_payload / "annotation" / "GCA_000001.8"
        )
        top_level_payload.mkdir(parents=True, exist_ok=True)
        nested_payload.mkdir(parents=True, exist_ok=True)
        return extraction_root

    monkeypatch.setattr(
        "gtdb_genomes.workflow.extract_archive",
        fake_extract_archive,
    )

    payload_directory, failures = extract_download_payload(
        "GCA_000001",
        tmp_path / "archive.zip",
        run_directories,
    )

    assert failures == ()
    assert payload_directory == ResolvedPayloadDirectory(
        final_accession="GCA_000001.7",
        directory=(
            run_directories.extracted_root
            / "GCA_000001"
            / "ncbi_dataset"
            / "data"
            / "GCA_000001.7"
        ),
    )


def test_extract_download_payload_falls_back_when_data_root_is_absent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A unique payload elsewhere in the tree should still be resolved."""

    run_directories = initialise_run_directories(tmp_path / "layout-missing-data-root")

    def fake_extract_archive(archive_path: Path, extraction_root: Path) -> Path:
        """Create one unique accession directory outside the normal data root."""

        del archive_path
        payload_directory = extraction_root / "relocated" / "GCA_000001.7"
        payload_directory.mkdir(parents=True, exist_ok=True)
        return extraction_root

    monkeypatch.setattr(
        "gtdb_genomes.workflow.extract_archive",
        fake_extract_archive,
    )

    payload_directory, failures = extract_download_payload(
        "GCA_000001",
        tmp_path / "archive.zip",
        run_directories,
    )

    assert failures == ()
    assert payload_directory == ResolvedPayloadDirectory(
        final_accession="GCA_000001.7",
        directory=(
            run_directories.extracted_root
            / "GCA_000001"
            / "relocated"
            / "GCA_000001.7"
        ),
    )


def test_extract_download_payload_fallback_ignores_nested_accession_directories(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Fallback payload discovery should ignore accession-like child directories."""

    run_directories = initialise_run_directories(
        tmp_path / "layout-fallback-nested-accession",
    )

    def fake_extract_archive(archive_path: Path, extraction_root: Path) -> Path:
        """Create one relocated payload with a nested accession-like child."""

        del archive_path
        top_level_payload = extraction_root / "relocated" / "GCA_000001.7"
        nested_payload = top_level_payload / "annotation" / "GCA_000001.8"
        top_level_payload.mkdir(parents=True, exist_ok=True)
        nested_payload.mkdir(parents=True, exist_ok=True)
        return extraction_root

    monkeypatch.setattr(
        "gtdb_genomes.workflow.extract_archive",
        fake_extract_archive,
    )

    payload_directory, failures = extract_download_payload(
        "GCA_000001",
        tmp_path / "archive.zip",
        run_directories,
    )

    assert failures == ()
    assert payload_directory == ResolvedPayloadDirectory(
        final_accession="GCA_000001.7",
        directory=(
            run_directories.extracted_root
            / "GCA_000001"
            / "relocated"
            / "GCA_000001.7"
        ),
    )


def test_auto_method_uses_unique_download_request_count_after_stem_collapse(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Auto mode should size the request after collapsing to datasets tokens."""

    supported_mapped_frame = pl.DataFrame(
        {
            "ncbi_accession": ["GCF_000001.1", "GCF_000001.2"],
            "final_accession": ["GCA_000001.1", "GCA_000001.2"],
            "conversion_status": ["paired_to_gca", "paired_to_gca"],
        },
    )
    args = CliArgs(
        gtdb_release="95",
        gtdb_taxa=("g__Escherichia",),
        outdir=tmp_path / "output",
        prefer_genbank=True,
        version_fixed=False,
        download_method="auto",
        threads=4,
        ncbi_api_key=None,
        include="genome",
        debug=False,
        keep_temp=False,
        dry_run=False,
    )

    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    observed_counts: list[int] = []

    def fake_select_download_method(
        requested_method: str,
        accession_count: int,
        preview_text: str | None = None,
    ) -> DownloadMethodDecision:
        """Capture the accession count passed into method selection."""

        observed_counts.append(accession_count)
        assert requested_method == "auto"
        assert preview_text == "Package size: 1.0 GB\n"
        return DownloadMethodDecision(
            requested_method="auto",
            method_used="direct",
            accession_count=accession_count,
            preview_size_bytes=1024,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow.select_download_method",
        fake_select_download_method,
    )

    plans, decision_method = plan_supported_downloads(
        supported_mapped_frame,
        args,
        logging.getLogger("test-auto-stem-collapse"),
        (),
    )

    assert len(plans) == 2
    assert {plan.download_request_accession for plan in plans} == {"GCA_000001"}
    assert observed_counts == [1]
    assert decision_method == "direct"


def test_release_80_contains_the_real_shared_preferred_accession_pair() -> None:
    """Release 80 should retain the known GCF/GCA duplicate pair."""

    taxonomy_frame = load_release_taxonomy(resolve_release("80"))
    selected = taxonomy_frame.filter(
        pl.col("ncbi_accession").is_in(
            ["GCF_001881595.2", "GCA_001881595.3"],
        ),
    )

    assert selected.select("ncbi_accession").rows() == [
        ("GCF_001881595.2",),
        ("GCA_001881595.3",),
    ]


def test_direct_mode_downloads_shared_preferred_accession_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Two originals that share one preferred accession should download once."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    download_calls: list[tuple[str, str]] = []
    extraction_calls: list[tuple[Path, Path]] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return one successful preferred download for the shared group."""

        del command, final_failure_status, sleep_func, runner
        download_calls.append((stage, attempted_accession or ""))
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create the extracted directory for one direct batch pass."""

        extraction_root.mkdir(parents=True, exist_ok=True)
        extraction_calls.append((archive_path, extraction_root))

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Return one shared resolved payload for the preferred accession."""

        assert extraction_root.name == "direct_batch_1"
        return (
            ResolvedPayloadDirectory(
                final_accession="GCA_001881595.5",
                directory=payload_directory,
            ),
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.collect_payload_directories",
        fake_collect_payload_directories,
    )

    run_directories = initialise_run_directories(tmp_path / "direct-shared-success")
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_001881595.2",
                selected_accession="GCA_001881595.3",
                download_request_accession="GCA_001881595",
                conversion_status="paired_to_gca",
            ),
            AccessionPlan(
                original_accession="GCA_001881595.3",
                selected_accession="GCA_001881595.3",
                download_request_accession="GCA_001881595",
                conversion_status="unchanged_original",
            ),
        ),
        build_cli_args(tmp_path / "out"),
        run_directories,
        logging.getLogger("test-direct-shared-success"),
    )

    assert result.download_concurrency_used == 1
    assert download_calls == [("preferred_download", "GCA_001881595")]
    assert extraction_calls == [
        (
            run_directories.downloads_root / "direct_batch_1.zip",
            run_directories.extracted_root / "direct_batch_1",
        ),
    ]
    assert result.executions["GCF_001881595.2"].final_accession == "GCA_001881595.5"
    assert result.executions["GCF_001881595.2"].download_status == "downloaded"
    assert result.executions["GCF_001881595.2"].download_batch == "direct_batch_1"
    assert result.executions["GCA_001881595.3"].final_accession == "GCA_001881595.5"
    assert result.executions["GCA_001881595.3"].download_status == "downloaded"
    assert result.executions["GCA_001881595.3"].download_batch == "direct_batch_1"


def test_direct_mode_retries_unresolved_accessions_in_later_batches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A later direct batch should retry only the unresolved request accession."""

    payload_one = tmp_path / "payload-one"
    payload_one.mkdir()
    payload_two = tmp_path / "payload-two"
    payload_two.mkdir()
    download_calls: list[tuple[str, str]] = []
    extraction_calls: list[tuple[Path, Path]] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return successful direct batch commands for both passes."""

        del command, final_failure_status, sleep_func, runner
        download_calls.append((stage, attempted_accession or ""))
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create one extraction root per direct batch pass."""

        extraction_root.mkdir(parents=True, exist_ok=True)
        extraction_calls.append((archive_path, extraction_root))

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Expose only one payload in the first pass, then the remaining one."""

        if extraction_root.name == "direct_batch_1":
            return (
                ResolvedPayloadDirectory(
                    final_accession="GCF_000001.1",
                    directory=payload_one,
                ),
            )
        if extraction_root.name == "direct_batch_2":
            return (
                ResolvedPayloadDirectory(
                    final_accession="GCF_000002.1",
                    directory=payload_two,
                ),
            )
        raise AssertionError(f"Unexpected extraction root: {extraction_root}")

    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.collect_payload_directories",
        fake_collect_payload_directories,
    )

    args = build_cli_args(tmp_path / "out")
    args.prefer_genbank = False

    run_directories = initialise_run_directories(tmp_path / "direct-batch-retry")
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_000001.1",
                selected_accession="GCF_000001.1",
                download_request_accession="GCF_000001.1",
                conversion_status="unchanged_original",
            ),
            AccessionPlan(
                original_accession="GCF_000002.1",
                selected_accession="GCF_000002.1",
                download_request_accession="GCF_000002.1",
                conversion_status="unchanged_original",
            ),
        ),
        args,
        run_directories,
        logging.getLogger("test-direct-batch-retry"),
    )

    assert result.download_concurrency_used == 1
    assert download_calls == [
        ("preferred_download", "GCF_000001.1;GCF_000002.1"),
        ("preferred_download", "GCF_000002.1"),
    ]
    assert extraction_calls == [
        (
            run_directories.downloads_root / "direct_batch_1.zip",
            run_directories.extracted_root / "direct_batch_1",
        ),
        (
            run_directories.downloads_root / "direct_batch_2.zip",
            run_directories.extracted_root / "direct_batch_2",
        ),
    ]
    assert result.executions["GCF_000001.1"].download_batch == "direct_batch_1"
    assert result.executions["GCF_000001.1"].failures == ()
    assert result.executions["GCF_000002.1"].download_batch == "direct_batch_2"
    assert [failure.final_status for failure in result.executions["GCF_000002.1"].failures] == [
        "retry_scheduled",
    ]
    assert [failure.attempted_accession for failure in result.executions["GCF_000002.1"].failures] == [
        "GCF_000002.1",
    ]


def test_direct_mode_falls_back_to_original_accession_after_preferred_phase(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Unresolved preferred rows should switch into original-accession fallback."""

    download_calls: list[tuple[str, str]] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return successful preferred and fallback batch commands."""

        del command, final_failure_status, sleep_func, runner
        download_calls.append((stage, attempted_accession or ""))
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create one extraction root per batch pass."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    payload_directory = tmp_path / "fallback-payload"
    payload_directory.mkdir()

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Keep the preferred batch unresolved, then resolve the fallback batch."""

        if extraction_root.name == "direct_batch_1":
            return ()
        if extraction_root.name == "direct_fallback_batch_1":
            return (
                ResolvedPayloadDirectory(
                    final_accession="GCF_001881595.2",
                    directory=payload_directory,
                ),
            )
        raise AssertionError(f"Unexpected extraction root: {extraction_root}")

    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.collect_payload_directories",
        fake_collect_payload_directories,
    )

    run_directories = initialise_run_directories(tmp_path / "direct-preferred-fallback")
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_001881595.2",
                selected_accession="GCA_001881595.3",
                download_request_accession="GCA_001881595",
                conversion_status="paired_to_gca",
            ),
            AccessionPlan(
                original_accession="GCA_001881595.3",
                selected_accession="GCA_001881595.3",
                download_request_accession="GCA_001881595",
                conversion_status="unchanged_original",
            ),
        ),
        build_cli_args(tmp_path / "out"),
        run_directories,
        logging.getLogger("test-direct-fallback-layout"),
    )

    assert download_calls == [
        ("preferred_download", "GCA_001881595"),
        ("fallback_download", "GCF_001881595.2"),
    ]
    assert result.executions["GCF_001881595.2"].final_accession == "GCF_001881595.2"
    assert result.executions["GCF_001881595.2"].download_batch == "direct_fallback_batch_1"
    assert result.executions["GCF_001881595.2"].download_status == "downloaded_after_fallback"
    assert (
        result.executions["GCF_001881595.2"].conversion_status
        == "paired_to_gca_fallback_original_on_download_failure"
    )
    assert [failure.attempted_accession for failure in result.executions["GCF_001881595.2"].failures] == [
        "GCA_001881595",
    ]
    assert result.executions["GCA_001881595.3"].final_accession is None
    assert result.executions["GCA_001881595.3"].download_status == "failed"
    assert result.executions["GCA_001881595.3"].download_batch == "direct_batch_1"
    assert [failure.attempted_accession for failure in result.executions["GCA_001881595.3"].failures] == [
        "GCA_001881595",
    ]
    assert [failure.final_status for failure in result.executions["GCA_001881595.3"].failures] == [
        "retry_exhausted",
    ]


def test_direct_mode_records_failed_fallback_after_layout_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Fallback exhaustion should retain both preferred and fallback history."""

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return successful direct batch commands for both phases."""

        del command
        del final_failure_status
        del sleep_func
        del runner
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create one extraction root per batch pass."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Keep both preferred and fallback phases unresolved."""

        del extraction_root
        return ()

    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.collect_payload_directories",
        fake_collect_payload_directories,
    )

    run_directories = initialise_run_directories(tmp_path / "direct-fallback-failed")
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_001881595.2",
                selected_accession="GCA_001881595.3",
                download_request_accession="GCA_001881595",
                conversion_status="paired_to_gca",
            ),
            AccessionPlan(
                original_accession="GCA_001881595.3",
                selected_accession="GCA_001881595.3",
                download_request_accession="GCA_001881595",
                conversion_status="unchanged_original",
            ),
        ),
        build_cli_args(tmp_path / "out"),
        run_directories,
        logging.getLogger("test-direct-fallback-failed"),
    )

    assert result.executions["GCF_001881595.2"].final_accession is None
    assert result.executions["GCF_001881595.2"].download_batch == "direct_fallback_batch_1"
    assert [failure.attempted_accession for failure in result.executions["GCF_001881595.2"].failures] == [
        "GCA_001881595",
        "GCF_001881595.2",
    ]
    assert [failure.final_status for failure in result.executions["GCF_001881595.2"].failures] == [
        "retry_exhausted",
        "retry_exhausted",
    ]
    assert result.executions["GCA_001881595.3"].final_accession is None
    assert result.executions["GCA_001881595.3"].download_batch == "direct_batch_1"
    assert [failure.attempted_accession for failure in result.executions["GCA_001881595.3"].failures] == [
        "GCA_001881595",
    ]


def test_auto_preview_failure_returns_exit_code_five_without_output_tree(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Preview failures in auto mode should stop before output creation."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(PreviewError("preview failed")),
    )

    output_dir = tmp_path / "preview-failure"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 5
    assert not output_dir.exists()


def test_auto_preview_uses_accession_input_file_and_keeps_output_absent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Auto preview should use a temporary input file outside the output tree."""

    preview_inputs: list[Path] = []
    preview_contents: list[str] = []

    def fake_run_preview_command(
        accession_file: Path,
        include: str,
        ncbi_api_key: str | None = None,
        datasets_bin: str = "datasets",
        debug: bool = False,
        sleep_func=None,
        runner=None,
    ) -> str:
        """Capture the preview input file used by auto mode."""

        del ncbi_api_key, datasets_bin, debug, sleep_func, runner
        preview_inputs.append(accession_file)
        preview_contents.append(accession_file.read_text(encoding="ascii"))
        assert include == "genome"
        assert accession_file.is_file()
        assert accession_file.parent.name.startswith("gtdb_genomes_preview_")
        return "Package size: 1.0 GB\n"

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_multi_accession_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        fake_run_preview_command,
    )

    output_dir = tmp_path / "preview-input-file"
    exit_code = main(
        [
            "--gtdb-release",
            "202",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()
    assert len(preview_inputs) == 1
    assert preview_contents == ["GCF_000001.1\nGCF_000002.1\n"]
    assert not preview_inputs[0].exists()


def test_dry_run_logs_info_milestones(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Dry-runs should emit the new high-level INFO milestones."""

    log_stream = install_capture_logger(monkeypatch)
    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    output_dir = tmp_path / "dry-run-info"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--dry-run",
        ],
    )

    assert exit_code == 0
    log_text = log_stream.getvalue()
    assert "INFO Starting run:" in log_text
    assert "INFO Checking unzip availability for dry-run" in log_text
    assert "INFO Resolved bundled release 95" in log_text
    assert "INFO Selected 1 supported accession(s) and 0 unsupported legacy accession(s)" in (
        log_text
    )
    assert "INFO Automatic planning selected direct for 1 supported accession(s)" in (
        log_text
    )
    assert "INFO Dry-run finished:" in log_text


def test_real_run_logs_info_milestones(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Real runs should emit the new high-level INFO milestones."""

    log_stream = install_capture_logger(monkeypatch)
    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_retryable_command",
        lambda *args, **kwargs: RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.extract_archive",
        lambda archive_path, extraction_root: extraction_root.mkdir(
            parents=True,
            exist_ok=True,
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.collect_payload_directories",
        lambda extraction_root: (
            ResolvedPayloadDirectory(
                final_accession="GCF_000001.1",
                directory=payload_directory,
            ),
        ),
    )

    output_dir = tmp_path / "real-run-info"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 0
    log_text = log_stream.getvalue()
    assert "INFO Starting run:" in log_text
    assert "INFO Resolved bundled release 95" in log_text
    assert "INFO Automatic planning selected direct for 1 supported accession(s)" in (
        log_text
    )
    assert "INFO direct_batch_1: starting preferred_download for 1 request accession(s)" in (
        log_text
    )
    assert "INFO direct_batch_1: completed with 1 resolved and 0 pending request accession(s)" in (
        log_text
    )
    assert "INFO Writing output manifests to" in log_text
    assert "INFO Run finished: successful_accessions=1 failed_accessions=0 exit_code=0" in (
        log_text
    )


def test_metadata_lookup_uses_accession_input_file_and_cleans_it_up(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Metadata lookup should use a temporary input file outside the output tree."""

    metadata_inputs: list[Path] = []
    metadata_contents: list[str] = []

    def fake_run_summary_lookup_with_retries(
        accessions: tuple[str, ...] | list[str],
        accession_file: Path,
        ncbi_api_key: str | None = None,
        datasets_bin: str = "datasets",
        sleep_func=None,
    ) -> SummaryLookupResult:
        """Capture the metadata input file used by workflow planning."""

        del ncbi_api_key, datasets_bin, sleep_func
        metadata_inputs.append(accession_file)
        metadata_contents.append(accession_file.read_text(encoding="ascii"))
        assert tuple(accessions) == ("GCF_000001.1", "GCF_000002.1")
        assert accession_file.is_file()
        assert accession_file.parent.name.startswith("gtdb_genomes_metadata_")
        return SummaryLookupResult(summary_map={}, failures=())

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_multi_accession_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        fake_run_summary_lookup_with_retries,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    output_dir = tmp_path / "metadata-input-file"
    exit_code = main(
        [
            "--gtdb-release",
            "202",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--prefer-genbank",
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()
    assert len(metadata_inputs) == 1
    assert metadata_contents == ["GCF_000001.1\nGCF_000002.1\n"]
    assert not metadata_inputs[0].exists()


def test_create_staging_directory_uses_tmpdir_when_configured(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Workflow staging directories should respect the configured temp root."""

    temp_root = tmp_path / "custom-temp-root"
    monkeypatch.setenv("TMPDIR", str(temp_root))

    with create_staging_directory("gtdb_genomes_test_") as staging_directory:
        staging_path = Path(staging_directory)
        assert staging_path.parent == temp_root
        assert staging_path.name.startswith("gtdb_genomes_test_")

    assert not staging_path.exists()


def test_total_runtime_failure_leaves_final_accession_blank(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Total failure should blank `final_accession` and exit 7."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return a failed accession execution for the synthetic run."""

        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    download_batch="GCF_000001.1",
                    payload_directory=None,
                    failures=(
                        CommandFailureRecord(
                            stage="preferred_download",
                            attempt_index=4,
                            max_attempts=4,
                            error_type="subprocess",
                            error_message="download failed",
                            final_status="retry_exhausted",
                        ),
                    ),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "runtime-failure"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 7
    accession_map_lines = (output_dir / "accession_map.tsv").read_text().splitlines()
    assert accession_map_lines[1].split("\t")[6] == ""
    assert accession_map_lines[1].split("\t")[9] == "failed_no_usable_accession"
    assert accession_map_lines[1].split("\t")[13] == "failed"
    run_summary_header, run_summary_rows = parse_tsv(output_dir / "run_summary.tsv")
    run_summary = dict(zip(run_summary_header, run_summary_rows[0], strict=True))
    assert run_summary["download_concurrency_used"] == "1"
    assert run_summary["rehydrate_workers_used"] == "0"


def test_mixed_uba_real_run_records_failed_unsupported_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Mixed supported and UBA runs should keep successes and audit skipped UBA rows."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_mixed_uba_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("metadata lookup should not run"),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    def fake_execute_accession_plans(
        plans: tuple[AccessionPlan, ...],
        args: CliArgs,
        decision_method: str,
        run_directories,
        logger,
        secrets: tuple[str, ...],
    ) -> DownloadExecutionResult:
        """Return one successful direct execution for the supported accession."""

        del args, run_directories, logger, secrets
        assert decision_method == "direct"
        assert [plan.original_accession for plan in plans] == ["GCF_000001.1"]
        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "mixed-uba-real"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 6
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_maps = [
        dict(zip(accession_header, row, strict=True))
        for row in accession_rows
    ]
    unsupported_row = next(
        row for row in accession_maps if row["gtdb_accession"] == "UBA11131"
    )
    assert unsupported_row["final_accession"] == ""
    assert unsupported_row["accession_type_original"] == "unknown"
    assert unsupported_row["accession_type_final"] == ""
    assert unsupported_row["conversion_status"] == "failed_no_usable_accession"
    assert unsupported_row["download_batch"] == "UBA11131"
    assert unsupported_row["download_status"] == "failed"

    taxon_header, taxon_rows = parse_tsv(
        output_dir / "taxa" / "g__Escherichia" / "taxon_accessions.tsv",
    )
    taxon_maps = [dict(zip(taxon_header, row, strict=True)) for row in taxon_rows]
    unsupported_taxon_row = next(
        row for row in taxon_maps if row["gtdb_accession"] == "UBA11131"
    )
    assert unsupported_taxon_row["final_accession"] == ""
    assert unsupported_taxon_row["download_status"] == "failed"
    assert unsupported_taxon_row["duplicate_across_taxa"] == "false"

    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    assert len(failure_rows) == 1
    failure = dict(zip(failure_header, failure_rows[0], strict=True))
    assert failure["gtdb_accession"] == "UBA11131"
    assert failure["attempted_accession"] == "UBA11131"
    assert failure["final_accession"] == ""
    assert failure["stage"] == "preflight"
    assert failure["error_type"] == "unsupported_accession"
    assert failure["final_status"] == "unsupported_input"
    assert "PRJNA417962" in failure["error_message_redacted"]


def test_uba_only_real_run_writes_failed_manifests_and_exits_seven(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """UBA-only real runs should skip downloads but still write audit manifests."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: (_ for _ in ()).throw(
            AssertionError("preflight should not run"),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_uba_only_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.execute_accession_plans",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("supported download execution should not run"),
        ),
    )

    output_dir = tmp_path / "uba-only-real"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 7
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_map = dict(zip(accession_header, accession_rows[0], strict=True))
    assert accession_map["gtdb_accession"] == "UBA11131"
    assert accession_map["final_accession"] == ""
    assert accession_map["download_method_used"] == "auto"
    assert accession_map["download_status"] == "failed"

    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    failure = dict(zip(failure_header, failure_rows[0], strict=True))
    assert failure["stage"] == "preflight"
    assert failure["error_type"] == "unsupported_accession"
    assert failure["final_status"] == "unsupported_input"

    run_summary_header, run_summary_rows = parse_tsv(output_dir / "run_summary.tsv")
    run_summary = dict(zip(run_summary_header, run_summary_rows[0], strict=True))
    assert run_summary["download_method_used"] == "auto"
    assert run_summary["download_concurrency_used"] == "0"


def test_shared_preferred_direct_manifest_uses_preferred_download_batch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Shared preferred direct success should record the preferred download batch."""

    payload_directory = tmp_path / "shared-preferred-payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_shared_preferred_taxonomy_frame(
            "d__Bacteria;p__Firmicutes;g__Bacillus",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one shared preferred direct-success result."""

        return DownloadExecutionResult(
            executions={
                "GCF_001881595.2": AccessionExecution(
                    original_accession="GCF_001881595.2",
                    final_accession="GCA_001881595.3",
                    conversion_status="paired_to_gca",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
                "GCA_001881595.3": AccessionExecution(
                    original_accession="GCA_001881595.3",
                    final_accession="GCA_001881595.3",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "shared-preferred-manifest"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Bacillus",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 0
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_maps = [
        dict(zip(accession_header, row, strict=True))
        for row in accession_rows
    ]
    assert {row["download_batch"] for row in accession_maps} == {"direct_batch_1"}


def test_direct_fallback_manifest_uses_actual_fallback_download_batch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Fallback rows should record the final per-accession direct batch."""

    payload_directory = tmp_path / "fallback-payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_shared_preferred_taxonomy_frame(
            "d__Bacteria;p__Firmicutes;g__Bacillus",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one fallback success and one preferred-group failure."""

        return DownloadExecutionResult(
            executions={
                "GCF_001881595.2": AccessionExecution(
                    original_accession="GCF_001881595.2",
                    final_accession="GCF_001881595.2",
                    conversion_status="paired_to_gca_fallback_original_on_download_failure",
                    download_status="downloaded_after_fallback",
                    download_batch="direct_fallback_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
                "GCA_001881595.3": AccessionExecution(
                    original_accession="GCA_001881595.3",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    download_batch="direct_batch_1",
                    payload_directory=None,
                    failures=(
                        CommandFailureRecord(
                            stage="preferred_download",
                            attempt_index=4,
                            max_attempts=4,
                            error_type="subprocess",
                            error_message="preferred failed",
                            final_status="retry_exhausted",
                            attempted_accession="GCA_001881595.3",
                        ),
                    ),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "shared-fallback-manifest"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Bacillus",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 6
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_maps = {
        row["gtdb_accession"]: row
        for row in (
            dict(zip(accession_header, values, strict=True))
            for values in accession_rows
        )
    }
    assert accession_maps["RS_GCF_001881595.2"]["download_batch"] == "direct_fallback_batch_1"
    assert accession_maps["GB_GCA_001881595.3"]["download_batch"] == "direct_batch_1"


def test_failure_manifest_collapses_shared_accession_taxa(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """One shared failed accession should yield one root failure row."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia;s__Escherichia coli",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one failed execution for the shared accession."""

        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    download_batch="GCF_000001.1",
                    payload_directory=None,
                    failures=(
                        CommandFailureRecord(
                            stage="preferred_download",
                            attempt_index=4,
                            max_attempts=4,
                            error_type="subprocess",
                            error_message="download failed",
                            final_status="retry_exhausted",
                            attempted_accession="GCF_000001.1",
                        ),
                    ),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "shared-failure"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--gtdb-taxon",
            "s__Escherichia coli",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 7
    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    assert len(failure_rows) == 1
    failure = dict(zip(failure_header, failure_rows[0], strict=True))
    assert failure["requested_taxon"] == "g__Escherichia;s__Escherichia coli"
    assert failure["attempted_accession"] == "GCF_000001.1"


def test_failure_manifest_collapses_shared_metadata_attempts() -> None:
    """Shared metadata retries should be written once per command attempt."""

    enriched_rows = [
        {
            "requested_taxon": "g__Escherichia",
            "taxon_slug": "g__Escherichia",
            "gtdb_accession": "RS_GCF_000001.1",
            "ncbi_accession": "GCF_000001.1",
            "final_accession": "GCF_000001.1",
        },
        {
            "requested_taxon": "s__Escherichia coli",
            "taxon_slug": "s__Escherichia_coli",
            "gtdb_accession": "RS_GCF_000002.1",
            "ncbi_accession": "GCF_000002.1",
            "final_accession": "GCF_000002.1",
        },
    ]
    executions = {
        "GCF_000001.1": AccessionExecution(
            original_accession="GCF_000001.1",
            final_accession="GCF_000001.1",
            conversion_status="unchanged_original",
            download_status="downloaded",
            download_batch="GCF_000001.1",
            payload_directory=None,
            failures=(),
        ),
        "GCF_000002.1": AccessionExecution(
            original_accession="GCF_000002.1",
            final_accession="GCF_000002.1",
            conversion_status="unchanged_original",
            download_status="downloaded",
            download_batch="GCF_000002.1",
            payload_directory=None,
            failures=(),
        ),
    }
    metadata_failures = (
        CommandFailureRecord(
            stage="metadata_lookup",
            attempt_index=1,
            max_attempts=4,
            error_type="metadata_lookup",
            error_message="temporary failure",
            final_status="retry_scheduled",
        ),
        CommandFailureRecord(
            stage="metadata_lookup",
            attempt_index=2,
            max_attempts=4,
            error_type="metadata_lookup",
            error_message="temporary failure",
            final_status="retry_exhausted",
        ),
    )

    failure_rows = build_failure_rows(
        enriched_rows,
        executions,
        metadata_failures,
        (),
        (),
    )

    assert len(failure_rows) == 2
    assert failure_rows[0]["requested_taxon"] == (
        "g__Escherichia;s__Escherichia coli"
    )
    assert failure_rows[0]["attempted_accession"] == (
        "GCF_000001.1;GCF_000002.1"
    )
    assert failure_rows[0]["final_accession"] == (
        "GCF_000001.1;GCF_000002.1"
    )


def test_batch_dehydrate_failure_falls_back_to_direct(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed batch dehydrated download should fall back to direct mode."""

    plans = (
        AccessionPlan(
            original_accession="GCF_000001.1",
            selected_accession="GCA_000001.1",
            download_request_accession="GCA_000001",
            conversion_status="paired_to_gca",
        ),
        AccessionPlan(
            original_accession="GCF_000002.1",
            selected_accession="GCA_000002.1",
            download_request_accession="GCA_000002",
            conversion_status="paired_to_gca",
        ),
    )
    args = CliArgs(
        gtdb_release="95",
        gtdb_taxa=("g__Escherichia",),
        outdir=tmp_path / "output",
        prefer_genbank=True,
        version_fixed=False,
        download_method="dehydrate",
        threads=4,
        ncbi_api_key=None,
        include="genome",
        debug=False,
        keep_temp=False,
        dry_run=False,
    )
    run_directories = initialise_run_directories(tmp_path / "batch-output")

    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_retryable_command",
        lambda *args, **kwargs: RetryableCommandResult(
            succeeded=False,
            stdout="",
            stderr="batch failed",
            failures=(
                CommandFailureRecord(
                    stage="preferred_download",
                    attempt_index=4,
                    max_attempts=4,
                    error_type="subprocess",
                    error_message="batch failed",
                    final_status="retry_exhausted",
                ),
            ),
        ),
    )

    def fake_execute_direct_accession_plans(
        plans: tuple[AccessionPlan, ...],
        args: CliArgs,
        run_directories,
        logger,
    ) -> DownloadExecutionResult:
        """Return a synthetic direct-download fallback result."""

        return DownloadExecutionResult(
            executions={
                plan.original_accession: AccessionExecution(
                    original_accession=plan.original_accession,
                    final_accession=plan.original_accession,
                    conversion_status="paired_to_gca_fallback_original_on_download_failure",
                    download_status="downloaded_after_fallback",
                    download_batch=plan.original_accession,
                    payload_directory=tmp_path,
                    failures=(),
                )
                for plan in plans
            },
            method_used="direct",
            download_concurrency_used=2,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow.execute_direct_accession_plans",
        fake_execute_direct_accession_plans,
    )

    result = execute_batch_dehydrate_plans(
        plans,
        args,
        run_directories,
        logging.getLogger("test"),
        (),
    )

    assert result.method_used == "dehydrate_fallback_direct"
    assert result.download_concurrency_used == 2
    assert result.executions["GCF_000001.1"].failures == ()
    assert result.shared_failures[0].failures[0].attempted_accession == (
        "GCA_000001;GCA_000002"
    )
