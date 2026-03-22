"""Contract-level edge-case tests for payload extraction."""

from __future__ import annotations

from pathlib import Path

import pytest

from gtdb_genomes.layout import LayoutError, initialise_run_directories
from gtdb_genomes.workflow_execution import ResolvedPayloadDirectory, extract_download_payload


def test_extract_download_payload_reports_layout_stage_for_archive_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Archive extraction failures should be labelled as layout failures."""

    run_directories = initialise_run_directories(tmp_path / "layout-error")
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_payloads.extract_archive",
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
        "gtdb_genomes.workflow_execution_payloads.extract_archive",
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
        "gtdb_genomes.workflow_execution_payloads.extract_archive",
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
        "gtdb_genomes.workflow_execution_payloads.extract_archive",
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
        "gtdb_genomes.workflow_execution_payloads.extract_archive",
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


def test_extract_download_payload_rejects_ambiguous_exact_payload_directories(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Duplicate exact payload directories should fail as an ambiguous layout."""

    run_directories = initialise_run_directories(
        tmp_path / "layout-ambiguous-exact-payload",
    )

    def fake_extract_archive(archive_path: Path, extraction_root: Path) -> Path:
        """Create two top-level payload directories for one exact accession."""

        del archive_path
        first_payload = extraction_root / "batch_a" / "GCA_000001.7"
        second_payload = extraction_root / "batch_b" / "GCA_000001.7"
        first_payload.mkdir(parents=True, exist_ok=True)
        second_payload.mkdir(parents=True, exist_ok=True)
        return extraction_root

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_payloads.extract_archive",
        fake_extract_archive,
    )

    payload_directory, failures = extract_download_payload(
        "GCA_000001.7",
        tmp_path / "archive.zip",
        run_directories,
    )

    assert payload_directory is None
    assert len(failures) == 1
    assert failures[0].stage == "layout"
    assert "Resolved multiple extracted payload directories" in (
        failures[0].error_message
    )


def test_extract_download_payload_rejects_duplicate_exact_accessions_across_layouts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A data-root payload should not mask a conflicting exact fallback payload."""

    run_directories = initialise_run_directories(
        tmp_path / "layout-ambiguous-across-roots",
    )

    def fake_extract_archive(archive_path: Path, extraction_root: Path) -> Path:
        """Create the same exact accession under the data root and elsewhere."""

        del archive_path
        first_payload = extraction_root / "ncbi_dataset" / "data" / "GCA_000001.7"
        second_payload = extraction_root / "relocated" / "GCA_000001.7"
        first_payload.mkdir(parents=True, exist_ok=True)
        second_payload.mkdir(parents=True, exist_ok=True)
        return extraction_root

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_payloads.extract_archive",
        fake_extract_archive,
    )

    payload_directory, failures = extract_download_payload(
        "GCA_000001.7",
        tmp_path / "archive.zip",
        run_directories,
    )

    assert payload_directory is None
    assert len(failures) == 1
    assert failures[0].stage == "layout"
    assert "Resolved multiple extracted payload directories" in (
        failures[0].error_message
    )
