"""Custom Hatch build hook for bundled GTDB payload verification."""

from __future__ import annotations

import base64
import csv
import io
import hashlib
from pathlib import Path
import sys
import tarfile
import zipfile

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hatch_metadata import get_external_runtime_requirements

SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from gtdb_genomes.provenance import (
    get_git_revision,
    read_pyproject_version,
    write_build_info,
)
from gtdb_genomes.release_resolver import (
    BundledDataError,
    build_release_resolution,
    get_release_manifest_path,
    load_release_manifest,
    validate_release_payload,
)


REQUIRES_EXTERNAL_PREFIX = "Requires-External: "


def append_requires_external_metadata(metadata_text: str) -> str:
    """Append the external-runtime requirements to one metadata payload."""

    existing_requirements = {
        line.removeprefix(REQUIRES_EXTERNAL_PREFIX)
        for line in metadata_text.splitlines()
        if line.startswith(REQUIRES_EXTERNAL_PREFIX)
    }
    appended_lines = [
        f"{REQUIRES_EXTERNAL_PREFIX}{requirement}"
        for requirement in get_external_runtime_requirements()
        if requirement not in existing_requirements
    ]
    if not appended_lines:
        return metadata_text
    metadata_body = metadata_text.rstrip("\n")
    return "\n".join([metadata_body, *appended_lines, ""])


def build_wheel_record_hash(payload_bytes: bytes) -> str:
    """Return the wheel `RECORD` hash field for one payload."""

    digest = hashlib.sha256(payload_bytes).digest()
    encoded_digest = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"sha256={encoded_digest}"


def build_wheel_record_text(
    member_payloads: tuple[tuple[str, bytes], ...],
    *,
    record_member_name: str,
) -> str:
    """Return one regenerated wheel `RECORD` payload."""

    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for member_name, payload_bytes in member_payloads:
        writer.writerow(
            (
                member_name,
                build_wheel_record_hash(payload_bytes),
                str(len(payload_bytes)),
            ),
        )
    writer.writerow((record_member_name, "", ""))
    return buffer.getvalue()


def build_copied_tar_info(member: tarfile.TarInfo) -> tarfile.TarInfo:
    """Return a writable copy of one tar member descriptor."""

    copied_member = tarfile.TarInfo(member.name)
    copied_member.mode = member.mode
    copied_member.uid = member.uid
    copied_member.gid = member.gid
    copied_member.size = member.size
    copied_member.mtime = member.mtime
    copied_member.type = member.type
    copied_member.linkname = member.linkname
    copied_member.uname = member.uname
    copied_member.gname = member.gname
    copied_member.devmajor = member.devmajor
    copied_member.devminor = member.devminor
    copied_member.pax_headers = dict(member.pax_headers)
    return copied_member


def patch_wheel_metadata(artifact_path: Path) -> None:
    """Inject `Requires-External` headers into one built wheel."""

    temporary_path = artifact_path.with_suffix(".tmp.whl")
    metadata_patched = False
    record_member: zipfile.ZipInfo | None = None
    rewritten_members: list[tuple[zipfile.ZipInfo, bytes | None]] = []
    with zipfile.ZipFile(artifact_path) as source_archive:
        for member in source_archive.infolist():
            if member.filename.endswith(".dist-info/RECORD"):
                record_member = member
                continue
            if member.is_dir():
                rewritten_members.append((member, None))
                continue
            payload_bytes = source_archive.read(member.filename)
            if member.filename.endswith(".dist-info/METADATA"):
                payload_bytes = append_requires_external_metadata(
                    payload_bytes.decode("utf-8"),
                ).encode("utf-8")
                metadata_patched = True
            rewritten_members.append((member, payload_bytes))
    if not metadata_patched:
        temporary_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"Built wheel is missing a dist-info METADATA file: {artifact_path}",
        )
    if record_member is None:
        temporary_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"Built wheel is missing a dist-info RECORD file: {artifact_path}",
        )
    record_text = build_wheel_record_text(
        tuple(
            (member.filename, payload_bytes)
            for member, payload_bytes in rewritten_members
            if payload_bytes is not None
        ),
        record_member_name=record_member.filename,
    )
    with zipfile.ZipFile(temporary_path, "w") as rewritten_archive:
        for member, payload_bytes in rewritten_members:
            if payload_bytes is None:
                rewritten_archive.writestr(member, b"")
                continue
            rewritten_archive.writestr(member, payload_bytes)
        rewritten_archive.writestr(record_member, record_text.encode("utf-8"))
    temporary_path.replace(artifact_path)


def patch_sdist_metadata(artifact_path: Path) -> None:
    """Inject `Requires-External` headers into one built sdist."""

    temporary_path = artifact_path.with_suffix(".tmp.tar.gz")
    metadata_patched = False
    with tarfile.open(artifact_path, "r:gz") as source_archive:
        with tarfile.open(temporary_path, "w:gz") as rewritten_archive:
            for member in source_archive.getmembers():
                copied_member = build_copied_tar_info(member)
                if member.isfile():
                    extracted_file = source_archive.extractfile(member)
                    if extracted_file is None:
                        raise RuntimeError(
                            f"Could not read sdist member {member.name}: {artifact_path}",
                        )
                    payload_bytes = extracted_file.read()
                    if member.name.endswith("/PKG-INFO") or member.name == "PKG-INFO":
                        payload_bytes = append_requires_external_metadata(
                            payload_bytes.decode("utf-8"),
                        ).encode("utf-8")
                        copied_member.size = len(payload_bytes)
                        metadata_patched = True
                    rewritten_archive.addfile(copied_member, io.BytesIO(payload_bytes))
                    continue
                rewritten_archive.addfile(copied_member)
    if not metadata_patched:
        temporary_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"Built sdist is missing a PKG-INFO file: {artifact_path}",
        )
    temporary_path.replace(artifact_path)


def patch_artifact_runtime_metadata(artifact_path: Path) -> None:
    """Inject external-runtime requirements into one built distribution."""

    if artifact_path.suffix == ".whl":
        patch_wheel_metadata(artifact_path)
        return
    if "".join(artifact_path.suffixes[-2:]) == ".tar.gz":
        patch_sdist_metadata(artifact_path)


class CustomBuildHook(BuildHookInterface):
    """Validate bundled taxonomy data before building any artefact."""

    def initialise_build_info(
        self,
        *,
        build_data: dict[str, object],
    ) -> None:
        """Generate packaged build metadata and stage it for the artefact."""

        build_directory = Path(self.directory)
        package_version = read_pyproject_version(PROJECT_ROOT)
        build_info_path = build_directory / "generated" / "gtdb_genomes" / "_build_info.json"
        write_build_info(
            build_info_path,
            package_version_value=package_version,
            git_revision=get_git_revision(),
        )
        force_include = build_data.setdefault("force_include", {})
        if not isinstance(force_include, dict):
            raise RuntimeError(
                "Build hook expected build_data['force_include'] to be a dict, "
                f"got {type(force_include).__name__}",
            )
        force_include[str(build_info_path)] = "gtdb_genomes/_build_info.json"

    def validate_bundled_taxonomy(self) -> None:
        """Validate every manifest-configured bundled release before build."""

        manifest_path = get_release_manifest_path()
        entries = load_release_manifest(manifest_path)
        if not entries:
            raise RuntimeError(
                f"Bundled release manifest is empty: {manifest_path}",
            )
        data_root = manifest_path.parent
        for entry in entries:
            validate_release_payload(
                build_release_resolution(
                    entry,
                    requested_release=entry.resolved_release,
                    data_root=data_root,
                    manifest_path=manifest_path,
                ),
            )

    def initialize(
        self,
        version: str,
        build_data: dict[str, object],
    ) -> None:
        """Reject builds that do not contain the validated bundled payload."""

        if version == "editable":
            return
        try:
            self.validate_bundled_taxonomy()
        except BundledDataError as error:
            raise RuntimeError(
                "Bundled GTDB taxonomy payload is not ready for packaging. "
                f"{error}",
            ) from error
        self.initialise_build_info(build_data=build_data)

    def finalize(
        self,
        version: str,
        build_data: dict[str, object],
        artifact_path: str,
    ) -> None:
        """Patch built artefacts with the external runtime metadata headers."""

        del build_data
        if version == "editable":
            return
        patch_artifact_runtime_metadata(Path(artifact_path))
