"""CLI entrypoint for refreshing GTDB taxonomy source metadata."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from gtdb_genomes.release_resolver import get_release_manifest_path
from gtdb_genomes.taxonomy_bundle import (
    TaxonomyBundleError,
    UQ_RELEASES_ROOT,
    refresh_taxonomy_bundle_manifest,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the refresh argument parser."""

    parser = argparse.ArgumentParser(
        prog="python -m gtdb_genomes.refresh_taxonomy_manifest",
        description="Refresh GTDB taxonomy source metadata from the UQ mirror.",
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=get_release_manifest_path(),
        help="Path to the GTDB taxonomy manifest. Default: bundled releases.tsv.",
    )
    parser.add_argument(
        "--releases-root-url",
        default=UQ_RELEASES_ROOT,
        help="Base directory URL for the GTDB release mirror.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the manifest refresh entrypoint."""

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        refresh_taxonomy_bundle_manifest(
            args.manifest_path,
            releases_root_url=args.releases_root_url,
            logger=logging.getLogger("gtdb_genomes.refresh_taxonomy_manifest"),
        )
    except TaxonomyBundleError as error:
        print(
            f"gtdb-genomes refresh-taxonomy-manifest: error: {error}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
