"""CLI entrypoint for bootstrapping GTDB taxonomy payloads."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from gtdb_genomes.release_resolver import (
    get_bundled_data_root,
    get_release_manifest_path,
)
from gtdb_genomes.taxonomy_bundle import TaxonomyBundleError, bootstrap_taxonomy_bundle


def build_parser() -> argparse.ArgumentParser:
    """Build the bootstrap argument parser."""

    parser = argparse.ArgumentParser(
        prog="python -m gtdb_genomes.bootstrap_taxonomy",
        description="Download and materialise bundled GTDB taxonomy payloads.",
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=get_release_manifest_path(),
        help="Path to the GTDB taxonomy manifest. Default: bundled releases.tsv.",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=get_bundled_data_root(),
        help="Directory that receives generated release payloads.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the taxonomy bootstrap entrypoint."""

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        bootstrap_taxonomy_bundle(
            args.manifest_path,
            data_root=args.data_root,
            logger=logging.getLogger("gtdb_genomes.bootstrap_taxonomy"),
        )
    except TaxonomyBundleError as error:
        print(
            f"gtdb-genomes bootstrap-taxonomy: error: {error}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
