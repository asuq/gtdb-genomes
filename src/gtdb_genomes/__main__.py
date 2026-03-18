"""Module entrypoint for `python -m gtdb_genomes`."""

from __future__ import annotations

import sys

from gtdb_genomes.cli import main


if __name__ == "__main__":
    sys.exit(main())
