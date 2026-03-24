# Contributing

Thank you for contributing to `gtdb-genomes`.

## Local Setup

Use the source-checkout workflow:

```bash
uv sync --group dev
uv run python -m gtdb_genomes.bootstrap_taxonomy
uv run gtdb-genomes --help
```

A Git checkout tracks only `data/gtdb_taxonomy/releases.tsv`. The bootstrap
step materialises the local `data/gtdb_taxonomy/<release>/*.tsv.gz` runtime
layout used by source-checkout runs and source builds.

## Before Opening A Change

Useful local checks:

```bash
uv run pytest -q
uv build
```

The detailed runtime contract, packaged-runtime boundary, and bundled-taxonomy
notes live in [docs/usage-details.md](docs/usage-details.md).

For Bioconda recipe-template specifics, see
[packaging/bioconda/README.md](packaging/bioconda/README.md).

Community packaging should use the tagged release `sdist`, not a repository snapshot.
