# Datasets floor validation

## Goal

Find the lowest `ncbi-datasets-cli` version that still works with the current
workflow on real dry-runs, especially the large metadata path exercised by
`g__Bacteroides --prefer-genbank`.

## Passing versions

- `18.21.0`: passed small and large real dry-runs
- `18.9.0`: passed small and large real dry-runs
- `18.4.1`: passed the large `g__Bacteroides` dry-run
- `18.4.0`: passed the large `g__Bacteroides` dry-run

## Failing versions

- `18.3.1`
- `18.2.3`
- `18.0.1`
- `17.3.0`

All of these failed the large `g__Bacteroides --prefer-genbank --dry-run`
probe with the same upstream error:

```text
Error: [gateway] synonym is not a valid V2reportsANITypeCategory
```

## Decision

The supported `datasets` floor should be `18.4.0`.

The supported `datasets` ceiling stays at `<18.22.0` because no newer local
Conda-packaged version than `18.21.0` was available to validate.

`unzip` stays at `>=6.0,<7.0` because only `6.0` was locally available to
validate.
