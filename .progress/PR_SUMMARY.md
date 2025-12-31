---
title: PR Summary - Fabric Schema Helper & Open Mirroring Manifest
tags: [enhancement, testing]
---

## Overview

- Introduced a shared Fabric schema helper that maps Polars dtypes (including decimals and booleans) into lightweight `_schema.json` entries, centralizing manifest logic.
- Updated Open Mirroring's mirrored journal `_schema.json` generation to use the shared helper and explicitly mark columns nullable, reducing Fabric schema drift and making behaviour predictable.
- Added focused unit coverage around the new helper and Open Mirroring schema manifests, and updated progress docs to record the Schema Manifest Improvements work.

## Key Changes

### Fabric Schema Helper

- `src/hygge/utility/fabric_schema.py`:
  - Added `map_polars_dtype_to_fabric` and `build_fabric_schema_columns` to translate Polars dtypes into Fabric-compatible logical types.
  - Extended mapping beyond the original journal use case to cover decimals, booleans, null/mixed columns, and to attach optional precision/scale metadata for decimal types.
  - Always marks columns as `nullable=True` in manifest output to avoid brittle non-null declarations that conflict with real-world NULLs.

### Open Mirroring Store

- `src/hygge/stores/openmirroring/store.py`:
  - Reworked `_write_schema_json` to call `build_fabric_schema_columns(Journal.JOURNAL_SCHEMA)`, so mirrored journal manifests share the same Polars → Fabric mapping used elsewhere.
  - Kept `_map_polars_dtype_to_fabric` as a backwards-compatible wrapper that now delegates to the shared helper, preserving the store API while consolidating mapping behaviour.
  - Ensured mirrored journal `_schema.json` now includes explicit `nullable` flags (and richer type information for decimals/booleans), reducing the chance Fabric misinterprets types or drops nullability when ingesting snapshots.

### Tests

- `tests/unit/hygge/utility/test_fabric_schema.py`:
  - New unit suite covering the helper's dtype mapping for strings, integers, floats, temporal types, decimals, booleans, and edge cases like `pl.Null` / `pl.Object`.
  - Verifies that schema column descriptors are correctly shaped and always marked nullable, and that decimals surface precision/scale when available.

- `tests/unit/hygge/stores/test_openmirroring_store.py`:
  - Extended schema manifest tests to assert mirrored journal manifests still include all `Journal.JOURNAL_SCHEMA` columns with expected logical types.
  - Added checks that every manifest column is marked `nullable=True`, matching the conservative, comfort-first behaviour of the helper.

- `tests/unit/hygge/stores/test_openmirroring_store_coverage.py`:
  - Updated coverage test for `_map_polars_dtype_to_fabric` to expect booleans to map to `"boolean"` rather than falling back to `"string"`, aligning coverage with the shared helper.

### Progress Documentation

- `.progress/HYGGE_PROGRESS.md`:
  - Marked Schema Manifest Improvements as complete and documented the introduction of the shared Fabric schema helper and reuse from Open Mirroring.

- `.progress/HYGGE_DONE.md`:
  - Added an entry describing the schema manifest work, including the new helper, expanded type support, and wiring through the Open Mirroring journal mirror.

## Testing

- Last local run: `pytest` (790 tests collected; initial failure in an Open Mirroring coverage test due to the updated boolean mapping has been resolved by aligning the expectation with the new helper behaviour).
- Please re-run `pytest` on this branch to confirm all tests are now green before merging the PR.

---

**Note**: Remember to add appropriate GitHub labels to this PR (e.g., `enhancement` and `testing`) for proper categorization in release notes.
