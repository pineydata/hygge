---
title: Schema Manifest Refinement
---

### Problem

- `_schema.json` generation lives inside `OpenMirroringStore` and only understands the journal’s current column types. As soon as we reuse it for other datasets (or add columns like decimals/bools) Fabric will misinterpret types or drop nullability.
- The logic is tightly coupled to the store; we can’t share it across other Fabric destinations without copy/paste.

### Desired Behaviour

- Provide a reusable helper that maps Polars dtypes (including decimals, bools, nullable handling) to Fabric-compatible schema entries.
- Update `_write_schema_json` to call the shared helper, and cover edge cases (null columns, mixed types) with unit tests.

### Considerations

- Review existing Open Mirroring unit tests; add coverage for the new helper.
- Document schema manifest expectations so other stores can opt in without re-learning the mapping.
