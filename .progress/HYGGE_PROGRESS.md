# Hygge Progress

## Current Focus

- **Flow refactoring complete**: Extracted flow creation logic from Coordinator to FlowFactory, split Flow into focused package structure (config, factory, flow), and maintained full backward compatibility. All tests passing (611 passed, 9 skipped).
- Journal durability pass complete: synchronized + atomic parquet writes with new concurrency regression tests.
- Incremental plumbing validated end-to-end for MSSQL, including safe watermark column handling.
- PR polish done (flow watermark tracker cleanup, sqlite store temp write, orphaned docstrings, identifier validation).
- Flow `run_type` now directly controls truncate behaviour across Open Mirroring, ADLS, and OneLake; store configs no longer expose a `full_drop` toggle.
- Remote journal storage now auto-resolves to ADLS/OneLake with mirrored-table opt-in for Open Mirroring flows; Fabric directory creation guardrails updated to coexist with Mounted Relational Database policies.
- Mirrored journal sync now rewrites the Fabric table from the canonical `.hygge_journal/journal.parquet` snapshot instead of streaming per-row appends, eliminating schema drift.
- Schema manifest now ships with the mirrored journal and we force publication into `__hygge.schema`, so Fabric column alignment stays predictable and telemetry doesn't mix with business tables.

## Upcoming

- Extend incremental watermark support to other homes (Parquet, ADLS, etc.) to make the pattern universal.
- Watermark Tracker Extraction: Extract watermark tracking logic from Flow to dedicated Watermark class for better separation of concerns.
- Error Handling Standardization: Establish consistent exception hierarchy and handling patterns across codebase.
- Replace sqlite store error-message sniffing with driver-aware error handling.
- Expose journal-backed run/coordinator summaries via CLI output and/or API helpers.
- Centralize mirrored journal handling so flows that share an Open Mirroring store can publish into a single shared telemetry table instead of creating per-flow copies.
