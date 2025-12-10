# Hygge Progress

## Current Focus

- **Store Interface Standardization complete**: Made store interface explicit by adding default implementations for all optional methods. Removed fragile `hasattr()` checks in favor of direct method calls. Improved type safety and developer experience. All tests passing (740 passed, 13 skipped).
- **Flow refactoring complete**: Extracted flow creation logic from Coordinator to FlowFactory, split Flow into focused package structure (config, factory, flow), and maintained full backward compatibility.
- Journal durability pass complete: synchronized + atomic parquet writes with new concurrency regression tests.
- Incremental plumbing validated end-to-end for MSSQL, including safe watermark column handling.
- PR polish done (flow watermark tracker cleanup, sqlite store temp write, orphaned docstrings, identifier validation).
- Flow `run_type` now directly controls truncate behaviour across Open Mirroring, ADLS, and OneLake; store configs no longer expose a `full_drop` toggle.
- Remote journal storage now auto-resolves to ADLS/OneLake with mirrored-table opt-in for Open Mirroring flows; Fabric directory creation guardrails updated to coexist with Mounted Relational Database policies.
- Mirrored journal sync now rewrites the Fabric table from the canonical `.hygge_journal/journal.parquet` snapshot instead of streaming per-row appends, eliminating schema drift.
- Schema manifest now ships with the mirrored journal and we force publication into `__hygge.schema`, so Fabric column alignment stays predictable and telemetry doesn't mix with business tables.

## Upcoming

- **Watermark Tracker Extraction** (Next Priority): Extract watermark tracking logic from Flow to dedicated Watermark class. Add upfront schema validation for better error experience. Improve testability and maintainability. Better separation of concerns.
- Large Data Volume & Stress Testing: Add stress tests for midmarket scale scenarios (100M+ rows, concurrent flows, connection pool exhaustion).
- Extend incremental watermark support to other homes (Parquet, ADLS, etc.) to make the pattern universal.
- Mirror Journal Batching: Batch journal mirror writes to reduce Fabric churn for flows with multiple entities.
- Schema Manifest Improvements: Extract reusable schema generation logic from OpenMirroringStore for code reuse.
