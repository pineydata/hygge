# Hygge Progress

## Current Focus

- **Mirror Journal Batching complete** (2025-12-31): Implemented batched mirrored journal updates to reduce landing zone churn. Modified `MirroredJournalWriter.append()` to set a dirty flag instead of immediately mirroring, added deferred `publish()` method that batches entity run notifications, and publishes once per successful entity completion. Added error handling to preserve dirty flag on failure for retry capability. This reduces transient empty snapshots in Fabric while maintaining timely updates after each successful entity.

- **Watermark Tracker Extraction complete** (PR #49): Extracted watermark tracking logic from Flow to dedicated `Watermark` class in `src/hygge/core/watermark.py`. Added upfront schema validation, improved testability and maintainability. Better separation of concerns achieved.
- **Store Interface Standardization complete** (PR #48): Made store interface explicit by adding default implementations for all optional methods. Removed fragile `hasattr()` checks in favor of direct method calls. Improved type safety and developer experience.
- **Stress Testing complete** (PR #50): Added comprehensive stress test suite for parquet-to-parquet data movement at midmarket scale (10M-100M rows). Validated framework reliability with concurrent flows (10+ flows simultaneously) and memory efficiency. MSSQL-specific stress tests deferred to future review cycle.
- **Test Coverage Improvements complete** (PR #46): Extended test coverage with visibility and gap analysis.
- **Entity Lifecycle Refactoring complete** (PR #42): Complete architecture overhaul for entity configuration lifecycle.
- **Enhanced Exception Hierarchy complete** (PR #41): Improved exception handling across the framework.
- **Flow Architecture Refactoring complete** (PR #40): Extracted flow creation logic from Coordinator to FlowFactory, split Flow into focused package structure (config, factory, flow), and maintained full backward compatibility.
- Journal durability pass complete: synchronized + atomic parquet writes with new concurrency regression tests.
- Incremental plumbing validated end-to-end for MSSQL, including safe watermark column handling.
- Flow `run_type` now directly controls truncate behaviour across Open Mirroring, ADLS, and OneLake; store configs no longer expose a `full_drop` toggle.
- Remote journal storage now auto-resolves to ADLS/OneLake with mirrored-table opt-in for Open Mirroring flows; Fabric directory creation guardrails updated to coexist with Mounted Relational Database policies.
- Mirrored journal sync now rewrites the Fabric table from the canonical `.hygge_journal/journal.parquet` snapshot instead of streaming per-row appends, eliminating schema drift.
- Schema manifest now ships with the mirrored journal and we force publication into `__hygge.schema`, so Fabric column alignment stays predictable and telemetry doesn't mix with business tables.
- **Schema Manifest Improvements complete**: Extracted a reusable Polars → Fabric schema helper (`hygge.utility.fabric_schema`), extended type coverage (decimals, booleans, null/mixed columns), and wired Open Mirroring's journal `_schema.json` generation through the shared helper for future reuse.
- **Large Data Volume & Stress Testing complete**: Added comprehensive stress test suite for parquet-to-parquet data movement at midmarket scale (10M-100M rows). Validated framework reliability with concurrent flows (10+ flows simultaneously) and memory efficiency. MSSQL-specific stress tests deferred to future review cycle.

## Upcoming

- **Schema Manifest Improvements** (In Progress/Under Review): Extract reusable schema generation logic from OpenMirroringStore for code reuse. PR created with shared `fabric_schema.py` helper that maps Polars dtypes to Fabric-compatible schema entries. Currently in `feature/schema-manifest` branch, awaiting merge to main.
- Extend incremental watermark support to other homes (Parquet, ADLS, etc.) to make the pattern universal.
