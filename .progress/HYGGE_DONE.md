# Hygge Done Log

- 2025-12-31: **Mirror Journal Batching complete** - Implemented batched mirrored journal updates to reduce landing zone churn. Modified `MirroredJournalWriter.append()` to set a dirty flag instead of immediately mirroring, added deferred `publish()` method that batches entity run notifications, and publishes once per successful entity completion. Added error handling to preserve dirty flag on failure for retry capability. This reduces transient empty snapshots in Fabric while maintaining timely updates after each successful entity. All tests passing (36 tests in test_journal.py, including new batching tests).

- 2024-11-23: **Flow refactoring complete** - Extracted flow creation logic from Coordinator to FlowFactory, split Flow into focused package structure (`flow/config.py`, `flow/factory.py`, `flow/flow.py`), added safe config access methods (`get_store_config()`, `get_home_config()`), and maintained full backward compatibility. All tests passing (611 passed, 9 skipped). Clean separation achieved: FlowFactory handles construction, Flow handles execution.

- 2025-11-08: Integrated journal into `Coordinator` (run ID generation, journal instantiation, flow/entity wiring) and added unit tests for journal context + run IDs.

- 2025-11-08: Added watermark-aware incremental reads for `MssqlHome`, updated Flow incremental orchestration, and expanded unit tests for homes & flows.

- 2025-11-10: Hardened journal persistence with synchronized + atomic writes, broadened regression coverage, tightened MSSQL watermark filtering, and rounded out PR polish items.

- 2025-11-10: Simplified Open Mirroring by dropping the store-level `full_drop` flag, wiring flow `run_type` into store configuration hooks, and updating tests/docs to cover the new truncate-vs-append behaviour.

- 2025-11-10: Extended run_type-driven truncate behaviour to ADLS and OneLake stores with per-run configuration hooks, destination truncation, and refreshed unit coverage.

- 2025-11-10: Added remote journal support with automatic placement in `Files/.hygge_journal/journal.parquet`, optional mirrored journal tables for Open Mirroring flows, and new unit coverage for the ADLS-backed journal writer.

- 2025-11-11: Taught the ADLS directory creator to skip Fabric mounted database roots so journal writes no longer violate the `<Tables, Files>` policy when mirroring to OneLake/Open Mirroring stores.

- 2025-11-11: Reworked the mirrored journal writer to reload the canonical `.hygge_journal/journal.parquet` snapshot (full-drop rewrite) so Fabric tables stay in sync without per-row schema drift.

- 2025-11-11: Forced mirrored journal telemetry into `__hygge.schema` and emitted `_schema.json` alongside `_metadata.json` so Fabric preserves column order/types during every snapshot publish.

- 2025-12-17: **Schema Manifest Improvements** - Extracted a reusable Fabric schema helper (`hygge.utility.fabric_schema`) that maps Polars dtypes (including decimals and booleans) into the lightweight `_schema.json` manifest format, added explicit nullable handling for null/mixed columns, and wired Open Mirroring's mirrored journal `_schema.json` generation through the shared helper. Extended unit coverage to exercise the helper directly and to assert journal schema manifests remain correct, with no breaking changes to existing journal behaviour.

- 2025-12-10: **Watermark Tracker Extraction complete** (PR #49) - Extracted watermark tracking logic from Flow to dedicated `Watermark` class in `src/hygge/core/watermark.py`. Added upfront schema validation for better error experience, improved testability and maintainability. Better separation of concerns achieved. Flow now uses `Watermark` class for all watermark operations.

- 2025-12-XX: **Stress Testing: Parquet-to-Parquet at Scale complete** (PR #50) - Added comprehensive stress test suite for parquet-to-parquet data movement at midmarket scale (10M-100M rows). Validated framework reliability with concurrent flows (10+ flows simultaneously) and memory efficiency. MSSQL-specific stress tests deferred to future review cycle (documented in `.issues/mssql-stress-testing.md`).

- 2025-12-XX: **Test Coverage Improvements complete** (PR #46) - Extended test coverage with visibility and gap analysis, improving overall test quality and maintainability.

- 2025-01-XX: **Store Interface Standardization complete** (PR #48) - Made store interface explicit by adding default implementations for all optional methods (`configure_for_run`, `cleanup_staging`, `reset_retry_sensitive_state`, `set_pool`). Removed fragile `hasattr()` checks in favor of direct method calls with safe defaults. Improved type safety and developer experience for store implementers. All existing stores remain fully compatible with no breaking changes. All tests passing (740 passed, 13 skipped).

- 2025-XX-XX: **Entity Lifecycle Refactoring complete** (PR #42) - Complete architecture overhaul for entity configuration lifecycle, improving separation of concerns and maintainability.

- 2025-XX-XX: **Enhanced Exception Hierarchy complete** (PR #41) - Improved exception handling across the framework with clearer error messages and better error recovery.

- 2025-XX-XX: **Flow Architecture Refactoring complete** (PR #40) - Extracted flow creation logic from Coordinator to FlowFactory, split Flow into focused package structure, maintained full backward compatibility.
