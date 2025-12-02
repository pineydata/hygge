# Hygge Done Log

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
