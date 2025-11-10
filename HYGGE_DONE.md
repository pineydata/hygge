# Hygge Done Log

- 2025-11-08: Integrated journal into `Coordinator` (run ID generation, journal instantiation, flow/entity wiring) and added unit tests for journal context + run IDs.

- 2025-11-08: Added watermark-aware incremental reads for `MssqlHome`, updated Flow incremental orchestration, and expanded unit tests for homes & flows.

- 2025-11-10: Hardened journal persistence with synchronized + atomic writes, broadened regression coverage, tightened MSSQL watermark filtering, and rounded out PR polish items.

- 2025-11-10: Simplified Open Mirroring by dropping the store-level `full_drop` flag, wiring flow `run_type` into store configuration hooks, and updating tests/docs to cover the new truncate-vs-append behaviour.

- 2025-11-10: Extended run_type-driven truncate behaviour to ADLS and OneLake stores with per-run configuration hooks, destination truncation, and refreshed unit coverage.

- 2025-11-10: Added remote journal support with automatic placement in `Files/.hygge_journal/journal.parquet`, optional mirrored journal tables for Open Mirroring flows, and new unit coverage for the ADLS-backed journal writer.
