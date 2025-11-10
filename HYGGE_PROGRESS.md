# Hygge Progress

## Current Focus

- Journal durability pass complete: synchronized + atomic parquet writes with new concurrency regression tests.
- Incremental plumbing validated end-to-end for MSSQL, including safe watermark column handling.
- PR polish done (flow watermark tracker cleanup, sqlite store temp write, orphaned docstrings, identifier validation).
- Flow `run_type` now directly controls truncate behaviour across Open Mirroring, ADLS, and OneLake; store configs no longer expose a `full_drop` toggle.
- Remote journal storage now auto-resolves to ADLS/OneLake with mirrored-table opt-in for Open Mirroring flows.

## Upcoming

- Extend incremental watermark support to other homes (Parquet, ADLS, etc.) to make the pattern universal.
- Replace sqlite store error-message sniffing with driver-aware error handling.
- Expose journal-backed run/coordinator summaries via CLI output and/or API helpers.
