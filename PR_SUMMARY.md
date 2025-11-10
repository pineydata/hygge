## Overview
- Extend hygge’s Azure lineage so flow `run_type` drives truncate-versus-append behavior consistently across ADLS, OneLake, and Open Mirroring stores; the legacy `full_drop` switches disappear in favor of per-run configuration hooks.
- Give the execution journal a remote home: lake-based flows now keep their parquet run log beside their data (`Files/.hygge_journal/journal.parquet`), while filesystem flows continue writing locally.
- Offer optional mirrored journal telemetry for Open Mirroring by streaming journal rows into a dedicated Fabric table; documentation and progress logs record the milestone.

## Key Changes
### Store & Flow Runtime Wiring
- `src/hygge/core/store.py`: introduces a `configure_for_run` hook so stores can reset staging state or truncate destinations when the flow requests a full drop.
- `src/hygge/core/flow.py`: calls the new hook during construction, ensuring each store reacts to the upcoming run type before data moves.

### Azure Store Enhancements
- `src/hygge/stores/adls/store.py`, `src/hygge/stores/onelake/store.py`, `src/hygge/stores/openmirroring/store.py`: track `full_drop_mode`, reset counters, and truncate destinations only for full-drop runs; Open Mirroring config now exposes `mirror_journal` / `journal_table_name`.
- `src/hygge/utility/azure_onelake.py`: adds byte-level reads to support remote journal operations.

### Journal & Coordinator Updates
- `src/hygge/core/journal.py`: refactored to support both local and ADLS-backed storage, handle atomic remote writes, create mirrored sinks when requested, and expose a shared read path for aggregations.
- `src/hygge/core/coordinator.py`: passes store instances/configs into the journal factory so it can detect remote scenarios; simplification of `full_drop` propagation.

### Tests & Docs
- `tests/unit/hygge/core/test_journal.py`: new ADLS stub verifies remote journal paths and append logic.
- `tests/unit/hygge/stores/*`: cover the per-run configuration hooks and full-drop handling across ADLS, OneLake, and Open Mirroring.
- `HYGGE_PROGRESS.md`, `HYGGE_DONE.md`: document the remote journal milestone.

## Testing
- `pytest`
