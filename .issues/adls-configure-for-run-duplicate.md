# ADLSStore.configure_for_run() Duplicate Code Block

**Status:** Ready to fix
**Effort:** 15 minutes
**File:** `src/hygge/stores/adls/store.py` lines 249–269

## Problem

`ADLSStore.configure_for_run()` has the entire reset-and-log block duplicated. Lines 249–258 and 260–269 are copy-pasted with a slightly different log message. This means:

- `sequence_counter`, `saved_paths`, and `uploaded_files` are reset **twice**
- The full_drop debug message is logged **twice** (with slightly different wording)

```python
def configure_for_run(self, run_type: str) -> None:
    """Allow flows to toggle truncate behaviour via run type."""
    super().configure_for_run(run_type)

    is_incremental = run_type != "full_drop"
    if self.incremental_override is not None:
        is_incremental = self.incremental_override

    self.full_drop_mode = not is_incremental

    # Reset per-run tracking so we don't carry state across executions
    self.sequence_counter = 0
    self.saved_paths = []
    self.uploaded_files = []

    if self.full_drop_mode:
        self.logger.debug(
            "Run configured for full_drop: will truncate destination "
            "before publishing new files."
        )

    # Reset per-run tracking so we don't carry state across executions  <-- DUPLICATE
    self.sequence_counter = 0
    self.saved_paths = []
    self.uploaded_files = []

    if self.full_drop_mode:
        self.logger.debug(
            "Run configured for full_drop: will truncate destination directory "
            "before publishing new files."
        )
```

## Fix

Delete lines 260–269 (the second block). Keep the first block. This is a pure cleanup — no behavior change (the double reset is harmless but confusing).

## Context

Found during code review of the Open Mirroring full_drop simplification PR. Pre-existing issue, not introduced by that PR. `OpenMirroringStore` inherits from `OneLakeStore` which inherits from `ADLSStore`, so this `configure_for_run()` runs as part of the `super()` chain.

## Related

- [openmirroring-refactor.md](openmirroring-refactor.md) — broader Open Mirroring store cleanup
