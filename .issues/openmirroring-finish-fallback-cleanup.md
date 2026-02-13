# Open Mirroring finish() Fallback Should Fail Fast

**Status:** Ready to fix
**Effort:** 30 minutes
**File:** `src/hygge/stores/openmirroring/store.py` — `finish()` method

## Problem

In `finish()`, the full_drop file-move loop has a second-guess fallback for staging paths that don't contain `_tmp`. The code explicitly says "This should not happen in full_drop mode" — and then builds a fallback path anyway:

```python
if "_tmp" in staging_path_str:
    final_path_str = self._convert_tmp_to_production_path(staging_path_str)
else:
    # Fallback: build final path from staging path
    # This should not happen in full_drop mode (all paths should
    # contain _tmp), but we handle it defensively
    self.logger.warning(
        f"full_drop mode: staging path '{staging_path_str}' "
        "does not contain '_tmp'. This is unexpected and may "
        "indicate a configuration issue. Using fallback path "
        "construction."
    )
    staging_path = Path(staging_path_str)
    final_dir = self.get_final_directory()
    if final_dir:
        final_path = final_dir / staging_path.name
        final_path_str = final_path.as_posix()
    else:
        filename = PathHelper.get_filename(staging_path_str)
        final_path_str = f"{self.base_path}/{filename}"
```

This is a textbook second-guess fallback. If a staging path in full_drop mode doesn't contain `_tmp`, something is genuinely wrong — guessing the path and moving data to a potentially incorrect location is worse than failing.

## Fix

Replace the else branch with a `StoreError`:

```python
if "_tmp" not in staging_path_str:
    raise StoreError(
        f"full_drop mode: staging path '{staging_path_str}' does not "
        f"contain '_tmp'. This indicates a bug or configuration issue. "
        f"All full_drop writes should target _tmp before being moved "
        f"to production."
    )
final_path_str = self._convert_tmp_to_production_path(staging_path_str)
```

This aligns with the fail-fast pattern established in the full_drop simplification PR (where partial folder deletion now raises `StoreError` instead of silently continuing).

## Context

Found during code review of the Open Mirroring full_drop simplification PR. Pre-existing issue. The full_drop PR established the precedent of failing fast on unexpected states — this fallback contradicts that pattern.

A similar concern exists in `_initialize_sequence_counter()` which has an outer `except Exception` that swallows all errors and falls back to `starting_sequence`. That one is more nuanced (first run may legitimately have no directory) but could be tightened to catch specific Azure exceptions rather than bare `Exception`.

## Related

- [openmirroring-refactor.md](openmirroring-refactor.md) — broader `finish()` decomposition
