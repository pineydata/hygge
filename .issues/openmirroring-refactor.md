# Open Mirroring Store Refactoring

**Status:** Low Priority
**File:** `src/hygge/stores/openmirroring/store.py` (1359 lines)

## Context

The Open Mirroring store implements Microsoft's Open Mirroring specification for Fabric. The spec has genuine complexity (row markers, metadata files, atomic operations, sequence counters). However, the current implementation has structural issues that make it harder to maintain.

## Issues

### 1. Duplicate JSON Writes Bypass ADLSOperations (~230 lines)

Three methods with near-identical structure:
- `_write_metadata_json` (111 lines)
- `_write_partner_events_json` (74 lines)
- `_write_schema_json` (44 lines)

All follow the same pattern:
1. Check if file exists (`adls_ops.read_json()` - used correctly)
2. Validate consistency (if exists)
3. Ensure directory exists (duplicates `create_directory_recursive`)
4. Get file_client directly and call `upload_data()` (bypasses `write_json()`)
5. Track tmp path for atomic operation

`ADLSOperations.write_json()` exists but only supports `overwrite=False`. The store needs `overwrite=True`, so it bypasses the utility entirely.

**Fix:** Add `overwrite` parameter to `ADLSOperations.write_json()`. Then these three methods collapse to ~30 lines each: build payload, call utility, track tmp path.

### 2. Monolithic `finish()` Method (172 lines)

The `finish()` method handles 7+ distinct operations:
1. Flush buffer
2. Log remaining rows
3. Delete production folder
4. Move data files from `_tmp` to production
5. Move `_metadata.json`
6. Move `_partnerEvents.json`
7. Move `_schema.json`
8. Aggregate and report errors
9. Log completion stats

Each is a separate concern. Should be decomposed.

### 3. Scattered Atomic Operation Logic

The "write to `_tmp`, then move to production" pattern appears in multiple places:
- Data files in `_save()`
- Metadata files in `_write_metadata_json()`
- Partner events in `_write_partner_events_json()`
- Schema in `_write_schema_json()`
- Move operations in `finish()`

The path conversion (`_convert_tmp_to_production_path`) is a symptom of this scattered logic.

## Proposed Improvements

### Enhance ADLSOperations.write_json()

```python
async def write_json(
    self,
    path: str,
    data: Dict,
    overwrite: bool = False,
    ensure_directory: bool = True,
) -> None:
    """Write JSON to lake storage with retries."""
    # Consolidate all JSON writing logic here
```

Then the store methods become thin wrappers that just build the payload and call `adls_ops.write_json()`.

### Decompose `finish()`

```python
async def finish(self) -> None:
    await self._flush_remaining_buffer()

    if self.full_drop_mode:
        await self._execute_atomic_swap()
    else:
        await super().finish()

async def _execute_atomic_swap(self) -> None:
    """ACID atomic operation: delete production, move from _tmp."""
    await self._delete_table_folder()
    errors = await self._move_staged_files_to_production()
    if errors:
        raise StoreError(self._format_move_errors(errors))
```

### Adopt StagedWriter Abstraction

See [staged-writer-abstraction.md](staged-writer-abstraction.md) for the core pattern.

Once implemented, OpenMirroringStore would adopt the mixin, removing ~100 lines of atomic operation logic from `finish()` and enabling proper rollback on failure.

## What's NOT a Problem

- **Config class size (~230 lines):** Open Mirroring has many options - this is appropriate
- **Validation methods:** Spec-mandated requirements, properly isolated
- **Sequence counter logic:** Needs to scan existing files, inherently complex

## Effort Estimate

- **ADLSOperations.write_json() enhancement:** 1 hour
- **Store JSON methods refactor:** 2 hours
- **`finish()` decomposition:** 2-3 hours
- **Adopt StagedWriter (after [staged-writer-abstraction.md](staged-writer-abstraction.md)):** 2-3 hours
- **Total:** ~1 day (excluding StagedWriter abstraction)

## Priority

Low. The code works correctly. This is maintenance/readability improvement, not a bug fix. Address when touching this file for other reasons.

## Related Issues

- [staged-writer-abstraction.md](staged-writer-abstraction.md) - Core abstraction this refactor would adopt
