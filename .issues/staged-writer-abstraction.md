# Staged Writer Abstraction for Stores

**Status:** Foundational – Blocks GCS Store
**Scope:** Core Store architecture

## Context

The "write to staging, swap on commit" pattern is fundamental for data reliability. It applies to ALL stores:
- **ParquetStore:** temp files on disk → atomic rename
- **ADLSStore/OneLakeStore/OpenMirroringStore:** `_tmp` directory → move
- **Future S3Store:** staging prefix → copy+delete

**This pattern is already duplicated across stores:**

| Store | `saved_paths` | `get_staging_directory()` | `_move_to_final()` | `cleanup_staging()` | `full_drop` logic |
|-------|--------------|---------------------------|-------------------|--------------------|--------------------|
| ParquetStore | ✓ line 69 | ✓ lines 87-98 | ✓ lines 181-206 | ✓ lines 208-218 | ✗ |
| ADLSStore | ✓ line 233 | ✓ lines 417-424 | ✓ lines 558-603 | ✓ lines 509-536 | ✓ lines 605-658 |
| OpenMirroringStore | ✓ (inherited) | ✓ (overridden) | ✓ (overridden) | ✓ (inherited) | ✓ lines 997-1051 |

Each implements the same pattern with slight variations. This is classic DRY violation.

## Pattern

1. Write all files to staging location
2. Track staged paths
3. On `finish()`: optionally delete production → move staged files → cleanup
4. On failure: rollback (clean up staged files)

## Proposed: Store-level Mixin

```python
class StagedWriteMixin:
    """Mixin for stores that need atomic staging behavior."""

    _staged_paths: list[str]

    def _get_staging_path(self, relative_path: str) -> str:
        """Override per store: how to build staging path."""
        raise NotImplementedError

    def _get_production_path(self, staging_path: str) -> str:
        """Override per store: convert staging → production path."""
        raise NotImplementedError

    async def _move_staged_to_production(self, staging: str, production: str) -> None:
        """Override per store: how to move files."""
        raise NotImplementedError

    async def _delete_production_directory(self) -> None:
        """Override per store: how to clear production."""
        raise NotImplementedError

    async def _commit_staged(self, delete_production_first: bool = False) -> None:
        """Common logic: atomic commit."""
        if delete_production_first:
            await self._delete_production_directory()

        for staging_path in self._staged_paths:
            production_path = self._get_production_path(staging_path)
            await self._move_staged_to_production(staging_path, production_path)

        self._staged_paths.clear()

    async def _rollback_staged(self) -> None:
        """Common logic: clean up on failure."""
        # Each store implements cleanup
        self._staged_paths.clear()
```

## Store Implementations

```python
# OpenMirroringStore
def _get_staging_path(self, relative_path: str) -> str:
    return self.base_path.replace("/Files/LandingZone/", "/Files/_tmp/") + relative_path

async def _move_staged_to_production(self, staging: str, production: str) -> None:
    await self._get_adls_ops().move_file(staging, production)

# ParquetStore (local)
def _get_staging_path(self, relative_path: str) -> str:
    return str(self.staging_dir / relative_path)

async def _move_staged_to_production(self, staging: str, production: str) -> None:
    Path(staging).rename(production)  # atomic on same filesystem
```

## Benefits

- Abstract pattern, store-specific implementation
- Removes ~100 lines from OpenMirroringStore
- Reusable for ANY store (local, cloud, future)
- Enables proper rollback on failure (currently missing in OpenMirroringStore)
- Each store controls its own staging strategy

## Effort Estimate

- **Mixin implementation:** 2-3 hours
- **OpenMirroringStore refactor:** 2-3 hours
- **ParquetStore adoption (optional):** 1-2 hours
- **Total:** ~1 day

## Priority

**Foundational.** Complete before GCS Store work begins.

This isn't optional cleanup – it:
- **Blocks GCS Store** (and every future cloud store) from clean implementation
- **Adds missing rollback** capability (reliability gap today)
- **Reduces new store effort** significantly

## Related Issues

- [openmirroring-refactor.md](openmirroring-refactor.md) - Would consume this abstraction
