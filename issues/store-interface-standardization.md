# Store Interface Standardization: Explicit Optional Methods

## Problem

Stores have optional methods (`configure_for_run`, `cleanup_staging`, `reset_retry_sensitive_state`) that aren't in the base class:

- Methods are called with `hasattr()` checks, which is fragile
- Unclear contracts for store implementers
- Runtime errors if methods are called on stores that don't implement them
- No way to know which methods are required vs optional

This makes the store interface unclear and error-prone.

## Current Behavior

### Optional Methods Called with hasattr()

```python
# In Flow.__init__()
if hasattr(self.store, "configure_for_run"):
    self.store.configure_for_run(self.run_type)

# In Flow._cleanup_before_retry()
try:
    await self.store.cleanup_staging()
except Exception as cleanup_error:
    self.logger.warning(f"Failed to cleanup staging: {str(cleanup_error)}")

try:
    await self.store.reset_retry_sensitive_state()
except Exception as reset_error:
    self.logger.warning(f"Failed to reset retry-sensitive state: {str(reset_error)}")
```

### No Clear Contract

Store implementers don't know which methods are required vs optional:
- Is `configure_for_run()` required?
- Is `cleanup_staging()` required?
- Is `reset_retry_sensitive_state()` required?

## Use Cases

1. **Clear Contracts**: Store implementers know which methods are required vs optional
2. **Type Safety**: Type checkers can verify method signatures
3. **Better Error Messages**: Fail fast with clear error messages if required methods are missing
4. **Easier Testing**: Mock stores can implement only required methods

## Proposed Solution

### Phase 1: Define Store Protocol/ABC with Optional Methods

Create a clear contract for stores with optional methods.

**File Location:** `src/hygge/core/store.py` (existing file - update base class)

```python
# In src/hygge/core/store.py

class Store(ABC):
    """Base class for all data stores."""

    # Required methods
    @abstractmethod
    async def write(self, data: pl.DataFrame) -> None:
        """Write data to this store."""
        pass

    @abstractmethod
    async def finish(self) -> None:
        """Finish writing data to this store."""
        pass

    # Optional methods with default implementations
    def configure_for_run(self, run_type: str) -> None:
        """
        Configure the store for the upcoming run type.

        This is an optional method that stores can override to adjust
        their strategy based on flow run_type. Default implementation
        is a no-op.

        Args:
            run_type: Run type ('full_drop' or 'incremental')
        """
        pass

    async def cleanup_staging(self) -> None:
        """
        Clean up staging/tmp directories before retry.

        This is an optional method that stores can override to clean up
        staging directories before retrying. Default implementation
        is a no-op.

        Raises:
            StoreError: If cleanup fails
        """
        pass

    async def reset_retry_sensitive_state(self) -> None:
        """
        Reset retry-sensitive state before retry.

        This is an optional method that stores can override to reset
        state that should be cleared before retrying (e.g., sequence
        counters). Default implementation is a no-op.

        Raises:
            StoreError: If reset fails
        """
        pass

    def set_pool(self, pool: ConnectionPool) -> None:
        """
        Set connection pool for stores that need it.

        This is an optional method that stores can override to receive
        a connection pool. Default implementation is a no-op.

        Args:
            pool: Connection pool to use
        """
        pass
```

### Phase 2: Update Flow to Use Methods Directly

Update Flow to call methods directly without `hasattr()` checks:

```python
# In Flow.__init__()
def __init__(self, ...):
    ...
    # Call configure_for_run directly (default implementation is no-op)
    self.store.configure_for_run(self.run_type)

# In Flow._cleanup_before_retry()
async def _cleanup_before_retry(self, retry_state) -> None:
    """Clean up staging/tmp and reset flow state before retrying."""
    ...

    # Call cleanup_staging directly (default implementation is no-op)
    try:
        await self.store.cleanup_staging()
    except StoreError as cleanup_error:
        self.logger.warning(f"Failed to cleanup staging: {str(cleanup_error)}")

    # Call reset_retry_sensitive_state directly (default implementation is no-op)
    try:
        await self.store.reset_retry_sensitive_state()
    except StoreError as reset_error:
        self.logger.warning(f"Failed to reset retry-sensitive state: {str(reset_error)}")
```

### Phase 3: Update Coordinator to Use Methods Directly

Update Coordinator to call methods directly:

```python
# In Coordinator._inject_store_pool()
def _inject_store_pool(self, store: Store, store_config) -> None:
    """Inject connection pool into stores that need it."""
    # Call set_pool directly (default implementation is no-op)
    if hasattr(store_config, "connection") and store_config.connection:
        pool = self.connection_pools.get(store_config.connection)
        if pool:
            store.set_pool(pool)
        else:
            conn_name = store_config.connection
            self.logger.warning(f"Connection '{conn_name}' referenced but not found")
```

### Phase 4: Document Store Interface

Document the store interface clearly:

```python
# In hygge/core/store.py

class Store(ABC):
    """
    Base class for all data stores.

    Stores must implement:
    - `write()`: Write data to the store
    - `finish()`: Finish writing data to the store

    Stores can optionally override:
    - `configure_for_run()`: Configure store for run type
    - `cleanup_staging()`: Clean up staging directories
    - `reset_retry_sensitive_state()`: Reset retry-sensitive state
    - `set_pool()`: Set connection pool for database stores

    Example:
        ```python
        class MyStore(Store, store_type="my_type"):
            async def write(self, data: pl.DataFrame) -> None:
                # Implementation
                pass

            async def finish(self) -> None:
                # Implementation
                pass

            def configure_for_run(self, run_type: str) -> None:
                # Optional: Override to adjust strategy
                if run_type == "full_drop":
                    # Truncate destination
                    pass
        ```
    """
```

## File Locations

- **Store Base Class**: `src/hygge/core/store.py` (existing file - add default implementations)
- **Flow**: `src/hygge/core/flow.py` (existing file - remove hasattr checks)
- **Coordinator**: `src/hygge/core/coordinator.py` (existing file - remove hasattr checks)
- **Store Implementations**: `src/hygge/stores/*/store.py` (existing files - override methods as needed)
- **Tests**: `tests/unit/hygge/core/test_store.py` (existing file - extend), `tests/unit/hygge/stores/test_*_store.py` (existing files - extend)

## Implementation Plan

1. **Add default implementations to Store base class** (highest impact)
   - Update `src/hygge/core/store.py` to add default implementations for optional methods
   - Update documentation in Store class docstring to clarify required vs optional methods
   - Update existing stores in `src/hygge/stores/*/store.py` to override methods as needed

2. **Update Flow to use methods directly** (high impact)
   - Update `src/hygge/core/flow.py` to remove `hasattr()` checks
   - Call methods directly (default implementations handle missing methods)
   - Update error handling to catch `StoreError`
   - Update tests in `tests/unit/hygge/core/test_flow.py`

3. **Update Coordinator to use methods directly** (medium impact)
   - Update `src/hygge/core/coordinator.py` to remove `hasattr()` checks
   - Call methods directly
   - Update error handling
   - Update tests in `tests/unit/hygge/core/test_coordinator.py`

4. **Update documentation** (low impact)
   - Document store interface clearly in `src/hygge/core/store.py`
   - Add examples of required vs optional methods
   - Update store implementation guide if one exists

## Testing Considerations

- Unit tests for store base class default implementations
- Unit tests for stores that override optional methods
- Integration tests to verify stores work correctly
- Test that default implementations are no-ops
- Test that stores can override optional methods

## Related Issues

- See `error-handling-standardization.md` for related error handling improvements
- See `coordinator-refactoring.md` for related Coordinator refactoring

## Priority

**Medium** - This will make the store interface clearer and more type-safe, but it's not blocking critical functionality. However, it should be done before adding more store implementations.
