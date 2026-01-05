---
title: Store Type Hints Enhancement
priority: low
---

### Problem

The Store interface uses `Any` in several places, reducing type safety and IDE support:

```python
# src/hygge/core/flow/flow.py
def __init__(
    self,
    name: str,
    home: Home,
    store: Any,  # Should be Store type
    ...
)

# src/hygge/core/flow/factory.py
def _create_store_instance(...) -> Store:  # Good
def _inject_connection_pool(
    store: Store,
    store_config: Any,  # Could be more specific
    ...
)
```

### Current State

- Store base class exists with abstract methods
- Optional methods have default implementations (good!)
- Type hints are inconsistent across the codebase
- IDE autocomplete is limited in some areas

### Impact

- **Developer Experience:** Limited IDE autocomplete and type checking
- **Maintainability:** Harder to understand expected interfaces
- **Documentation:** Types serve as implicit documentation

### Desired Behaviour

Explicit Store protocol or ABC that defines the full interface:

```python
from typing import Protocol, Optional, Any

class StoreProtocol(Protocol):
    """Protocol defining the Store interface."""

    name: str
    logger: Any

    async def write(self, df: pl.DataFrame) -> None:
        """Write data to store."""
        ...

    async def finish(self) -> None:
        """Finalize writes and move staged data."""
        ...

    # Optional methods with default implementations
    def configure_for_run(self, run_type: str) -> None:
        """Configure store for run type (truncate vs append)."""
        ...

    async def cleanup_staging(self) -> None:
        """Clean up staging/tmp directory before retrying."""
        ...

    async def reset_retry_sensitive_state(self) -> None:
        """Reset retry-sensitive state (counters, paths)."""
        ...

    def set_pool(self, pool: Any) -> None:
        """Set connection pool for database stores."""
        ...
```

### Proposed Changes

1. **Add StoreProtocol to `src/hygge/core/store.py`:**
   ```python
   from typing import Protocol, runtime_checkable

   @runtime_checkable
   class StoreProtocol(Protocol):
       """Protocol defining the Store interface for type checking."""
       ...
   ```

2. **Update Flow type hints:**
   ```python
   # src/hygge/core/flow/flow.py
   def __init__(
       self,
       name: str,
       home: Home,
       store: "Store",  # Use Store type
       ...
   )
   ```

3. **Update FlowFactory type hints:**
   ```python
   # src/hygge/core/flow/factory.py
   from ..store import Store, StoreConfig

   def _create_store_instance(...) -> Store: ...
   def _inject_connection_pool(
       store: Store,
       store_config: "StoreConfig",  # More specific
       ...
   )
   ```

4. **Add StoreConfig base type:**
   ```python
   class StoreConfig(Protocol):
       """Protocol for store configuration."""
       type: str
       # Common fields...
   ```

### Considerations

- **Backward Compatibility:** Protocol is structural, existing stores remain compatible
- **Runtime Overhead:** `@runtime_checkable` adds minimal overhead
- **Import Cycles:** May need careful import structure to avoid cycles
- **Pydantic Models:** StoreConfig classes already use Pydantic, protocol should align

### Acceptance Criteria

- [ ] StoreProtocol defined in `src/hygge/core/store.py`
- [ ] Flow class uses Store type hint
- [ ] FlowFactory uses explicit type hints
- [ ] IDE autocomplete works for store methods
- [ ] mypy passes (if configured)
- [ ] No breaking changes to existing stores

### Estimated Effort

1 day

### Related

- `src/hygge/core/store.py` - Store base class
- `src/hygge/core/flow/flow.py` - Flow class
- `src/hygge/core/flow/factory.py` - FlowFactory class
- `src/hygge/stores/` - Store implementations
- `.issues/__TECHNICAL_REVIEW_SUMMARY.md` - Technical review tracking

### Notes

This is a **low priority** improvement. The current code works correctly, and the benefit is primarily developer experience. Consider implementing when:

- Adding new stores where type hints would help
- Refactoring flow/factory code
- Setting up stricter mypy configuration
