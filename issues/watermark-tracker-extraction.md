# Watermark Extraction: Separate Watermark Logic from Flow Execution

## Problem

Watermark tracking logic in `Flow` is mixed with execution logic:

- Type checking and aggregation happens during batch processing
- Warnings are tracked per-batch instead of being validated upfront
- No validation that watermark columns exist before starting
- Watermark logic is scattered across multiple methods in Flow

This makes the code harder to test and maintain, and violates separation of concerns.

## Current Behavior

Watermark tracking happens during batch processing:

```python
# In Flow._consumer()
async def _consumer(self, queue: asyncio.Queue, ...):
    while True:
        batch = await queue.get()
        # Write to store
        await self.store.write(batch)
        # Update watermark tracker (happens during processing)
        self._update_watermark(batch)
        ...
```

Watermark validation happens reactively (warns when column is missing) instead of proactively (validates before starting).

## Use Cases

1. **Upfront Validation**: Validate watermark configuration before starting flow execution
2. **Easier Testing**: Isolated watermark logic can be tested independently
3. **Better Error Messages**: Fail fast with clear error messages instead of warnings during execution
4. **Clearer Responsibilities**: Flow handles orchestration, Watermark handles watermark logic

## Proposed Solution

### Phase 1: Create Watermark Class

Create a dedicated `Watermark` class.

**File Location:** `src/hygge/core/watermark.py` (new file)

```python
# In src/hygge/core/watermark.py
class Watermark:
    """Tracks watermark values across batches."""

    def __init__(
        self,
        watermark_config: Dict[str, str],
        logger: Logger,
    ):
        self.primary_key = watermark_config.get("primary_key")
        self.watermark_column = watermark_config.get("watermark_column")
        self.logger = logger

        self._watermark_candidate: Optional[Any] = None
        self._watermark_type: Optional[str] = None

    def validate_schema(self, schema: pl.Schema) -> None:
        """Validate that watermark columns exist in schema."""
        if self.primary_key not in schema:
            raise ConfigError(
                f"Primary key '{self.primary_key}' not found in data schema"
            )

        if self.watermark_column not in schema:
            raise ConfigError(
                f"Watermark column '{self.watermark_column}' not found in data schema"
            )

        # Validate watermark column type is supported
        watermark_dtype = schema[self.watermark_column]
        if not self._is_supported_type(watermark_dtype):
            raise ConfigError(
                f"Unsupported watermark type: {watermark_dtype}. "
                f"Supported types: datetime, integer, string"
            )

    def _is_supported_type(self, dtype: pl.DataType) -> bool:
        """Check if dtype is supported for watermark tracking."""
        if dtype == pl.Datetime:
            return True
        if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                     pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
            return True
        if dtype == pl.Utf8:
            return True
        return False

    def update(self, batch: pl.DataFrame) -> None:
        """Update watermark value from batch."""
        if self.watermark_column not in batch.columns:
            return

        column_series = batch[self.watermark_column]
        if column_series.is_null().all():
            return

        dtype = column_series.dtype
        candidate_type: Optional[str] = None
        candidate_value: Optional[Any] = None

        if dtype == pl.Datetime:
            candidate_type = "datetime"
            candidate_value = column_series.max()
        elif dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                       pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
            candidate_type = "int"
            candidate_value = int(column_series.max())
        elif dtype == pl.Utf8:
            candidate_type = "string"
            candidate_value = str(column_series.max())
        else:
            return

        if candidate_value is None:
            return

        if self._watermark_candidate is None:
            self._watermark_candidate = candidate_value
            self._watermark_type = candidate_type
            return

        if self._watermark_type != candidate_type:
            self.logger.warning(
                f"Inconsistent watermark types across batches: "
                f"{self._watermark_type} vs {candidate_type}"
            )
            return

        if candidate_value > self._watermark_candidate:
            self._watermark_candidate = candidate_value

    def get_watermark_value(self) -> Optional[Any]:
        """Get current watermark value."""
        return self._watermark_candidate

    def get_watermark_type(self) -> Optional[str]:
        """Get current watermark type."""
        return self._watermark_type

    def serialize_watermark(self) -> Optional[str]:
        """Serialize watermark value for journal storage."""
        if self._watermark_candidate is None:
            return None

        if self._watermark_type == "datetime":
            return self._watermark_candidate.isoformat()
        elif self._watermark_type == "int":
            return str(self._watermark_candidate)
        elif self._watermark_type == "string":
            return self._watermark_candidate
        else:
            return None
```

### Phase 2: Integrate Watermark into Flow

Update Flow to use Watermark:

```python
# In Flow.__init__()
def __init__(self, ...):
    ...
    self.watermark_config = watermark_config

    # Create watermark if config provided
    if self.watermark_config:
        self.watermark = Watermark(
            self.watermark_config,
            self.logger,
        )
    else:
        self.watermark = None

# In Flow._execute_flow()
async def _execute_flow(self) -> None:
    """Execute a single attempt of the flow."""
    ...

    # Validate watermark configuration before starting
    if self.watermark:
        # Get schema from first batch (or validate against home schema if available)
        # This should happen before producer starts
        ...

    # Start producer and consumer
    ...
```

### Phase 3: Validate Watermark Configuration Upfront

Validate watermark configuration during flow setup:

```python
# In Flow._prepare_incremental_context()
async def _prepare_incremental_context(self) -> None:
    """Resolve watermark context for incremental runs."""
    ...

    # If watermark config provided, validate it
    if self.watermark and self.watermark_config:
        # Try to get schema from home (if available)
        if hasattr(self.home, "get_schema"):
            schema = await self.home.get_schema()
            self.watermark.validate_schema(schema)
        # Otherwise, validate on first batch
        ...
```

## File Locations

- **Watermark**: `src/hygge/core/watermark.py` (new file)
- **Flow**: `src/hygge/core/flow.py` (existing file - refactor)
- **Tests**: `tests/unit/hygge/core/test_watermark.py` (new file), `test_flow.py` (existing file - extend tests)

## Implementation Plan

1. **Create Watermark class** (highest impact)
   - Create `src/hygge/core/watermark.py`
   - Move watermark tracking logic from `src/hygge/core/flow.py` to Watermark
   - Add schema validation method
   - Add unit tests in `tests/unit/hygge/core/test_watermark.py`

2. **Integrate Watermark into Flow** (medium impact)
   - Update `src/hygge/core/flow.py` to use Watermark
   - Remove watermark tracking methods from Flow
   - Update Flow tests in `tests/unit/hygge/core/test_flow.py` to use Watermark

3. **Add upfront validation** (medium impact)
   - Validate watermark configuration during flow setup in Flow
   - Fail fast with clear error messages
   - Update tests to verify validation

## Testing Considerations

- Unit tests for Watermark (type detection, aggregation, serialization)
- Integration tests to verify Flow still works correctly
- Test edge cases (missing columns, unsupported types, null values)
- Test validation errors are raised correctly

## Related Issues

- See `coordinator-refactoring.md` for related Flow refactoring
- See `error-handling-standardization.md` for related error handling improvements

## Priority

**Medium** - This will make watermark logic easier to test and maintain, but it's not blocking critical functionality. However, it should be done before adding more watermark features.
