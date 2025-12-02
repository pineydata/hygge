# Error Handling Standardization: Consistent Exception Usage

## Problem

Custom exception hierarchy exists (`HyggeError`, `FlowError`, `HomeError`, `StoreError`, `ConfigError`), but usage is inconsistent:

- Some places catch generic `Exception` instead of specific exceptions
- Retry logic uses string matching on error messages (brittle)
- Journal failures are logged as warnings but don't surface clearly
- Some errors are swallowed silently
- **Exception context is lost when re-raising** - original stack traces are not preserved
- **97 instances of `except Exception`** found across codebase - most are appropriate for error boundaries, but some could be more specific

This makes error handling unpredictable and debugging difficult, especially in production at midmarket org scale where clear error messages are critical for troubleshooting.

## Current Behavior

### Generic Exception Handling

```python
# In Flow._execute_flow()
try:
    await producer
    await consumer
except Exception as e:  # Too generic
    self.logger.error(f"Flow failed: {self.name}, error: {str(e)}")
    raise FlowError(f"Flow failed: {str(e)}")
```

### String Matching in Retry Logic

```python
# In Flow._should_retry_flow_error()
def _should_retry_flow_error(self, exception: Exception) -> bool:
    if not isinstance(exception, FlowError):
        return False

    # String matching is brittle
    error_str = str(exception).lower()
    return "connection" in error_str and (
        "forcibly closed" in error_str
        or "communication link failure" in error_str
        or "08s01" in error_str
    )
```

### Silent Journal Failures

```python
# In Flow._record_entity_run()
try:
    await self.journal.record_entity_run(...)
except Exception as e:
    # Journal failures should not break flows
    self.logger.warning(f"Failed to record entity run in journal: {str(e)}")
    # But this makes it hard to debug journal issues
```

## Use Cases

1. **Predictable Error Handling**: Specific exceptions make error handling predictable
2. **Better Retry Logic**: Retry based on exception types, not string matching
3. **Clearer Error Messages**: Users get clear, actionable error messages
4. **Easier Debugging**: Errors are surfaced clearly, not hidden in logs

## Proposed Solution

### Phase 1: Define Specific Exception Types

Create specific exception types for common error scenarios.

**File Location:** `src/hygge/utility/exceptions.py` (existing file - extend)

```python
# In src/hygge/utility/exceptions.py

class HyggeError(Exception):
    """Base exception for all hygge errors."""
    pass

class FlowError(HyggeError):
    """Base exception for flow-related errors."""
    pass

class FlowExecutionError(FlowError):
    """Error during flow execution."""
    pass

class FlowConnectionError(FlowError):
    """Transient connection error during flow execution."""
    pass

class HomeError(HyggeError):
    """Base exception for home-related errors."""
    pass

class HomeConnectionError(HomeError):
    """Connection error when reading from home."""
    pass

class HomeReadError(HomeError):
    """Error reading data from home."""
    pass

class StoreError(HyggeError):
    """Base exception for store-related errors."""
    pass

class StoreConnectionError(StoreError):
    """Connection error when writing to store."""
    pass

class StoreWriteError(StoreError):
    """Error writing data to store."""
    pass

class ConfigError(HyggeError):
    """Configuration error."""
    pass

class JournalError(HyggeError):
    """Base exception for journal-related errors."""
    pass

class JournalWriteError(JournalError):
    """Error writing to journal."""
    pass
```

### Phase 2: Update Retry Logic to Use Exception Types

Update retry logic to use exception types instead of string matching:

```python
# In Flow._should_retry_flow_error()
def _should_retry_flow_error(self, exception: Exception) -> bool:
    """Determine if a FlowError should be retried."""
    # Retry on transient connection errors
    if isinstance(exception, (FlowConnectionError, HomeConnectionError, StoreConnectionError)):
        return True

    # Don't retry on other errors
    return False
```

### Phase 3: Update Error Handling to Use Specific Exceptions

Update error handling throughout the codebase:

```python
# In Flow._execute_flow()
try:
    await producer
    await consumer
except FlowConnectionError as e:
    # Transient connection error - will be retried
    raise
except FlowError as e:
    # Other flow errors - don't retry
    raise
except Exception as e:
    # Unexpected errors - wrap in FlowError
    # CRITICAL: Use 'from e' to preserve exception context
    raise FlowExecutionError(f"Unexpected error in flow {self.name}: {str(e)}") from e
```

**Key Improvement:** Always use exception chaining (`from e`) when re-raising exceptions to preserve original stack traces. This is critical for debugging production issues at midmarket scale.

### Phase 4: Surface Journal Errors Clearly

Make journal errors visible but non-blocking:

```python
# In Flow._record_entity_run()
try:
    await self.journal.record_entity_run(...)
except JournalWriteError as e:
    # Log error clearly but don't break flow
    self.logger.error(
        f"Failed to record entity run in journal: {str(e)}. "
        f"This is non-blocking, but journal tracking may be incomplete."
    )
    # Optionally: Track journal failures in flow result
    self.journal_failures.append(str(e))
except Exception as e:
    # Unexpected errors - log and wrap
    self.logger.error(
        f"Unexpected error recording entity run in journal: {str(e)}"
    )
    raise JournalError(f"Unexpected journal error: {str(e)}") from e
```

### Phase 5: Update Store/Home Implementations

Update store and home implementations to raise specific exceptions:

```python
# In MssqlHome._get_batches()
try:
    # Read data from database
    ...
except pyodbc.Error as e:
    # Check if it's a connection error
    if "08S01" in str(e) or "connection" in str(e).lower():
        raise HomeConnectionError(f"Connection error reading from {self.name}: {str(e)}") from e
    else:
        raise HomeReadError(f"Error reading from {self.name}: {str(e)}") from e
```

## File Locations

- **Exception Types**: `src/hygge/utility/exceptions.py` (existing file - extend)
- **Flow**: `src/hygge/core/flow.py` (existing file - update error handling)
- **Store Implementations**: `src/hygge/stores/*/store.py` (existing files - update error handling)
- **Home Implementations**: `src/hygge/homes/*/home.py` (existing files - update error handling)
- **Journal**: `src/hygge/core/journal.py` (existing file - update error handling)
- **Tests**: `tests/unit/hygge/utility/test_retry.py` (existing file - extend), `tests/unit/hygge/core/test_flow.py` (existing file - extend)

## Implementation Plan

1. **Define specific exception types** (highest impact)
   - Add new exception types to `src/hygge/utility/exceptions.py`
   - Update `src/hygge/utility/__init__.py` to export new exceptions
   - Document when each exception should be used
   - Update existing code to use new exceptions

2. **Update retry logic** (high impact)
   - Update `src/hygge/core/flow.py` to replace string matching with exception type checking
   - Update `src/hygge/utility/retry.py` if needed for exception type support
   - Update retry decorator to use exception types
   - Add unit tests in `tests/unit/hygge/utility/test_retry.py`

3. **Update error handling** (medium impact)
   - Update `src/hygge/core/flow.py` to replace generic `Exception` catches with specific exceptions
   - **CRITICAL: Add exception chaining (`from e`) when re-raising** - preserves stack traces for production debugging
   - Update error messages to be clear and actionable
   - Add unit tests in `tests/unit/hygge/core/test_flow.py` for error handling

4. **Update store/home implementations** (medium impact)
   - Update store implementations in `src/hygge/stores/*/store.py` to raise specific exceptions
   - Update home implementations in `src/hygge/homes/*/home.py` to raise specific exceptions
   - Add unit tests for error scenarios
   - Document error conditions

5. **Surface journal errors** (low impact)
   - Update `src/hygge/core/journal.py` and `src/hygge/core/flow.py` to make journal errors visible but non-blocking
   - Optionally track journal failures in flow results
   - Add unit tests in `tests/unit/hygge/core/test_journal.py` and `test_flow.py` for journal error handling

## Testing Considerations

- Unit tests for exception types
- Unit tests for retry logic with different exception types
- Integration tests to verify error handling works correctly
- Test that journal errors don't break flows
- Test that connection errors are retried correctly

## Related Issues

- See `coordinator-refactoring.md` for related error handling in Coordinator
- See `watermark-tracker-extraction.md` for related error handling in Flow

## Technical Review Findings

**From Technical Review (2025):**
- Found 97 instances of `except Exception` - most are appropriate for error boundaries (Flow, Coordinator), but some could be more specific
- Exception context is lost when re-raising - original stack traces not preserved (e.g., `flow.py:222`)
- Connection errors should use specific exception types instead of string matching
- Error handling is functional but could be more precise for better production debugging

**Impact at Midmarket Scale:**
- Clear error messages are critical when debugging production issues at 2am
- Exception chaining (`from e`) is essential for preserving context
- Specific exceptions make retry logic more reliable and predictable

## Priority

**High** - This directly addresses technical review findings and significantly improves production debugging. Should be prioritized before adding more features that rely on error handling. Exception chaining should be implemented immediately as it's a simple change with high impact.
