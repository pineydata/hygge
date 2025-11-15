---
title: PR Summary
---

## Overview

- Implemented automatic retry mechanism for transient connection errors at the flow level, ensuring data integrity by cleaning up staging directories before each retry.
- Enhanced retry decorator with custom retry conditions and before-sleep callbacks for flexible error handling.
- Fixed summary display to always show execution results, even when flows fail.

## Key Changes

### Flow Retry & Error Handling

- `src/hygge/core/flow.py`:
  - Implemented flow-level retry using enhanced `with_retry` decorator
  - Added `_should_retry_flow_error()` to identify transient connection errors
  - Added `_cleanup_before_retry()` to reset flow state and clean staging before retries
  - Retries entire flow (producer + consumer) on transient errors for atomicity

- `src/hygge/core/coordinator.py`:
  - Fixed summary display to always show execution results before re-raising exceptions
  - Ensures dbt-style summary is visible even when flows fail

### Retry Utility

- `src/hygge/utility/retry.py`:
  - Enhanced `with_retry` decorator with `retry_if_func` for custom retry conditions
  - Added `before_sleep_func` parameter for cleanup/setup before each retry
  - Fixed logger compatibility (uses standard logger for tenacity's before_sleep_log)
  - Changed from `retry_if` to `retry_if_exception` (correct tenacity import)

### Store Cleanup

- `src/hygge/core/store.py`:
  - Added abstract `cleanup_staging()` method for stores to clean temporary directories

- `src/hygge/stores/parquet/store.py`:
  - Implemented `cleanup_staging()` to remove local staging files before retry

- `src/hygge/stores/adls/store.py`:
  - Implemented `cleanup_staging()` to remove cloud `_tmp` directories before retry
  - Fixed path construction to correctly target entity-specific staging directories

### Tests

- `tests/unit/hygge/core/test_flow.py`:
  - Added tests for flow retry on transient connection errors
  - Added tests for state reset and cleanup before retry
  - Added tests for non-transient error handling (no retry)
  - Added tests for max retry limit

- `tests/unit/hygge/utility/test_retry.py`:
  - Comprehensive unit tests for `with_retry` decorator
  - Tests for custom retry conditions, before_sleep callbacks, exponential backoff, timeout enforcement, and more

## Testing

- All tests passing: `pytest` (502 tests collected, all passing)
