---
title: SQLite Store Test Failures
priority: high
---

### Problem

Six SQLite store tests are failing with `RuntimeError: Event loop is closed`:

```
FAILED tests/unit/hygge/stores/test_sqlite_store.py::TestSqliteStoreDataWriting::test_write_single_batch
FAILED tests/unit/hygge/stores/test_sqlite_store.py::TestSqliteStoreDataWriting::test_write_multiple_batches
FAILED tests/unit/hygge/stores/test_sqlite_store.py::TestSqliteStoreDataWriting::test_write_incremental_data
FAILED tests/unit/hygge/stores/test_sqlite_store.py::TestSqliteStoreDataWriting::test_table_created_automatically
FAILED tests/unit/hygge/stores/test_sqlite_store.py::TestSqliteStoreDataWriting::test_append_to_existing_table
FAILED tests/unit/hygge/stores/test_sqlite_store.py::TestSqliteStoreIntegration::test_sqlite_store_progress_tracking
```

Error message:
```
RuntimeError: Event loop is closed

The above exception was the direct cause of the following exception:

hygge.utility.exceptions.FlowError: Consumer failed: Event loop is closed
```

### Root Cause Analysis

This is an **async event loop cleanup issue** in the test fixtures, not a production bug. The error occurs when:

1. Test completes and pytest-asyncio begins cleanup
2. Event loop is closed before all async operations complete
3. Queued operations (like `asyncio.Queue.get()`) fail with "Event loop is closed"

This is a common issue with pytest-asyncio fixtures where the scope of the event loop doesn't match the lifecycle of async resources.

### Impact

- **Production:** No impact - SQLite store works correctly in production
- **Tests:** 6 tests failing, reducing confidence in test suite
- **CI:** Tests show failures, complicating PR review

### Desired Behaviour

All SQLite store tests should pass without event loop errors.

### Proposed Solution

1. **Check fixture scope alignment:**
   ```python
   @pytest.fixture(scope="function")
   async def sqlite_store():
       # Ensure store cleanup completes before event loop closes
       store = SqliteStore(...)
       yield store
       await store.close()  # Explicit cleanup within event loop
   ```

2. **Use `pytest.mark.asyncio(loop_scope="function")`** if available in pytest-asyncio version

3. **Ensure async cleanup completes before fixture teardown:**
   ```python
   @pytest.fixture
   async def store_with_cleanup():
       store = SqliteStore(...)
       try:
           yield store
       finally:
           # Cancel any pending operations
           # Then cleanup
           await store.close()
   ```

4. **Consider wrapping cleanup in asyncio.shield()** if operations must complete

### Implementation Notes

- Check `tests/unit/hygge/stores/test_sqlite_store.py` for fixture definitions
- Compare with working store tests (ParquetStore, ADLSStore) for patterns
- May need to update pytest-asyncio configuration in `pytest.ini`
- Test locally with `pytest tests/unit/hygge/stores/test_sqlite_store.py -v`

### Acceptance Criteria

- [ ] All 6 SQLite store tests passing
- [ ] No "Event loop is closed" errors in test output
- [ ] Pattern documented for other async store tests
- [ ] CI pipeline shows green for all tests

### Estimated Effort

0.5 day

### Related

- `tests/unit/hygge/stores/test_sqlite_store.py` - Failing tests
- `src/hygge/stores/sqlite/store.py` - SQLite store implementation
- `pytest.ini` - pytest-asyncio configuration
- `.issues/__TECHNICAL_REVIEW_SUMMARY.md` - Technical review tracking
