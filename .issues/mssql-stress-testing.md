---
title: MSSQL Stress Testing
---

### Problem

The technical review identified a need for stress testing at midmarket scale, including:
- Large data volume tests (100M+ rows)
- Concurrent flow stress tests (10+ flows running simultaneously)
- Connection pool exhaustion scenarios

### Current Status

✅ **Parquet-to-Parquet stress testing completed:**
- Large volume tests (10M-100M rows) - validated
- Concurrent flows (10+ flows simultaneously) - validated
- Memory efficiency at scale - validated
- Data integrity verification - validated

⏸️ **MSSQL stress testing deferred:**
- Database-specific stress tests require Azure SQL setup
- Connection pool exhaustion scenarios need dedicated database resources
- Can be addressed in a future review cycle

### Desired Behaviour

Add comprehensive MSSQL stress tests covering:
1. **Large volume MSSQL writes** (100M+ rows)
   - Scale existing `test_mssql_large_volume.py` (currently 500K rows) to 100M+
   - Validate high-throughput writes at production scale
   - Test optimal batch sizes and parallel workers

2. **Concurrent MSSQL flows** (10+ flows)
   - Multiple flows writing to MSSQL simultaneously
   - Test connection pool behavior under concurrent load
   - Validate no connection pool exhaustion or deadlocks

3. **Connection pool exhaustion scenarios**
   - Test behavior when flows exceed available connections
   - Validate graceful handling of connection limits
   - Test retry behavior under connection pressure

### Considerations

- **Parquet-to-parquet stress tests are sufficient for initial validation:**
  - Core framework performance validated at scale
  - Data integrity verified across large volumes
  - Concurrent execution validated
  - Memory efficiency confirmed

- **MSSQL stress tests can be deferred because:**
  - Database-specific tests require Azure SQL credentials and setup
  - Connection pool scenarios need dedicated test infrastructure
  - Current MSSQL tests (500K rows) already validate basic functionality
  - Framework architecture is validated through parquet-to-parquet tests

- **When to revisit:**
  - Next technical review cycle (quarterly)
  - If production issues arise related to MSSQL scale
  - When dedicated test database infrastructure is available

### Implementation Notes

When implementing MSSQL stress tests:
- Extend `tests/integration/test_mssql_large_volume.py`
- Follow pattern established in `test_parquet_to_parquet_stress.py`
- Use `@pytest.mark.slow` for extreme volume tests
- Require Azure SQL credentials (skip if not available)
- Test connection pool limits explicitly

### Related

- `tests/integration/test_parquet_to_parquet_stress.py` - Completed stress test suite
- `tests/integration/test_mssql_large_volume.py` - Existing MSSQL large volume test (500K rows)
- `.issues/__TECHNICAL_REVIEW_SUMMARY.md` - Technical review tracking
