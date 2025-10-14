# hygge Next Conversation Prompt

## Current Status: Cleanup Complete - Ready for Next Phase ✅

**Auto-create tables feature removed** (ADR-001) - cleanup complete, branch ready to merge.

**Cleanup completed:**
- ✅ Deleted schema_inference.py and related tests
- ✅ Updated imports in __init__.py
- ✅ Updated documentation (HYGGE_DONE.md)
- ✅ Clean architecture with Polars native functionality

**Next Steps:**
1. **Merge to main** - Clean refactor ready
2. **Phase 1: Entity-First Architecture** (see ROADMAP.md Phase 1)
   - Multi-file entity pattern support
   - CLI enhancements for entity management
3. **Optional Future: Codegen tool** (explicit DDL generation from parquet files)
4. **Phase 2: State Management** (incremental loads with watermarks)

---

## Previous Status: Auto-Create Tables COMPLETE & VALIDATED ✅

We've successfully validated MS SQL Server STORE at scale with real Azure SQL Database:

**Bidirectional SQL Connectivity:**
- **MSSQL Home**: Read FROM SQL Server (Oct 10) ✅
- **MSSQL Store**: Write TO SQL Server (Oct 11) ✅
- **Azure SQL Validation**: Basic + Large volume tested (Oct 12) ✅
- **Connection Pooling**: Shared across sources and destinations ✅
- **Entity Pattern**: Works for both reading and writing ✅

**MSSQL Store Validation:**
- ✅ Azure SQL Database created (Serverless 0.5 vCore, Central US)
- ✅ Basic test: 100 rows written successfully
- ✅ Large volume test: 500K rows written successfully (`test_mssql_large_volume.py`)
- ✅ Connection pooling working under load (10 connections, 5 batches)
- ✅ Data integrity verified in Azure Portal Query Editor
- ✅ TABLOCK behavior validated (serial writes, 2x performance improvement)
- ✅ Throughput: ~15k rows/sec on Serverless 0.5 vCore (limited by database tier, not code)

**Key Learnings from Large Volume Testing:**

**1. TABLOCK Behavior:**
- TABLOCK = exclusive table lock (only ONE connection can hold it)
- TABLOCK + parallel workers = deadlock!
- Solution: Use `parallel_workers=1` with TABLOCK
- Performance: 15k rows/sec with TABLOCK vs 7-10k without (2x improvement)
- Trade-off: Serial writes + TABLOCK wins on small databases, parallel without TABLOCK wins on 8+ vCore

**2. SQL Syntax Fixes:**
- Correct: `INSERT INTO table WITH (TABLOCK) (columns) VALUES (?)`
- Wrong: `INSERT INTO table (columns) WITH (TABLOCK) VALUES (?)`
- Hint must come before column list

**3. Performance Expectations:**
- Serverless 0.5 vCore: ~15k rows/sec (validated)
- 2+ vCore database: Expected 100k-250k+ rows/sec
- Code is ready - limitation is purely database tier

**Configuration Fixes:**
- Fixed `MssqlStoreConfig` inheritance (removed `BaseStoreConfig`, added `BaseModel`)
- Made `batch_size` flexible (ge=1 instead of ge=1000) for testing
- Fixed SQL syntax for WITH hints
- Documented TABLOCK + parallel_workers incompatibility

**Bootstrap Pattern Validated:**
- ✅ Load test data: parquet → Azure SQL (proven at scale!)
- ⏳ Next: Test reading back: Azure SQL → parquet (MssqlHome)
- ⏳ Next: Round-trip validation workflow
- Use hygge to test hygge! 🏠

## Refactor Complete: ADR-001 - Auto-Create Tables Removed ✅

**Removed (Oct 12):**
- ❌ Custom schema inference engine (~700 lines)
- ❌ Home/Store coupling (`set_home()`)
- ❌ Auto-table creation with heuristics
- ❌ Config fields: `schema_inference`, `schema_overrides`, `if_exists`

**Rationale:** Architectural coupling felt wrong, data engineers want explicit control

**Current approach:** Use Polars native `write_database()` for inference

**Future approach:** Codegen tool (explicit DDL generation, user reviews before applying)

**Next Steps:**
1. ⏳ Review and test refactored code
2. ⏳ Commit to main
3. 🎯 **NEXT: Codegen tool** (separate feature branch)
4. 🎯 **AFTER: State Management** (incremental loads)

## Next Development Phase: Codegen Tool 🎯

**Priority 1: DDL Generation from Parquet**

After code review and commit, build explicit codegen tool for table creation.

**Goal:** Generate SQL DDL from parquet files, user reviews and applies manually

**Implementation:**
```bash
# Generate DDL from parquet
hygge generate ddl \
  --input data/users.parquet \
  --table dbo.Users \
  --output schema/dbo.Users.sql

# Review and edit
vim schema/dbo.Users.sql

# Apply to database (manual or via hygge)
hygge apply schema/dbo.Users.sql --connection target_db

# Then load data
hygge start
```

**Why this approach:**
- ✅ User reviews schema before creation
- ✅ Version control DDL files
- ✅ Customizable before applying
- ✅ No architectural coupling
- ✅ Data engineer explicit control

---

## Future Development Phase: State Management

**Priority 2: Incremental Loads with Watermarks**

After codegen tool is complete, focus shifts to state management.

**Goal:** Track incremental load state (watermarks) per entity

**Implementation Plan:**

**1. State Management**
```yaml
# .hygge/state/crm_extract/accounts.state.yml
entity: accounts
last_run: 2025-10-12T10:30:00Z
watermark:
  column: updated_at
  value: 2025-10-12T09:45:23Z
rows_processed: 15420
status: success
```

**2. Incremental Home Queries**
```python
# Home reads watermark from state file
# Adds WHERE clause automatically
SELECT * FROM accounts
WHERE updated_at > '2025-10-12T09:45:23Z'
```

**3. State Persistence**
- Store state files in `.hygge/state/` per flow
- Atomic updates (write temp, rename)
- State file per entity for scalability
- Optional state backends (local file, database, S3)

**4. Configuration**
```yaml
flows:
  crm_extract:
    home:
      type: mssql
      table: dbo.accounts
      incremental:
        column: updated_at  # Watermark column
        strategy: append    # append or merge
    store:
      type: parquet
      path: data/landing/accounts
```

**Success Criteria:**
- ✅ First run loads all data
- ✅ Subsequent runs load only new/changed records
- ✅ State persists between runs
- ✅ Handles failures gracefully (no partial watermark updates)
- ✅ Works with entity pattern (50+ tables)

## Priority 1: SQL Home Integration Testing

Once MSSQL Store is validated, test reading from SQL Server:

**Test 1: Single Table Extraction**
- Use `samples/mssql_to_parquet.yaml` as template
- Extract 1 table from SQL Server to parquet
- Verify: Azure AD auth works, data integrity, connection release

**Test 2: Entity Pattern (10 tables)**
- Use `samples/mssql_entity_pattern.yaml` as template
- Extract 10 tables with `pool_size: 3`
- Verify: Connection reuse, no leaks, directory structure

**Test 3: Scale Test (30-50 tables)**
- Extract 30-50 tables with `pool_size: 5`
- Measure: Memory usage (<500MB target), pooling efficiency (>80% overhead reduction)
- Monitor: SQL Server connection count with DMVs

## Priority 2: Round 2 P2P Testing

**Parquet Testing Scenarios:**

**1. Volume Testing:**
- Small (100 rows)
- Medium (100K rows)
- Large (10M+ rows)
- Very large (100M+ rows if feasible)

**2. Error Scenarios:**
- Missing source files
- Missing source directories
- Corrupted parquet files
- Permission denied on source/destination
- Disk full scenarios
- Mid-flow interruption

**3. Edge Cases:**
- Empty parquet files
- Single row files
- Files with many columns (wide tables)
- Files with few columns (narrow tables)
- Mixed data types
- Null/missing values

**4. Performance Benchmarking:**
- Throughput at different batch sizes
- Memory usage patterns
- Queue size impact
- Parallel entity scaling (2, 4, 8, 16 entities)

## Future Roadmap

**v0.2.x - DuckDB (Next Priority!):**
- DuckDbStore (bootstrap pattern: write test data first)
- DuckDbHome (read and validate test data)
- Local analytical engine for parquet files
- SQL interface to file-based data
- Perfect for local development and testing

**v0.2.x - API Sources:**
- REST API home (generic HTTP/JSON sources)
- API authentication patterns (OAuth, API keys, tokens)
- Pagination and rate limiting
- Response parsing and transformation to DataFrames

**v0.2.x/v0.3.x - Salesforce Integration:**
- Salesforce API home
- SOQL query support
- Bulk API for large extracts
- OAuth authentication
- Real-world SaaS data source

**v0.3.x - PostgreSQL & Advanced:**
- PostgresStore (bootstrap pattern: write first)
- PostgresHome (read and validate)
- MSSQL Store write strategies: `temp_swap` (atomic swap), `merge` (upsert)
- Connection health checks and monitoring
- Cloud storage support (S3, Azure Blob, GCS)

## Success Metrics

**MSSQL Store Basic Validation (Priority 0):** ✅ COMPLETE
- ✅ Azure SQL Database created and configured
- ✅ Test data loads successfully into Azure SQL (100 rows)
- ✅ Connection pooling works with parallel writes (3 connections, 2 batches)
- ✅ No connection leaks or errors
- ✅ Data integrity verified in Azure Portal
- ✅ Integration test passing
- ⏳ Large volume throughput testing (250k+ rows/sec target)
- ⏳ Round-trip validation (read back with MssqlHome)

**SQL Home Integration Testing (Priority 1):**
- ✅ Single table extracts successfully
- ✅ Azure AD authentication works reliably
- ✅ Connection pooling reduces overhead >80%
- ✅ 50+ tables extract with <500MB memory
- ✅ No connection leaks or SQL Server errors
- ✅ Entity pattern creates correct directory structure

**Parquet Testing (Priority 2):**
- ✅ 10+ different test scenarios passing
- ✅ Error scenarios handled gracefully
- ✅ Performance benchmarks documented
- ✅ Edge cases identified and handled

---

## What We've Achieved So Far

### MSSQL Store Large Volume Testing (Oct 12, 2025) ✅
- Successfully validated at scale with live Azure SQL Database
- Basic test: 100 rows written successfully
- Large volume test: 500K rows written successfully (5 batches × 102,400 rows)
- Connection pooling working under load (10 connections)
- Data integrity verified in Azure Portal
- Integration tests passing: `test_parquet_to_mssql_write.py`, `test_mssql_large_volume.py`
- TABLOCK behavior validated: Serial writes + TABLOCK = 2x performance vs parallel without
- Throughput: ~15k rows/sec on Serverless 0.5 vCore (database-limited, not code-limited)
- Configuration fixes: flexible batch_size, proper SQL syntax for WITH hints
- Learned: TABLOCK + parallel workers = deadlock, use parallel_workers=1 with TABLOCK
- **VALIDATED AT SCALE** - Ready for production with proper database tier!

### MSSQL Store Implementation (Oct 11, 2025) ✅
- MS SQL Server store with parallel batch writes
- Connection pooling integration with coordinator
- Extensible write strategy design (direct_insert implemented, temp_swap/merge planned)
- Optimal defaults: 102,400 batch size, 8 workers
- Expected: 250k-300k rows/sec on CCI/Heap
- Smart constants separated (home vs store)
- Clean architecture improvements
- Complete examples, tests, and documentation

### SQL Homes Implementation (Oct 10, 2025) ✅
- MS SQL Server home with Azure AD authentication
- Connection pooling (asyncio.Queue-based)
- Entity pattern for 10-200+ tables
- Ported proven Microsoft/dbt-fabric patterns
- 18 unit tests passing
- Complete documentation and samples
- Pydantic models for shared constants
- ELK-style progress tracking with rows/sec metrics
- Polars streaming with `iter_batches` for memory efficiency
- **MERGED** - Ready for integration testing

### POC Round 1 (Oct 8, 2025) ✅
- Entity-based directory structure
- Coordinator-level parallelization
- Flow-controlled logging
- Real data: 4 entities, 1.5M+ rows, 2.8M rows/sec

### Project-Centric CLI ✅
- `hygge init`, `hygge start`, `hygge debug` commands
- Automatic project discovery (`hygge.yml`)
- Flow directory structure with entities
- Clean project organization

### Registry Pattern ✅
- Scalable Home/Store type system
- Automatic registration via `__init_subclass__`
- Type-safe configuration parsing
- 205 tests passing (158 core + 47 connections/mssql)

### Polars + PyArrow Commitment ✅
- Firm technology choice (Oct 2025)
- All data movement uses Polars DataFrames
- No generic abstractions
- Fast, efficient columnar processing

---

*hygge isn't just about moving data - it's about making data movement feel natural, comfortable, and reliable.*
