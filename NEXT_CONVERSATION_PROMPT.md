# hygge Next Conversation Prompt

## Current Status: MSSQL Store Implementation COMPLETE ✅

We've successfully implemented MS SQL Server STORE (write) support with parallel batch writes:

**Bidirectional SQL Connectivity:**
- **MSSQL Home**: Read FROM SQL Server (Oct 10) ✅
- **MSSQL Store**: Write TO SQL Server (Oct 11) ✅
- **Connection Pooling**: Shared across sources and destinations ✅
- **Entity Pattern**: Works for both reading and writing ✅

**MSSQL Store Features:**
- Parallel batch writes: 8 concurrent workers (optimal for modern SQL Server)
- Optimal defaults: 102,400 batch size (CCI direct-to-compressed threshold)
- Expected: 250k-300k rows/sec on CCI/Heap tables
- Extensible design: `direct_insert` (current), `temp_swap`/`merge` (future)
- Connection pooling integration with coordinator

**Architecture Improvements:**
- Separated home vs store constants (50k vs 102k batch sizes)
- Made staging directories optional (database stores don't need file staging)
- DRY helper method for pool injection (eliminated 24 lines of duplication)
- Clean, extensible write strategy pattern

**Complete Bootstrap Pattern:**
- Load test data: parquet → Azure SQL (MssqlStore)
- Test reading: Azure SQL → parquet (MssqlHome)
- Round-trip validation workflow
- Use hygge to test hygge! 🏠

## Next Development Phase: MSSQL Store Testing 🧪

**Focus**: Bootstrap test data into Azure SQL, validate round-trip

**Priority 0: MSSQL Store Testing with Azure SQL**

**Step 1: Set up Azure SQL Database**
- Create Basic or Serverless tier database
- Configure firewall rules (allow Azure services + your IP)
- Create test table: `dbo.hygge_test_roundtrip`

**Step 2: Load Test Data (Bootstrap!)**
- Use `samples/parquet_to_mssql_test.yaml` or `examples/parquet_to_mssql_example.py`
- Load parquet file into Azure SQL using MssqlStore
- Verify: Data written successfully, no connection leaks
- Measure: Throughput (target 250k+ rows/sec)

**Step 3: Full Round-Trip Test**
- Read data back: Azure SQL → parquet (MssqlHome)
- Compare: Original parquet vs round-trip parquet
- Verify: Data integrity, schema preservation, row counts match

**Step 4: Integration Test**
- Run `tests/integration/test_parquet_to_mssql_roundtrip.py`
- Validates: ParquetHome → MssqlStore → MssqlHome → ParquetStore
- Proves: Both directions work correctly

**Prerequisites:**
- Azure SQL Database created
- ODBC Driver 18 installed
- Azure AD authentication configured
- Environment variables set: `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`

**Why This Matters:**
- Validates MSSQL Store implementation with real database
- Proves bidirectional SQL connectivity works
- Establishes baseline for write performance
- Enables testing MSSQL Home by loading test data first!

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

**MSSQL Store Testing (Priority 0):**
- ✅ Test data loads successfully into Azure SQL
- ✅ Connection pooling works with parallel writes
- ✅ Achieves 250k+ rows/sec throughput
- ✅ Round-trip data integrity verified
- ✅ No connection leaks or errors

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

### MSSQL Store Implementation (Oct 11, 2025) ✅
- MS SQL Server store with parallel batch writes
- Connection pooling integration with coordinator
- Extensible write strategy design (direct_insert implemented, temp_swap/merge planned)
- Optimal defaults: 102,400 batch size, 8 workers
- Expected: 250k-300k rows/sec on CCI/Heap
- Smart constants separated (home vs store)
- Clean architecture improvements
- Complete examples, tests, and documentation
- **READY** - Ready for Azure SQL testing

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
