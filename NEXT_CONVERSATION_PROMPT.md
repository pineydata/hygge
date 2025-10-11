# hygge Next Conversation Prompt

## Current Status: SQL Homes Implementation MERGED ✅

We've successfully implemented and merged MS SQL Server support with connection pooling:

- **MSSQL Home**: Full SQL Server support with Azure AD authentication
- **Connection Pooling**: asyncio.Queue-based pooling for efficient concurrent access
- **Entity Pattern**: Extract 10-200+ tables with single flow definition
- **23 Unit Tests Passing**: All connection pooling and MSSQL logic validated
- **Complete Documentation**: Samples, README updates, prerequisites guide
- **Pydantic Models**: Shared constants with validation and documentation
- **ELK-Style Progress Tracking**: Rows/sec metrics and performance monitoring
- **Polars Streaming**: Efficient `iter_batches` implementation for large tables

**Ready for integration testing!** Core implementation is complete, tested, and merged. Now ready to validate with real SQL Server.

## Next Development Phase: SQL Home Integration Testing 🧪

**Focus**: Validate MSSQL implementation with real SQL Server, then expand parquet testing

**Priority: SQL Home Integration Testing**

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

**Prerequisites:**
- ODBC Driver 18 installed (`brew install msodbcsql18`)
- Azure AD authentication configured (`az login`)
- SQL Server access with appropriate permissions

**Why This Matters:**
- Validates the core SQL extraction use case
- Proves connection pooling prevents connection exhaustion
- Establishes baseline for extracting hundreds of tables
- Real-world validation before scaling to 200+ tables

## After SQL Testing: Round 2 P2P Testing

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

## After Integration Testing: Future Enhancements

Once SQL homes are validated with real data:

**v0.2.x - Additional Databases:**
- PostgresHome with connection pooling
- DuckDbHome (if needed)
- Connection health checks and monitoring
- turbodbc support (if build issues resolved)

**v0.3.x - SQL Stores:**
- MssqlStore for SQL Server destinations
- PostgresStore
- Reuse connection pooling infrastructure

## Success Metrics

**SQL Integration Testing:**
- ✅ Single table extracts successfully
- ✅ Azure AD authentication works reliably
- ✅ Connection pooling reduces overhead >80%
- ✅ 50+ tables extract with <500MB memory
- ✅ No connection leaks or SQL Server errors
- ✅ Entity pattern creates correct directory structure

**Parquet Testing:**
- ✅ 10+ different test scenarios passing
- ✅ Error scenarios handled gracefully
- ✅ Performance benchmarks documented
- ✅ Edge cases identified and handled

---

## What We've Achieved So Far

### SQL Homes Implementation (Dec 2024) ✅
- MS SQL Server home with Azure AD authentication
- Connection pooling (asyncio.Queue-based)
- Entity pattern for 10-200+ tables
- Ported proven Microsoft/dbt-fabric patterns
- 23 unit tests passing
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
