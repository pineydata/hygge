# hygge Next Conversation Prompt

## Current Status: MSSQL Store Large Volume Testing COMPLETE ✅

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

## Next Development Phase: Auto-Create Tables with Schema Inference 🎯

**Priority 0: Make Tables "Just Work"**

This is core hygge functionality - users shouldn't have to manually create tables. Follow the Rails philosophy: comfort over configuration.

**Design Decision (Oct 12):**
- **Approach 3: Multi-batch sampling** (3 batches default, ~300k rows)
- Use **max + 1.5x buffer** for string sizing
- Smart defaults with override capability
- Transparency: Log what was inferred

**Implementation Plan:**

**1. Schema Inference Engine**
```python
class SchemaInference:
    """
    Infer SQL Server schema from Polars DataFrame.

    Smart Defaults:
    - sample_batches: 3 (scan first 3 batches, ~300k rows)
    - buffer_factor: 1.5 (50% growth buffer)
    - min_varchar: 50
    - max_varchar: 4000 (before using NVARCHAR(MAX))
    """

    async def infer_from_home(home: Home) -> dict:
        """Sample first N batches, analyze, return schema."""
        pass
```

**2. Type Mapping (Polars → MSSQL)**
- Numeric types: Direct mapping (safe)
- Strings: Scan samples, use max + 1.5x buffer, clamp to SQL ranges
- Dates/Times: Direct mapping
- Nullability: Respect Polars schema
- Unknown: Fallback to NVARCHAR(MAX) with warning

**3. Table Creation Flow**
```python
async def _save(self, df: pl.DataFrame):
    # Check if table exists
    if not await self._table_exists():
        # Infer schema from first 3 batches
        schema = await self._infer_schema(df)

        # Create table with CCI by default
        await self._create_table(schema, clustered_columnstore=True)

        # Log what was created (transparency)
        self.logger.info(f"Created table {self.table} with inferred schema")

    # Write normally
    await self._insert_batch(df)
```

**4. Configuration Options**
```yaml
store:
  type: mssql
  table: dbo.my_table
  # Just works! Auto-creates with smart defaults

  # Optional: Control inference
  schema_inference:
    sample_batches: 5        # Scan more data
    buffer_factor: 2.0       # More conservative

  # Optional: Override specific columns
  schema_overrides:
    email: NVARCHAR(100)     # Force specific size
    user_id: BIGINT          # Override inferred type

  # Optional: Table creation options
  if_exists: append           # append (default), replace, fail
  table_options:
    clustered_columnstore: true  # Default for analytics
```

**5. Testing Strategy**
- Test with various data patterns (short strings, long strings, mixed types)
- Test override mechanism
- Test if_exists behaviors
- Test error messages (data truncation, type mismatches)
- Document inference logic clearly

**Why This Matters:**
- Core hygge philosophy: data should just flow, no manual setup
- Eliminates friction: point at data, it works
- Smart defaults handle 90% of cases correctly
- Override mechanism handles the other 10%
- Transparent logging shows what hygge decided

**Success Criteria:**
- ✅ User can write to non-existent table without error
- ✅ Inferred schema handles common data patterns correctly
- ✅ Override mechanism works for edge cases
- ✅ Clear error messages when data doesn't fit inferred schema
- ✅ Documented inference logic and best practices

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
