# mssql-python Migration Evaluation

**Status**: Under Evaluation
**Date**: 2025-11-XX
**Driver**: `mssql-python` v1.0 (Microsoft Official, GA November 2025)

## Executive Summary

Microsoft's official `mssql-python` driver (GA v1.0) is a promising replacement for `pyodbc`, but requires careful evaluation before migration. Additionally, recent architectural research evaluates **TDS vs ODBC paths** for SQL Server → Polars data movement.

**Important Distinction**:
- **`mssql-python`** = Microsoft's DB-API 2.0 compliant driver (likely ODBC-based, sync)
- **TDS `mssql`** = Async-native TDS protocol driver (different package, different architecture)

### `mssql-python` Key Considerations:

- ✅ **DB-API 2.0 compliant** - Should work with Polars `read_database()` without changes
- ✅ **Azure AD authentication** - Supports Microsoft Entra ID (managed identity, device code flow)
- ✅ **Cross-platform** - Windows, Linux, macOS (including Apple Silicon)
- ✅ **Built-in connection pooling** - Though hygge already has its own
- ⚠️ **No native async** - Still requires `asyncio.to_thread()` wrapper (same as pyodbc)
- ⚠️ **No bulk copy operations** - May impact `MssqlStore` write performance
- ⚠️ **Fabric compatibility issues** - DECLARE CURSOR not supported in Fabric

### Research Findings:

Recent architectural analysis confirms that **ODBC path (current `pyodbc`) is optimal for bulk data movement** workloads like hygge's, while TDS path is better suited for async-first architectures with many concurrent small queries. See "Research Synthesis" section below for details.

## Current Implementation

### Architecture
- **Connection**: `MssqlConnection` uses `pyodbc` with Azure AD token injection
- **Home**: `MssqlHome` uses `pl.read_database()` with `iter_batches=True`
- **Store**: `MssqlStore` uses `pyodbc.fast_executemany` for bulk inserts
- **Pooling**: Custom `ConnectionPool` with `asyncio.Queue`

### Key Code Paths
- `src/hygge/connections/mssql.py` - Connection factory with Azure AD
- `src/hygge/homes/mssql/home.py` - Extraction via Polars `read_database()`
- `src/hygge/stores/mssql/store.py` - Bulk writes via `fast_executemany`

## Migration Assessment

### ✅ Low Risk: Polars Compatibility

**Current Code:**
```python
for batch_df in pl.read_database(
    query,
    self._connection,  # pyodbc connection
    iter_batches=True,
    batch_size=batch_size,
):
    yield (batch_df, len(batch_df))
```

**Assessment**: Since `mssql-python` is DB-API 2.0 compliant, Polars should accept it directly. **No code changes needed** for `MssqlHome._extract_batches_sync()`.

### ⚠️ Medium Risk: Azure AD Authentication

**Current Implementation:**
- Uses `pyodbc` with `SQL_COPT_SS_ACCESS_TOKEN` (connection attribute)
- Token obtained via `DefaultAzureCredential`
- Token cached with 5-minute expiry buffer

**mssql-python Approach:**
- Supports Microsoft Entra ID authentication
- Different API for token-based auth
- Need to verify: managed identity, device code flow compatibility

**Required Changes:**
- Rewrite `MssqlConnection._build_connection_string()`
- Update `MssqlConnection._get_token()` usage
- Test all Azure AD authentication methods

### ⚠️ High Risk: Store Write Performance

**Current Implementation:**
```python
cursor.fast_executemany = True
cursor.executemany(sql, values)  # Bulk insert
```

**mssql-python Limitation:**
- **No bulk copy operations** (per early adopter reports)
- May need to fall back to row-by-row inserts or batch inserts
- Performance impact unknown - needs benchmarking

**Required Changes:**
- Rewrite `MssqlStore._insert_batch_to_temp()`
- Potentially implement chunked batch inserts
- Performance testing required

### ✅ Low Risk: Connection Pooling

**Current**: Custom `ConnectionPool` with `asyncio.Queue`
**mssql-python**: Built-in pooling available, but hygge's pooling should work with any DB-API 2.0 connection.

**Assessment**: Can keep existing pooling, or evaluate built-in pooling for future optimization.

### ⚠️ Unknown: Async Support

**Current**: Uses `asyncio.to_thread()` to wrap blocking `pyodbc` operations
**mssql-python**: No native async mentioned - likely still requires `asyncio.to_thread()` wrapper

**Assessment**: No change in async pattern needed, but no performance improvement either.

## Migration Plan (If Proceeding)

### Phase 1: Proof of Concept
1. Install `mssql-python` as optional dependency
2. Create `MssqlConnectionV2` alongside existing `MssqlConnection`
3. Test Azure AD authentication (managed identity, device code)
4. Verify Polars `read_database()` compatibility
5. Benchmark read performance vs `pyodbc`

### Phase 2: Store Write Evaluation
1. Implement `MssqlStore` write path with `mssql-python`
2. Benchmark write performance (batch inserts vs `fast_executemany`)
3. Evaluate performance impact of no bulk copy operations
4. Determine if acceptable for production

### Phase 3: Full Migration (If Phase 1 & 2 Successful)
1. Update `MssqlConnection` to use `mssql-python`
2. Update `MssqlStore` write operations
3. Update dependencies (`pyproject.toml`)
4. Comprehensive integration testing
5. Update documentation

## Open Questions

1. **Performance**: Does `mssql-python` offer measurable performance improvements over `pyodbc`?
2. **Bulk Operations**: Can we work around lack of bulk copy, or is it a blocker?
3. **Fabric Compatibility**: Are hygge users using Fabric? (DECLARE CURSOR issue)
4. **Stability**: v1.0 is new - is it production-ready for hygge's use cases?
5. **Breaking Changes**: Will migration break existing user configurations?

## Research Synthesis: SQL Server → Polars Architecture

### New Research Findings

Recent architectural research evaluates two paths for SQL Server → Polars data movement:

**Key Insight**: The research distinguishes between **TDS (async-native)** and **ODBC (bulk-optimized)** paths, each with different strengths:

#### TDS Path (`mssql` TDS Driver)
- **Strengths**: True async I/O, lightweight, cross-platform consistent, future-proof
- **Weaknesses**: Row-by-row Python objects (no columnar), requires custom columnar builder, CPU-bound conversion, **no bulk copy operations**
- **Best For**: Many concurrent small/medium queries, async-first architectures, cloud/serverless

#### ODBC Path (`pyodbc`, `turbodbc`)
- **Strengths**: Mature Arrow ecosystem, vectorized fetches, excellent for bulk extracts (10M+ rows)
- **Weaknesses**: Sync-only (needs `asyncio.to_thread()`), driver manager overhead, platform inconsistencies
- **Best For**: Large OLAP-style extracts, maximum throughput, bulk data movement

### Research Recommendation

The research recommends **TDS as primary for async workloads**, but acknowledges:
> "ODBC is better for bulk extracts where columnar efficiency matters"

**For libraries focused on bulk data movement** (like hygge), the research suggests:
- ODBC path is better aligned for large extracts
- Arrow-native support provides better performance
- Bulk copy operations are critical

## Architecture Analysis: TDS vs ODBC Paths

### Critical Distinction

**Important**: The research distinguishes between two different approaches:

1. **ODBC Path** (`pyodbc`, `turbodbc`, likely `mssql-python`)
   - DB-API 2.0 compliant
   - Mature Arrow/Polars ecosystem support
   - Excellent for bulk extracts (10M+ rows)
   - Vectorized fetches possible via `turbodbc`
   - **Sync-only** (requires `asyncio.to_thread()` wrapper)
   - **Bulk copy operations** (`fast_executemany` for writes)

2. **TDS Path** (`mssql` TDS driver)
   - **Async-native** (designed for async/await)
   - Direct TDS protocol (no driver manager)
   - Cross-platform consistency
   - **No native columnar output** (row-by-row Python objects)
   - Requires custom columnar buffer builder
   - **No bulk copy operations** (major limitation)

### Current hygge Architecture Assessment

**Current State** (`pyodbc`):
- ✅ **Extraction**: Uses Polars `read_database()` with `iter_batches=True`
  - Leverages Polars' optimized Arrow conversion
  - Efficient columnar batching
  - Wrapped in `asyncio.to_thread()` for async compatibility
- ✅ **Storage**: Uses `pyodbc.fast_executemany` for bulk inserts
  - Critical performance feature (102,400 row batches optimized for CCI)
  - Parallel writes via connection pool
- ✅ **Async Pattern**: Thread pool wrapper works well
  - Multiple concurrent extractions via `asyncio.gather()`
  - Connection pooling prevents blocking issues

### Architectural Fit Analysis

| Requirement | Current (`pyodbc`) | `mssql-python` (ODBC) | TDS (`mssql`) |
|------------|-------------------|----------------------|---------------|
| Bulk write performance | ✅ `fast_executemany` | ❓ Unknown/limited | ❌ No bulk ops |
| Polars columnar reads | ✅ Native support | ✅ DB-API 2.0 | ⚠️ Custom builder needed |
| Async concurrency | ✅ Thread pool works | ✅ Thread pool works | ✅ Native async |
| Large extracts (10M+ rows) | ✅ Excellent | ✅ Likely good | ⚠️ Row-by-row overhead |
| Azure AD auth | ✅ Token injection | ✅ Supported | ✅ Supported |
| Cross-platform | ⚠️ Driver manager issues | ✅ Better | ✅ Better |
| Production stability | ✅ Mature | ⚠️ New v1.0 | ⚠️ Unknown |

### Key Insight from Research

The research recommends **TDS as primary for async workloads**, but acknowledges:
- **ODBC is better for bulk extracts** (hygge's primary use case)
- TDS requires significant custom columnar conversion work
- For large data movement, ODBC's Arrow-native support is superior

**For hygge specifically**: ODBC path is better aligned with our needs:
- Heavy focus on bulk data movement (Home → Store)
- Polars columnar operations (leverages existing Arrow support)
- Bulk writes are critical (Store performance)

## Recommendation

**Status**: **DEFER MIGRATION - STAY WITH `pyodbc`**

### Primary Recommendation: Keep `pyodbc`

**Rationale**:
1. **Bulk write performance is critical** - `fast_executemany` is essential for `MssqlStore`
2. **Current architecture works well** - No known performance bottlenecks
3. **Polars integration is optimal** - Native Arrow support via `read_database()`
4. **Production stability** - `pyodbc` is battle-tested in hygge's use cases
5. **`mssql-python` doesn't solve async** - Still requires `asyncio.to_thread()` wrapper

### What `mssql-python` Would Provide

**Potential Benefits**:
- Better cross-platform consistency (especially macOS/Linux)
- Microsoft official support (long-term maintenance)
- Modern C++ core (potential performance improvements)

**Blockers**:
- **No confirmed bulk copy operations** - May regress write performance
- **No async advantage** - Still blocking operations
- **New and untested** - v1.0 GA is very recent

### Future Architecture Consideration: TDS Path

If we wanted to pursue **true async-native** architecture:

**When to Consider**:
- Need for many concurrent small queries (async advantage)
- Cloud/serverless deployments (no driver manager)
- Cross-platform consistency becomes critical

**Required Work**:
1. Build custom columnar buffer converter (row → Arrow)
2. Implement TDS-based connection layer
3. Rewrite Store write operations (no bulk copy fallback)
4. Extensive performance benchmarking

**Assessment**: **Not worth it for hygge's current use cases**
- Bulk extract/write patterns favor ODBC
- Custom columnar builder is significant work
- No clear performance win for our workloads

### Recommended Path Forward

**Short-term (Next 6-12 months)**:
1. ✅ **Stay with `pyodbc`** - It works, it's fast, it's stable
2. 🔍 **Monitor** `mssql-python` development:
   - Watch for bulk copy operation support
   - Monitor community feedback and benchmarks
   - Track production adoption
3. 🔧 **Address `pyodbc` pain points** if they arise:
   - Cross-platform driver installation issues
   - Specific Azure AD authentication problems
   - Performance bottlenecks in specific scenarios

**Medium-term (12-24 months)**:
1. **Re-evaluate `mssql-python`** if:
   - Bulk copy operations are confirmed/added
   - Performance benchmarks show clear wins
   - Cross-platform issues become critical
2. **Consider TDS path** only if:
   - Async-first architecture becomes a priority
   - Many concurrent small queries become common
   - Cloud/serverless deployment patterns change

**Long-term Vision**:
- **Pluggable extraction layer** (Phase 1 from research) could allow both ODBC and TDS
- Keep ODBC as default (bulk operations)
- Make TDS optional for async-heavy workloads
- This would require significant architectural refactoring

### Action Items

1. ✅ **Document this decision** - Keep `pyodbc` for now
2. 📝 **Track `mssql-python` releases** - Add to backlog/watchlist
3. 🔍 **Monitor user feedback** - Cross-platform issues, performance needs
4. 📊 **Benchmark current performance** - Establish baseline for future comparison
5. 🚫 **Don't pursue TDS path** - Not aligned with hygge's bulk data movement focus

### Conclusion

**Stay with `pyodbc`**. The research confirms that ODBC is the right path for bulk data movement workloads like hygge's. `mssql-python` may offer some benefits in the future, but currently lacks critical features (bulk copy) and doesn't solve the async problem. TDS path would require significant work for marginal benefit in our use cases.

**hygge's philosophy**: Comfort and reliability over premature optimization. `pyodbc` is comfortable, reliable, and performs well. No migration needed.

## References

- [Microsoft Announcement](https://techcommunity.microsoft.com/blog/sqlserver/announcing-general-availability-of-the-mssql-python-driver/)
- [GitHub Repository](https://github.com/microsoft/mssql-python)
- [PyPI Package](https://pypi.org/project/mssql-python/)
