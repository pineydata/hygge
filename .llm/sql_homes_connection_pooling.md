# SQL Homes and Connection Pooling Design

**Status:** Ready to build - start small, scale up
**Version:** v0.1.x
**Date:** October 9, 2025

## Problem

Extract 200+ Salesforce tables from MS SQL Server to Parquet. Need connection pooling and efficient concurrency to avoid hitting SQL Server connection limits.

## Solution

- **pyodbc** with Azure AD authentication (proven in elk2)
- **Connection pooling** to reuse connections across tables
- **Async + thread pool** for lightweight coordination with blocking I/O
- **Entity pattern** for DRY configuration (1 flow, 200 entities)

## Build Strategy

**Start small, scale up:**
1. Build for 10 tables first
2. Test and learn
3. Scale to 70 tables
4. Learn what breaks
5. Fix and scale to 200+

Don't optimize for scale until you validate the basics work.

## Key Decisions

### 1. Database-Specific Types
Use `type: mssql`, `type: postgres`, etc. (not generic "sql"). Different databases have different drivers and auth requirements.

### 2. Connection Pooling Day-1
Build pooling in v0.1.x to avoid hitting SQL Server connection limits. Use `asyncio.Queue` for simple implementation. Add health checks later when needed.

### 3. Use `pool_size` Configuration
```yaml
connections:
  salesforce_db:
    type: mssql
    server: ${SQL_SERVER}
    database: ${SQL_DATABASE}
    pool_size: 8  # Not "threads"
```

### 4. Entity Pattern for Multiple Tables
One flow, many entities (DRY):
```yaml
flows:
  salesforce_extract:
    home:
      type: mssql
      connection: salesforce_db
    store:
      type: parquet
      path: data/raw
    entities:
      - Account
      - Contact
      - Opportunity
```

### 5. Pools Managed by Coordinator
Named pools shared across flows and entities. Coordinator creates pools at startup, passes to Homes/Stores.

### 6. Bidirectional Design
Build connection utilities to work for both Homes (v0.1.x) and Stores (future). Same pool serves both.

### 7. pyodbc for v0.1.x
Use pyodbc (proven in elk2, clean install). Defer turbodbc to v0.2.x due to build complexity.

### 8. Async + Thread Pool Pattern
**Critical for scale:**
```python
async def extract():
    conn = await pool.acquire()  # Async coordination

    # ALL blocking I/O wrapped in asyncio.to_thread()
    data = await asyncio.to_thread(pl.read_database, query, conn)

    await pool.release(conn)  # Async coordination
```

Why: 200 async tasks = 200KB memory vs 200 threads = 1.6GB memory. Keep event loop responsive.

## Architecture

```
src/hygge/
├── utility/connections/
│   ├── base.py      # BaseConnection interface
│   ├── mssql.py     # MssqlConnection (Azure AD, token caching)
│   └── pool.py      # ConnectionPool (asyncio.Queue)
│
├── homes/sql/
│   ├── base.py      # BaseSqlHome (shared SQL logic)
│   └── mssql.py     # MssqlHome (uses pool + pyodbc)
│
└── stores/sql/      # Future
    ├── base.py      # BaseSqlStore
    └── mssql.py     # MssqlStore (uses same pool)
```

### Key Components

**MssqlConnection** - Connection factory with Azure AD auth
- Ports proven pattern from elk2/utils/connections.py
- Token caching with 5-minute expiry buffer
- ALL pyodbc calls wrapped in `asyncio.to_thread()`

**ConnectionPool** - Simple queue-based pooling
- Pre-creates N connections at startup
- `acquire()` blocks if pool empty (asyncio.Queue handles this)
- `release()` returns connection to pool
- Add health checks later when needed

**BaseSqlHome** - Shared SQL logic for all databases
- Query building (table or custom SQL)
- Batching with Polars
- **CRITICAL:** Wrap `pl.read_database()` in `asyncio.to_thread()`

**MssqlHome** - MS SQL implementation
- Gets connection from pool (or creates dedicated for testing)
- Inherits async batching from BaseSqlHome
- Releases connection after read

**Coordinator** - Manages pool lifecycle
- Creates pools at startup
- Passes pools to Home/Store instances
- Cleans up on shutdown

## Configuration

**Single table:**
```yaml
connections:
  my_db:
    type: mssql
    server: ${SQL_SERVER}
    database: ${SQL_DATABASE}
    pool_size: 4

flows:
  users_to_parquet:
    home:
      type: mssql
      connection: my_db
      table: dbo.users
    store:
      type: parquet
      path: data/users
```

**Many tables (entity pattern):**
```yaml
connections:
  salesforce_replica:
    type: mssql
    server: ${SALESFORCE_SERVER}
    database: salesforce_replica
    pool_size: 8

flows:
  salesforce_to_lake:
    home:
      type: mssql
      connection: salesforce_replica
    store:
      type: parquet
      path: data/salesforce
    entities:
      - Account
      - Contact
      - Opportunity
      # ... more tables
```

## Implementation Plan

### v0.1.x - MSSQL Home
**Build:**
- MssqlConnection (pyodbc + Azure AD from elk2)
- ConnectionPool (asyncio.Queue)
- BaseSqlHome (shared SQL logic)
- MssqlHome (uses pool + pyodbc)
- Coordinator pool management

**Test:**
1. Unit: Token caching, pool acquire/release, query building
2. Integration: 10 tables with pool_size=3
3. Scale: 30-50 tables, verify memory usage and pooling

**Validate before release:**
- Works with Azure AD on target SQL Server
- Connection pooling reduces overhead >80%
- Memory reasonable (<500MB for 50 tables)
- Entity pattern works

### v0.2.x - Enhancements
- turbodbc support (if build issues resolved)
- PostgresHome, DuckDbHome
- Connection health checks
- Pool monitoring

### v0.3.x - SQL Stores
- MssqlStore, PostgresStore
- Reuses same connection utilities and pools

## Expected Performance

**Connection pooling saves ~80-90% of connection overhead:**
- Without pool: Every table pays connection cost
- With pool: Only initial connections pay cost, rest reuse

**Memory efficiency with async:**
- 200 threads: ~1.6GB memory
- 200 async tasks: ~200KB + thread pool = ~300MB

**Rough timing (200 tables, 1M rows each):**
- Sequential: ~3 hours
- With pool_size=8: ~25-40 minutes (8x parallelism)

These are projections. Build and measure to validate.

## Testing

**Unit:**
- Token caching and refresh
- Pool acquire/release lifecycle
- Query building (table vs custom SQL)
- Entity name substitution

**Integration:**
- 10 tables → parquet with pool_size=3
- Verify connection reuse and data integrity
- Test entity pattern (one flow, multiple entities)

**Scale:**
- 30-50 tables with pool_size=5
- Monitor memory usage (<500MB target)
- Verify no connection limit errors
- Measure pooling overhead savings

## Critical Implementation Notes

**1. Fix token cache thread-safety**
Don't use class-level `asyncio.Lock()` - it breaks with multiple event loops. Use global cache or instance-based singleton pattern.

**2. Add connection validation**
Simple queue-based pool is good start, but add basic validation before returning connections:
```python
async def acquire(self):
    conn = await self._connections.get()
    if not self._is_alive(conn):
        conn = await self.factory.get_connection()
    return conn
```

**3. Fix batching memory issue**
Don't load all batches into memory with `await asyncio.to_thread(list, _read_batches())`. This will spike memory with 200+ concurrent tables. Use true async iteration or yield batches one at a time.

**4. Wrap ALL blocking operations**
Every pyodbc and Polars operation must use `asyncio.to_thread()`:
- `pyodbc.connect()` ✓
- `credential.get_token()` ✓
- `pl.read_database()` ✓
- `pl.write_parquet()` ✓
- `conn.close()` ✓

Miss one and you block the event loop.

## Dependencies

```txt
# New for v0.1.x
pyodbc>=4.0.0
azure-identity>=1.12.0

# Already in hygge
polars>=0.19.0
pyarrow>=13.0.0
```

**System requirement:** ODBC Driver 18 for SQL Server
- macOS: `brew install msodbcsql18`
- Linux/Windows: Via package manager

## Success Criteria

v0.1.x is successful when:
- ✅ Extracts 50+ tables from SQL Server → Parquet
- ✅ pyodbc + Azure AD works reliably
- ✅ Connection pooling reduces overhead >80%
- ✅ Memory usage <500MB for 50 tables
- ✅ asyncio.to_thread() prevents event loop blocking
- ✅ Tests validate pooling and entity pattern
- ✅ Simple YAML configuration

Start with 10 tables. Then 50. Then scale up.
