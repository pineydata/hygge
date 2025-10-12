# MSSQL Store Performance & Configuration Guide

## Executive Summary

Guide for optimal MSSQL Store implementation focusing on **Clustered Columnstore Index (CCI)** and **Heap** tables - the fast-path targets for bulk loading.

**Performance Target:** 200-250x faster than naive row-by-row inserts (250k-300k rows/sec sustained)

**Core Strategy:** `fast_executemany` + parallel workers + connection pooling

---

## The Performance Stack

### Baseline (Naive Row-by-Row)
```python
for row in df.iter_rows():
    cursor.execute("INSERT INTO table VALUES (?)", row)
```
- Speed: ~1,000 rows/sec
- **Never do this**

### Level 1: fast_executemany (Single Connection)
```python
cursor.fast_executemany = True
cursor.executemany("INSERT INTO table VALUES (?)", rows)
```
- Speed: ~10,000-20,000 rows/sec
- **Speedup: 10-20x**
- Good for simple cases

### Level 2: fast_executemany + Parallel Workers (The Horizon)
```python
# Multiple connections from pool writing in parallel
async def write_batches():
    tasks = [write_batch(chunk) for chunk in chunks]
    await asyncio.gather(*tasks)
```
- Speed: ~40,000-60,000 rows/sec (rowstore with indexes)
- Speed: ~250,000-300,000 rows/sec (CCI/Heap)
- **Speedup: 200-250x** (CCI/Heap)
- **This is the hygge approach - reliable, fast, no special permissions needed**

---

## Configuration Deep Dive

### 1. Batch Size: The Magic Number 102,400

```yaml
batch_size: 102400  # 102,400 rows per batch
```

**Why 102,400?**

**For Clustered Columnstore:**
- SQL Server optimization threshold for direct-to-compressed rowgroups
- < 102,400: Goes to delta store (slower, fragmented)
- ≥ 102,400: Goes directly to compressed rowgroups (optimal)

**For Heap:**
- Large enough to amortize network/transaction overhead
- Efficient use of memory and network
- Good balance for reliability (not too large to retry)

**Memory Impact (Salesforce Tables):**
- Salesforce standard objects: 100-200 columns, ~400 bytes avg per row
- 102,400 rows * 400 bytes = ~40MB per batch
- 8 workers * 40MB = **~320MB peak memory** (comfortable for modern systems)
- Custom objects with many fields: up to 50-60MB per batch
- Total working set: ~500MB with 8 workers (acceptable)

**Don't Go Smaller:**
- < 102,400 on CCI = delta store fragmentation
- < 10,000 = too many network round trips

**What About Small Tables?**
- **Batch size is per-batch, not total table size**
- Table with 1,000 rows? → One batch of 1,000 rows (perfectly fine!)
- Table with 50,000 rows? → One batch of 50,000 rows (still good)
- Table with 1,000,000 rows? → ~10 batches of 102,400 rows (optimal)
- **The 102,400 threshold matters for each batch written, not total data**
- Small tables complete quickly and don't hurt anything

**Could Go Larger (Edge Cases):**
- 250,000: Slightly faster, bigger retry cost
- 500,000: Diminishing returns, risky on failures

**Recommendation: 102,400 is optimal for CCI/Heap**

---

### 2. Parallel Workers vs Connection Pool

#### The Relationship

```
pool_max_size >= parallel_workers + buffer
```

**Optimal Configuration:**

```yaml
connections:
  my_db:
    type: mssql
    server: myserver.database.windows.net
    database: mydatabase
    pool:
      min_size: 2        # Warm connections ready
      max_size: 10       # 8 workers + 2 buffer
      acquire_timeout: 30
      max_lifetime: 3600  # 1 hour (Azure token refresh)
```

#### Worker Count Sweet Spots

| Table Type | Workers | Pool Max | Throughput | Notes |
|------------|---------|----------|------------|-------|
| CCI | 8 | 10 | 250k rows/sec | CCI creates separate rowgroups per worker |
| Heap | 8-12 | 14 | 300k rows/sec | No compression overhead, can push harder |
| Rowstore + indexes | 4 | 6 | 50k rows/sec | Index maintenance limits concurrency |

#### Why 8 Workers?

**SQL Server Perspective:**
- Modern SQL Server (8+ cores) handles 8 concurrent INSERTs easily
- Each CCI worker creates independent rowgroups (no contention)
- TABLOCK hint enables parallel optimization

**Reliability Perspective:**
- 8x concurrency boost without overwhelming pool
- Predictable behavior under load
- Easy to reason about failure scenarios
- Room for health checks and retries

**Diminishing Returns:**
- < 4 workers: Leaving performance on table
- 8 workers: Sweet spot
- > 12 workers: SQL Server bottlenecks, pool exhaustion risk

---

### 3. Connection Pool Configuration

#### Production Settings

```yaml
pool:
  min_size: 2              # Keep warm connections
  max_size: 10             # 8 workers + 2 buffer
  acquire_timeout: 30      # Seconds to wait for connection
  max_lifetime: 3600       # 1 hour (refresh before token expiry)
  health_check_interval: 60  # Verify connections alive
```

#### Why These Values

**min_size: 2**
- Warm connections always ready (no cold start)
- Handles health checks without blocking workers
- Small enough not to waste resources

**max_size: 10**
- 8 workers actively writing
- 1-2 for health checks, coordinator queries, retries
- SQL Server easily handles 10 concurrent sessions

**acquire_timeout: 30**
- Long enough to wait during momentary pool exhaustion
- Short enough to fail fast and trigger retry logic
- Balances patience with responsiveness

**max_lifetime: 3600 (1 hour)**
- Azure AD tokens typically valid ~1 hour
- Prevents stale connection issues
- Forces periodic refresh before problems occur
- Aligns with Azure best practices

**health_check_interval: 60**
- Catches stale connections before use
- Not so frequent it adds overhead
- Runs on idle connections in background

---

### 4. Table Hints: TABLOCK

```yaml
table_hints: "TABLOCK"
```

**What it does:**
- Table-level lock instead of row-level locks
- Enables SQL Server parallel INSERT optimizations
- Reduces locking overhead dramatically

**Performance Impact:**
- CCI + TABLOCK: 2-3x faster than without
- Heap + TABLOCK: 1.5-2x faster than without

**When to use:**
- Staging tables (exclusive access)
- Columnstore tables (loading windows)
- Heap tables (bulk loading scenarios)

**When NOT to use:**
- Production tables with concurrent readers
- Tables with active OLTP workload
- Shared tables with other writers

**Recommendation: Use for CCI/Heap in loading scenarios**

---

## The Horizon Configuration

### Complete Example

```yaml
# Connection pool (shared across flows)
connections:
  production_db:
    type: mssql
    server: myserver.database.windows.net
    database: mydatabase
    pool:
      min_size: 2
      max_size: 10
      acquire_timeout: 30
      max_lifetime: 3600
      health_check_interval: 60

# Flow with optimal settings
flows:
  fast_load:
    home:
      type: mssql
      connection: source_db
      table: dbo.SourceTable
      batch_size: 50000  # Reading is less batch-sensitive

    store:
      type: mssql
      connection: production_db
      table: dbo.DestinationCCI  # Columnstore or heap

      # Optimal performance settings
      batch_size: 102400
      parallel_workers: 8
      table_hints: "TABLOCK"

      # Reliability features
      options:
        retry_attempts: 3
        retry_backoff: exponential
        transaction_isolation: "READ_COMMITTED"
```

### Expected Performance

**Throughput:**
- 250,000-300,000 rows/sec sustained
- 200-250x faster than naive row-by-row
- 1 million rows in 3-4 seconds
- 100 million rows in 5-7 minutes

**Reliability:**
- Automatic retry on transient Azure/network failures
- Connection health checks prevent stale connections
- Batch size small enough to retry without huge cost
- Pool prevents connection exhaustion
- Graceful degradation under load

---

## Implementation Architecture

### Store Class Structure

```python
class MssqlStore(Store):
    """MS SQL Server store with parallel batch writes."""

    async def _save(self, df: pl.DataFrame, staging_path: str):
        """Main write logic."""
        # 1. Split DataFrame into batches
        chunks = self._split_into_batches(df, self.batch_size)

        # 2. Write batches in parallel with limited concurrency
        await self._write_parallel(chunks, self.parallel_workers)

    async def _write_parallel(self, chunks, max_workers):
        """Write chunks in parallel using connection pool."""
        tasks = []
        for chunk in chunks:
            task = self._write_batch(chunk)
            tasks.append(task)

            # Launch workers in waves
            if len(tasks) >= max_workers:
                await asyncio.gather(*tasks)
                tasks = []

        # Process remaining
        if tasks:
            await asyncio.gather(*tasks)

    async def _write_batch(self, df_chunk: pl.DataFrame):
        """Write single batch using pooled connection."""
        conn = await self.pool.acquire()
        try:
            # Wrap blocking pyodbc in thread
            await asyncio.to_thread(self._insert_batch, conn, df_chunk)
        finally:
            await self.pool.release(conn)

    def _insert_batch(self, conn, df_chunk: pl.DataFrame):
        """Blocking insert operation (runs in thread pool)."""
        cursor = conn.cursor()
        cursor.fast_executemany = True

        # Build INSERT statement
        placeholders = ",".join(["?"] * len(df_chunk.columns))
        sql = f"INSERT INTO {self.table}"
        if self.table_hints:
            sql += f" WITH ({self.table_hints})"
        sql += f" VALUES ({placeholders})"

        # Execute batch
        values = df_chunk.to_numpy().tolist()
        cursor.executemany(sql, values)
        conn.commit()
```

### Key Design Decisions

**Why asyncio.gather() instead of thread pool?**
- Leverages existing async architecture
- Better integration with connection pool
- Cleaner error handling and cancellation
- asyncio.to_thread() wraps blocking pyodbc operations

**Why waves of workers?**
- Prevents overwhelming connection pool
- Predictable resource usage
- Easy to reason about and debug
- Graceful handling of slow batches

**Why commit per batch?**
- Limits transaction size
- Enables progress tracking
- Faster retry on failure
- Reduces lock duration

---

## Configuration Schema

```python
class MssqlStoreConfig(StoreConfig, BaseStoreConfig):
    """Configuration for MS SQL Server store."""

    # Connection (same pattern as MssqlHome)
    connection: Optional[str] = Field(
        None,
        description="Named connection pool reference"
    )
    server: Optional[str] = Field(None)
    database: Optional[str] = Field(None)

    # Target table
    table: str = Field(..., description="Destination table name")

    # Performance settings (optimized for CCI/Heap)
    batch_size: int = Field(
        default=102_400,  # Optimal for CCI, works for all
        ge=1000,
        description="Rows per batch (reduce for heavily indexed rowstore)"
    )
    parallel_workers: int = Field(
        default=8,  # Optimal for modern SQL Server
        ge=1,
        le=16,
        description="Concurrent writers (reduce for heavily indexed rowstore)"
    )
    table_hints: Optional[str] = Field(
        default=None,
        description="Table hints (e.g., 'TABLOCK' for staging/columnstore)"
    )


    # Additional options
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional MSSQL store options"
    )
```

---

## The hygge Way

### Smart Defaults (Convention over Configuration)

```yaml
# Minimal config - optimal defaults!
store:
  type: mssql
  connection: my_db
  table: dbo.MyTable
  # Uses smart defaults: batch_size=102400, workers=8
```

### Advanced Config (Fine-tuning)

```yaml
# Add TABLOCK for staging/columnstore scenarios
store:
  type: mssql
  connection: my_db
  table: dbo.MyColumnstoreTable
  table_hints: "TABLOCK"  # Only thing you might need to add!
  # batch_size: 102400  (already the default)
  # parallel_workers: 8  (already the default)

# Or reduce for heavily indexed rowstore tables
store:
  type: mssql
  connection: my_db
  table: dbo.HeavilyIndexedTable
  batch_size: 10000      # Lower for tables with many indexes
  parallel_workers: 4     # Be gentler on indexed tables
```
