# ADBC Connectors - Arrow Database Connectivity

**Status:** Watching / Research
**Horizon:** 2-3 (New Homes, Future Enhancement)
**Priority:** Medium - aligns with Polars/Arrow commitment
**Effort:** Variable - depends on driver maturity

---

## What Is ADBC?

**Arrow Database Connectivity (ADBC)** is a database connectivity standard from the Apache Arrow project. It's designed as a modern replacement for ODBC/JDBC, optimized for columnar data.

| Aspect | ODBC/JDBC | ADBC |
|--------|-----------|------|
| **Data format** | Row-based | Columnar (Arrow) |
| **Conversion overhead** | High (row ↔ column transforms) | Near-zero (already in Arrow) |
| **Performance** | Baseline | 10-40× faster for analytical queries |
| **Best for** | OLTP, transactional | Analytics, bulk reads, ML pipelines |

**Key insight:** ODBC/JDBC were designed in an era when row-based data was the norm. Modern analytics tools (Polars, DuckDB, Snowflake, BigQuery) are columnar-first. ADBC eliminates the expensive row↔column conversions.

---

## Why This Matters for hygge

hygge is **built on Polars + PyArrow**. This is a firm commitment, not a suggestion:

```
Source Format → [Home] → Polars DataFrame → [Flow] → Polars DataFrame → [Store]
```

Polars uses Arrow as its memory format under the hood. ADBC returns Arrow RecordBatches natively. This means:

```
Database → [ADBC] → Arrow RecordBatch → Polars DataFrame
                    ↑ zero-copy possible
```

**With ADBC, there's no intermediate conversion** - data flows directly from database to Polars in native format. This is the most performant path possible.

---

## Current Driver Availability

### Production-Ready Python Drivers (PyPI)

| Driver | Database | Status | Notes |
|--------|----------|--------|-------|
| `adbc-driver-postgresql` | PostgreSQL | Stable | Best path for Postgres |
| `adbc-driver-sqlite` | SQLite | Stable | Good for local dev |
| `adbc-driver-flightsql` | Flight SQL servers | Stable | Generic Arrow protocol |
| `adbc-driver-snowflake` | Snowflake | Experimental | Active development |
| `adbc-driver-bigquery` | BigQuery | Preview | Google backing |

### Coming Soon (Watch List)

| Driver | Database | Status | Who's Building |
|--------|----------|--------|----------------|
| `adbc-driver-mssql` | SQL Server | In development | Columnar startup |
| MySQL driver | MySQL | In development | Columnar startup |
| Redshift driver | Redshift | In development | Columnar startup |
| Trino driver | Trino | In development | Columnar startup |

**Columnar** (a startup from Arrow contributors) is actively building drivers for SQL Server, MySQL, Redshift, and Trino. They also provide a `dbc` CLI for driver management.

---

## Where hygge Could Benefit

### Immediate Opportunity: PostgreSQL Home

If we add a PostgreSQL home, ADBC is the obvious choice:

```python
import polars as pl

# Polars supports ADBC natively via connection URI
df = pl.read_database(
    query="SELECT * FROM users",
    connection="postgresql://localhost/db"  # Uses ADBC when driver available
)
```

### Current Reality: SQL Server (MSSQL)

hygge's existing MSSQL home uses `pyodbc`. This is **still the right choice** because:
- No mature ADBC driver for SQL Server yet
- `pyodbc` is battle-tested and reliable
- We "stand on cozy shoulders" - use proven tools

**When to reconsider:** Once Columnar's SQL Server driver reaches stability, we could add an ADBC-based MSSQL home as an alternative for read-heavy workloads.

### Future Homes (When Drivers Mature)

| Home | Current Best Path | Future ADBC Path |
|------|-------------------|------------------|
| PostgreSQL (new) | N/A - not built yet | `adbc-driver-postgresql` ✅ |
| MSSQL (existing) | `pyodbc` | Columnar driver (watching) |
| BigQuery (planned) | `google-cloud-bigquery` | `adbc-driver-bigquery` (preview) |
| Snowflake (future) | Snowflake connector | `adbc-driver-snowflake` (experimental) |
| DuckDB (planned) | Native `duckdb` | Built-in ADBC support |

---

## Implementation Approach

### Standing on Cozy Shoulders

ADBC follows hygge's philosophy perfectly:

> When great tools exist, we use them. We wrap proven tools in hygge's comfortable patterns – we don't rebuild them.

ADBC drivers are the "proven tools" for columnar database access. We wrap them in hygge's Home pattern.

### Polars Integration

Polars already supports ADBC via `pl.read_database()`:

```python
import polars as pl

# Method 1: Connection URI (Polars auto-selects ADBC if available)
df = pl.read_database(
    query="SELECT * FROM users WHERE active = true",
    connection="postgresql://user:pass@localhost/db"
)

# Method 2: Explicit ADBC connection
import adbc_driver_postgresql.dbapi as pg_adbc

with pg_adbc.connect("postgresql://localhost/db") as conn:
    df = pl.read_database(
        query="SELECT * FROM users",
        connection=conn
    )
```

### Proposed Home Pattern

```python
# src/hygge/homes/postgres/home.py (future)

class PostgresHome(Home):
    """PostgreSQL home using ADBC for optimal Arrow performance."""

    async def read(self) -> pl.DataFrame:
        # ADBC provides Arrow-native path to Polars
        return pl.read_database(
            query=self._build_query(),
            connection=self.config.connection_uri
        )
```

---

## Performance Expectations

Based on published benchmarks:

| Scenario | ODBC | ADBC | Improvement |
|----------|------|------|-------------|
| TPC-H SF1 query (DuckDB) | ~28 sec | ~0.7 sec | **38×** |
| Large result set fetch | Baseline | 10-40× faster | Varies by query |
| Memory usage | Row buffers | Zero-copy possible | Significant |

**When ADBC shines:**
- Large analytical queries (millions of rows)
- Column-heavy selections (SELECT many columns)
- Batch/bulk read operations
- ETL/ELT workloads (hygge's sweet spot!)

**When ADBC doesn't matter much:**
- Small result sets (< 10K rows)
- Single-row lookups
- OLTP workloads

---

## Roadmap Integration

### Horizon 2: New Homes (Current Priority)

**GCS Home** and **BigQuery Home** are the current priority for GA360 support.

For BigQuery specifically, we should evaluate:
1. `google-cloud-bigquery` with Arrow export (proven, stable)
2. `adbc-driver-bigquery` (newer, preview status)

**Recommendation:** Start with `google-cloud-bigquery` (proven), keep ADBC option for future.

### Horizon 3: Alternative Implementations

Once ADBC drivers mature, we could offer:
- `PostgresADBCHome` as the default Postgres home
- `MSSQLADBCHome` as an alternative to pyodbc-based home
- Unified configuration with `driver: adbc` option

---

## Action Items

### Now (Watching)
- [ ] Track Columnar's SQL Server driver development
- [ ] Monitor `adbc-driver-bigquery` stability for BigQuery Home decision

### When Building New Homes
- [ ] **PostgreSQL Home**: Use ADBC from the start (`adbc-driver-postgresql`)
- [ ] **BigQuery Home**: Evaluate ADBC vs native client, choose based on stability
- [ ] **DuckDB Home**: Use native DuckDB (has built-in ADBC support)

### Future (When Drivers Mature)
- [ ] Consider `MSSQLADBCHome` as alternative to pyodbc
- [ ] Add `driver: adbc | legacy` config option for homes with multiple drivers

---

## Related Resources

**Official:**
- [Apache Arrow ADBC Specification](https://arrow.apache.org/docs/format/ADBC.html)
- [ADBC Python Documentation](https://arrow.apache.org/adbc/current/python/index.html)

**Ecosystem:**
- [DuckDB ADBC Announcement](https://duckdb.org/2023/08/04/adbc) - Performance benchmarks
- [Snowflake ADBC Support](https://medium.com/snowflake/arrow-database-connectivity-adbc-support-for-snowflake-7bfb3a2d9074)
- [Columnar.tech](https://columnar.tech) - New drivers for SQL Server, MySQL, etc.

**hygge Related:**
- [welcome-data-from-everywhere.md](welcome-data-from-everywhere.md) - Multi-cloud integration vision
- [ROADMAP.md](ROADMAP.md) - Overall project direction

---

## Summary

ADBC is the future of database connectivity for columnar workloads. It aligns perfectly with hygge's Polars + Arrow commitment.

**Current stance:** Use proven drivers where available (PostgreSQL, SQLite, DuckDB). Watch driver development for SQL Server. Don't force ADBC where drivers aren't mature.

**hygge philosophy applies:** Stand on cozy shoulders. ADBC drivers are becoming those shoulders for database connectivity.
