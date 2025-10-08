# Python Extract & Load Framework: Technology Recommendation

## Executive Summary

For a pure extract-and-load (E-L) framework in Python with minimal transformation requirements, **Polars with PyArrow backend** is the recommended approach. This combination provides the best balance of performance, developer experience, and database compatibility while maintaining memory efficiency for large datasets.

---

## Requirements

- In-memory data transfer from source to destination databases
- Minimal to no data transformation
- Support for multiple database types including MS SQL Server
- Memory-efficient handling of large datasets
- Schema validation and type handling
- Batching capabilities for large tables

---

## Evaluated Approaches

### 1. Polars with PyArrow Backend (RECOMMENDED)

**Description**: Use Polars as the primary interface with PyArrow handling the underlying memory representation.

#### Pros
- Clean, intuitive API for database operations via `read_database()` and `write_database()`
- Automatic batching with configurable batch sizes for memory management
- Built-in schema validation and type inference
- Works with any database via SQLAlchemy connection strings
- PyArrow columnar memory format provides zero-copy operations where possible
- Easy fallback to pure PyArrow when needed via `.to_arrow()` and `.from_arrow()`
- Active development and strong community support
- Excellent documentation and examples

#### Cons
- Slight API overhead compared to raw PyArrow (negligible for most use cases)
- Requires SQLAlchemy as a dependency for database connections
- May load entire result set into memory by default (mitigated with batch_size parameter)
- Less mature than some alternatives for certain edge case database types

#### Implementation Pattern
```python
import polars as pl
from sqlalchemy import create_engine

source_engine = create_engine('mssql+pyodbc://...')
dest_engine = create_engine('postgresql://...')

df = pl.read_database(
    query="SELECT * FROM source_table",
    connection=source_engine,
    batch_size=10000
)

df.write_database(
    table_name="dest_table",
    connection=dest_engine,
    if_table_exists="append"
)
```

---

### 2. DuckDB

**Description**: Use DuckDB as an in-memory analytical database for data transfer operations.

#### Pros
- Exceptional performance for analytical queries and aggregations
- Native SQL interface familiar to SQL-oriented developers
- Handles larger-than-memory datasets with automatic spill-to-disk
- Excellent integration with Arrow and Polars
- Fast `COPY` statements for bulk operations
- Can attach to PostgreSQL and MySQL via extensions

#### Cons
- **Cannot directly connect to MS SQL Server** (dealbreaker for your use case)
- Requires extraction layer with database drivers for most sources
- Overkill for pure extract-and-load without transformations
- SQL-first approach may require more code for simple operations
- Extension management adds complexity

#### Best Use Case
Consider DuckDB if you need complex analytical transformations between extract and load, or if all your sources are file-based or PostgreSQL/MySQL.

---

### 3. Pure PyArrow

**Description**: Use PyArrow directly with database-specific drivers for data transfer.

#### Pros
- Minimal dependencies and overhead
- Maximum control over memory usage and batching
- Zero-copy operations where possible
- Columnar memory format optimized for analytical workloads
- Industry standard for in-memory data representation
- Interoperable with many other tools (Polars, DuckDB, Pandas)

#### Cons
- **No native database connectors** - requires manual driver management
- Manual batching logic must be implemented
- Type mapping between database types and Arrow types is manual
- More boilerplate code for common operations
- Schema validation requires manual implementation
- Steeper learning curve for developers unfamiliar with Arrow concepts

#### Implementation Pattern
```python
import pyarrow as pa
import pyodbc

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()
cursor.execute("SELECT * FROM table")

columns = [desc[0] for desc in cursor.description]
batch_size = 10000

while True:
    rows = cursor.fetchmany(batch_size)
    if not rows:
        break

    batch = pa.RecordBatch.from_pylist(
        [dict(zip(columns, row)) for row in rows]
    )

    # Manual load logic here
    load_batch_to_destination(batch)
```

#### Best Use Case
Consider pure PyArrow if you need maximum control and minimal dependencies, or if you're building low-level infrastructure where the overhead of Polars is unacceptable.

---

### 4. Database Drivers + SQLAlchemy Only

**Description**: Use native database drivers with SQLAlchemy for connection management, without Arrow/Polars.

#### Pros
- Most straightforward for simple use cases
- Mature, well-tested libraries
- Direct support for all major databases
- Familiar to most Python developers
- Minimal abstraction layer

#### Cons
- Row-oriented processing is memory-inefficient for large datasets
- No automatic batching or streaming capabilities
- Manual schema validation required
- Less performant than columnar approaches
- Type conversions handled by driver (inconsistent across databases)
- Difficult to implement efficient large dataset transfers

#### Best Use Case
Small datasets (< 100K rows) where simplicity is more important than performance.

---

## Recommendation Matrix

| Approach | Memory Efficiency | DB Compatibility | Developer Experience | Performance | Code Maintainability |
|----------|------------------|------------------|---------------------|-------------|---------------------|
| **Polars + PyArrow** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| DuckDB | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Pure PyArrow | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| Drivers Only | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |

---

## Implementation Considerations for Polars + PyArrow

### Database Connection Strings

```python
# MS SQL Server
'mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server'

# PostgreSQL
'postgresql://user:pass@host:5432/database'

# MySQL
'mysql+pymysql://user:pass@host:3306/database'

# Oracle
'oracle+cx_oracle://user:pass@host:1521/service'
```

### Required Dependencies

```
polars[all]  # Includes database support
sqlalchemy
pyodbc  # For SQL Server
psycopg2-binary  # For PostgreSQL
pymysql  # For MySQL
cx_Oracle  # For Oracle
```

### Memory Management

```python
# For very large tables, use batch_size parameter
df = pl.read_database(
    query="SELECT * FROM large_table",
    connection=engine,
    batch_size=50000  # Adjust based on available memory
)

# Or use lazy API for query optimization
lazy_df = pl.read_database(query, connection=engine).lazy()
result = lazy_df.select(['col1', 'col2']).collect()
```

### Error Handling Patterns

```python
from sqlalchemy.exc import SQLAlchemyError
from polars.exceptions import ComputeError

try:
    df = pl.read_database(query, connection=source_engine)
    df.write_database(table_name, connection=dest_engine)
except SQLAlchemyError as e:
    # Handle database connection/query errors
    logger.error(f"Database error: {e}")
except ComputeError as e:
    # Handle Polars computation errors
    logger.error(f"Polars error: {e}")
```

---

## Future Considerations

- **Incremental loads**: Add logic for tracking last successful load timestamps
- **Connection pooling**: Implement for high-frequency loads
- **Parallel extraction**: For very large tables, consider partitioned reads
- **Schema drift detection**: Compare source and destination schemas before load
- **Data quality checks**: Validate row counts, null checks post-load

---

## Conclusion

**Polars with PyArrow backend** provides the optimal balance for a pure extract-and-load framework. It offers excellent performance through Arrow's columnar memory format, broad database compatibility via SQLAlchemy, and a developer-friendly API that reduces boilerplate code. The automatic batching and schema handling features address your core requirements while maintaining the flexibility to drop down to PyArrow when needed.

While DuckDB offers superior performance for analytical workloads, its inability to connect directly to MS SQL Server makes it unsuitable for your multi-database requirements. Pure PyArrow provides maximum control but requires significantly more implementation effort without meaningful benefits for your use case.
