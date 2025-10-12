"""
MS SQL Server store implementation.

Writes data to MS SQL Server databases using connection pooling,
parallel batch writes, and efficient bulk loading patterns.
"""
import asyncio
from typing import Any, Dict, Optional

import polars as pl
from pydantic import BaseModel, Field, model_validator

from hygge.connections import ConnectionPool, MssqlConnection
from hygge.connections.constants import (
    MSSQL_CONNECTION_DEFAULTS,
    MSSQL_STORE_BATCHING_DEFAULTS,
)
from hygge.core.store import Store, StoreConfig
from hygge.utility.exceptions import StoreError


class MssqlStore(Store, store_type="mssql"):
    """
    MS SQL Server data store with extensible write strategies.

    Current implementation: direct_insert (high-performance append)
    Future strategies: temp_swap (atomic table swap), merge (upsert pattern)




    Example:
        ```python
        config = MssqlStoreConfig(
            connection="production_db",  # Named pool
            table="dbo.StagingTable",
            table_hints="TABLOCK",  # Optional for exclusive access
            write_strategy="direct_insert"  # Default, explicit for clarity
        )
        store = MssqlStore("staging", config, pool=pool)

        async for df in home.read():
            await store.write(df)
        await store.close()
        ```
    """

    def __init__(
        self,
        name: str,
        config: "MssqlStoreConfig",
        flow_name: Optional[str] = None,
        entity_name: Optional[str] = None,
    ):
        """
        Initialize MSSQL store.

        Args:
            name: Name for this store instance
            config: MSSQL store configuration
            flow_name: Optional flow name (not used for MSSQL)
            entity_name: Optional entity name for table substitution
        """
        # Get merged options from config
        merged_options = config.get_merged_options()

        super().__init__(name, merged_options)
        self.config = config
        self.entity_name = entity_name
        self.pool = None  # Will be set by coordinator

        # Table name with entity substitution
        self.table = config.table
        if entity_name:
            self.table = self.table.replace("{entity}", entity_name)

        # Performance settings
        self.parallel_workers = merged_options.get("parallel_workers", 8)
        self.table_hints = merged_options.get("table_hints")

        # Write strategy (extensible for future temp_swap, merge, etc.)
        self.write_strategy = config.write_strategy

        # Track batch statistics
        self.batches_written = 0
        self.rows_written = 0

    def set_pool(self, pool: ConnectionPool) -> None:
        """
        Set the connection pool for this store.

        Called by coordinator after pool creation.

        Args:
            pool: Connection pool to use for database operations
        """
        self.pool = pool

    async def _save(self, df: pl.DataFrame, staging_path: Optional[str] = None) -> None:
        """
        Save data batch to MSSQL.

        Strategy depends on write_strategy configuration:
        - direct_insert: Write directly to target table (current implementation)
        - temp_swap: Write to temp table, swap at close() (future)
        - merge: Upsert/merge pattern (future)

        Args:
            df: Polars DataFrame to write
            staging_path: Unused for database stores

        Raises:
            StoreError: If write fails
        """
        try:
            if len(df) == 0:
                self.logger.debug("Skipping empty DataFrame")
                return

            # Route to appropriate strategy
            if self.write_strategy == "direct_insert":
                await self._save_direct_insert(df)
            # Future strategies would branch here:
            # elif self.write_strategy == "temp_swap":
            #     await self._save_to_temp_table(df)
            # elif self.write_strategy == "merge":
            #     await self._save_merge(df)
            else:
                raise StoreError(f"Unknown write_strategy: {self.write_strategy}")

        except Exception as e:
            self.logger.error(f"Failed to write to MSSQL table {self.table}: {str(e)}")
            raise StoreError(f"Failed to write to MSSQL: {str(e)}")

    async def _save_direct_insert(self, df: pl.DataFrame) -> None:
        """
        Direct INSERT strategy: Write batches directly to target table.

        Best for:
        - Test data loading
        - Staging tables with exclusive access
        - Append-only workflows
        - When using TABLOCK hint


        Args:
            df: Polars DataFrame to write
        """
        # Ensure we have a connection pool
        if not self.pool:
            # No pool - create dedicated connection for testing/simple cases
            self.logger.warning(
                "No connection pool configured - creating dedicated connection. "
                "For production, use connection pools in coordinator."
            )
            await self._write_single_connection(df)
            return

        # Split DataFrame into chunks for parallel writing
        chunk_size = max(1, len(df) // self.parallel_workers)
        chunks = []

        for i in range(0, len(df), chunk_size):
            chunk = df.slice(i, min(chunk_size, len(df) - i))
            if len(chunk) > 0:
                chunks.append(chunk)

        self.logger.debug(
            f"Writing {len(df):,} rows in {len(chunks)} parallel chunks "
            f"(~{chunk_size:,} rows each)"
        )

        # Write chunks in parallel using asyncio.gather
        start_time = asyncio.get_event_loop().time()
        await asyncio.gather(*[self._write_chunk(chunk) for chunk in chunks])
        elapsed = asyncio.get_event_loop().time() - start_time

        # Track statistics
        self.batches_written += 1
        self.rows_written += len(df)
        rows_per_sec = len(df) / elapsed if elapsed > 0 else 0

        self.logger.success(
            f"Wrote batch {self.batches_written}: {len(df):,} rows "
            f"in {elapsed:.2f}s ({rows_per_sec:,.0f} rows/sec)"
        )

    async def _write_chunk(self, df_chunk: pl.DataFrame) -> None:
        """
        Write a single DataFrame chunk using a pooled connection.

        Acquires connection from pool, writes data, releases connection.

        Args:
            df_chunk: Polars DataFrame chunk to write

        Raises:
            StoreError: If write fails
        """
        connection = None
        try:
            # Acquire connection from pool
            connection = await self.pool.acquire()

            # Offload synchronous pyodbc operations to thread pool
            await asyncio.to_thread(self._insert_batch, connection, df_chunk)

        except Exception as e:
            self.logger.error(f"Failed to write chunk: {str(e)}")
            raise StoreError(f"Failed to write chunk: {str(e)}")

        finally:
            # Always release connection back to pool
            if connection:
                await self.pool.release(connection)

    def _insert_batch(self, connection, df: pl.DataFrame) -> None:
        """
        Blocking INSERT operation using fast_executemany.

        This runs in a thread pool via asyncio.to_thread().

        Args:
            connection: pyodbc connection from pool
            df: Polars DataFrame to insert

        Raises:
            Exception: If INSERT fails
        """
        cursor = connection.cursor()
        cursor.fast_executemany = True

        try:
            # Build INSERT statement
            columns = df.columns
            placeholders = ",".join(["?"] * len(columns))
            column_list = ",".join([f"[{col}]" for col in columns])

            # Build SQL: INSERT INTO table WITH (hints) (columns) VALUES (?)
            sql = f"INSERT INTO {self.table}"
            if self.table_hints:
                sql += f" WITH ({self.table_hints})"
            sql += f" ({column_list}) VALUES ({placeholders})"

            # Convert DataFrame to list of tuples for executemany
            values = [tuple(row) for row in df.iter_rows()]

            # Execute batch insert
            cursor.executemany(sql, values)
            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            cursor.close()

    async def _write_single_connection(self, df: pl.DataFrame) -> None:
        """
        Write data using a single dedicated connection (no pool).

        Used for testing or simple cases without coordinator.

        Args:
            df: Polars DataFrame to write

        Raises:
            StoreError: If write fails
        """
        connection = None
        try:
            # Create dedicated connection
            self.logger.debug("Creating dedicated connection (no pool)")
            factory = MssqlConnection(
                server=self.config.server,
                database=self.config.database,
                options=self.config.get_connection_options(),
            )
            connection = await factory.get_connection()

            # Write using fast_executemany
            await asyncio.to_thread(self._insert_batch, connection, df)

            self.batches_written += 1
            self.rows_written += len(df)
            self.logger.success(f"Wrote {len(df):,} rows to {self.table}")

        except Exception as e:
            self.logger.error(f"Failed to write with dedicated connection: {str(e)}")
            raise StoreError(f"Failed to write: {str(e)}")

        finally:
            if connection:
                await asyncio.to_thread(connection.close)

    async def close(self) -> None:
        """
        Finalize writes and cleanup.

        For direct_insert: Just flush remaining data and log stats
        For temp_swap (future): Atomic swap temp table → production
        For merge (future): Finalize merge operation
        """
        await self.finish()

        # Future strategies would finalize here:
        # if self.write_strategy == "temp_swap":
        #     await self._swap_temp_to_production()
        # elif self.write_strategy == "merge":
        #     await self._finalize_merge()

        # Log final statistics
        if self.batches_written > 0:
            self.logger.success(
                f"Store {self.name} completed: {self.rows_written:,} total rows "
                f"across {self.batches_written} batches"
            )


class MssqlStoreConfig(BaseModel, StoreConfig, config_type="mssql"):
    """
    Configuration for MS SQL Server store with extensible write strategies.

    Supports both named connection pools and direct connection parameters.

    Write Strategies:
    - direct_insert: High-performance append (current, default)
    - temp_swap: Atomic table swap via temp table (future - v0.2)
    - merge: Upsert/merge pattern (future - v0.2)

    Example with named connection:
        ```yaml
        store:
          type: mssql
          connection: production_db  # References connections: section
          table: dbo.StagingTable
          table_hints: TABLOCK  # Optional for exclusive access
          write_strategy: direct_insert  # Default, explicit for clarity
        ```

    Example with direct connection:
        ```yaml
        store:
          type: mssql
          server: myserver.database.windows.net
          database: mydatabase
          table: dbo.StagingTable
          batch_size: 102400  # Optimal for CCI (default)
          parallel_workers: 8   # Optimal for modern SQL Server (default)
          write_strategy: direct_insert  # Can omit, this is default
        ```
    """

    # Store type
    type: str = Field(default="mssql", description="Store type")

    # Connection options (mutually exclusive with server/database)
    connection: Optional[str] = Field(
        None, description="Named connection pool reference"
    )

    # Direct connection parameters
    server: Optional[str] = Field(None, description="SQL Server address")
    database: Optional[str] = Field(None, description="Database name")

    # Target table
    table: str = Field(..., description="Destination table name (e.g., 'dbo.Users')")

    # Connection options
    driver: str = Field(
        default=MSSQL_CONNECTION_DEFAULTS.driver, description="ODBC driver name"
    )
    encrypt: str = Field(
        default=MSSQL_CONNECTION_DEFAULTS.encrypt, description="Enable encryption"
    )
    trust_cert: str = Field(
        default=MSSQL_CONNECTION_DEFAULTS.trust_cert,
        description="Trust server certificate",
    )
    timeout: int = Field(
        default=MSSQL_CONNECTION_DEFAULTS.timeout,
        ge=1,
        description="Connection timeout in seconds",
    )

    # Performance settings (optimized for CCI/Heap, but allow smaller for testing)
    batch_size: int = Field(
        default=MSSQL_STORE_BATCHING_DEFAULTS.batch_size,
        ge=1,
        description="Rows per batch (102,400 optimal for CCI)",
    )
    parallel_workers: int = Field(
        default=MSSQL_STORE_BATCHING_DEFAULTS.parallel_workers,
        ge=1,
        le=16,
        description="Concurrent writers (8 optimal for modern SQL Server)",
    )
    table_hints: Optional[str] = Field(
        default=None,
        description="Table hints (e.g., 'TABLOCK' for staging/columnstore)",
    )

    # Write strategy (extensible design)
    write_strategy: str = Field(
        default="direct_insert",
        description=(
            "Write strategy: 'direct_insert' (default), "
            "'temp_swap' (future), 'merge' (future)"
        ),
    )

    # Additional options
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional MSSQL store options"
    )

    @model_validator(mode="after")
    def validate_connection_params(self):
        """Validate that either connection name OR server/database is provided."""
        has_connection = self.connection is not None
        has_direct = self.server is not None and self.database is not None

        if not has_connection and not has_direct:
            raise ValueError(
                "Must provide either 'connection' (named pool) or "
                "both 'server' and 'database' (direct connection)"
            )

        if has_connection and has_direct:
            raise ValueError(
                "Cannot specify both 'connection' and 'server/database'. "
                "Use one or the other."
            )

        return self

    @model_validator(mode="after")
    def validate_write_strategy(self):
        """Validate write strategy."""
        valid_strategies = ["direct_insert"]  # Future: temp_swap, merge
        if self.write_strategy not in valid_strategies:
            raise ValueError(
                f"write_strategy must be one of {valid_strategies}, "
                f"got '{self.write_strategy}'. "
                f"Note: 'temp_swap' and 'merge' strategies are planned "
                f"for future releases."
            )
        return self

    def get_merged_options(self) -> Dict[str, Any]:
        """Get all options including defaults."""
        options = {
            "batch_size": self.batch_size,
            "parallel_workers": self.parallel_workers,
            "table_hints": self.table_hints,
        }
        options.update(self.options)
        return options

    def get_connection_options(self) -> Dict[str, Any]:
        """Get options for MssqlConnection factory."""
        return {
            "driver": self.driver,
            "encrypt": self.encrypt,
            "trust_cert": self.trust_cert,
            "timeout": self.timeout,
        }
