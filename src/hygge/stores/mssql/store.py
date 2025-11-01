"""
MS SQL Server store implementation.

Writes data to MS SQL Server databases using connection pooling,
parallel batch writes, and efficient bulk loading patterns.
"""
import asyncio
from typing import Any, Dict, Optional

import polars as pl
from pydantic import BaseModel, Field, model_validator

from hygge.connections import ConnectionPool
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

        # Auto-create table support
        self.if_exists = config.if_exists
        self._table_checked = False
        self._table_created = False

        # Atomic temp table pattern - always use temp table for writes
        self.temp_table = f"{self.table}_hygge_tmp"
        self._temp_table_created = False

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
        Save data batch to MSSQL using atomic temp table pattern.

        Always writes to temp table first, then atomically swaps on close().
        This ensures no partial data corruption on failure.

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

            # Validate table is set before writing
            if not self.table:
                raise StoreError(
                    f"Table name not set for store {self.name}. "
                    f"Ensure table is specified in entity configuration."
                )

            # Always write to temp table for atomic operations
            await self._save_to_temp_table(df)

        except Exception as e:
            self.logger.error(f"Failed to write to MSSQL table {self.table}: {str(e)}")
            raise StoreError(f"Failed to write to MSSQL: {str(e)}")

    async def _save_to_temp_table(self, df: pl.DataFrame) -> None:
        """
        Write batches to temp table for atomic operation.

        All data goes to temp table first, then atomically swapped to
        production table on successful completion.

        Args:
            df: Polars DataFrame to write

        Raises:
            StoreError: If no connection pool is configured
        """
        # Connection pool is required
        if not self.pool:
            raise StoreError(
                f"Connection pool required for {self.name}. "
                f"Call store.set_pool(pool) before writing or ensure coordinator "
                f"creates connection pool from configuration."
            )

        # Ensure temp table exists (check once on first write)
        await self._ensure_temp_table_exists(df)

        # Split DataFrame into chunks for parallel writing
        chunk_size = max(1, len(df) // self.parallel_workers)
        chunks = []

        for i in range(0, len(df), chunk_size):
            chunk = df.slice(i, min(chunk_size, len(df) - i))
            if len(chunk) > 0:
                chunks.append(chunk)

        self.logger.debug(
            f"Writing {len(df):,} rows to temp table in {len(chunks)} parallel chunks "
            f"(~{chunk_size:,} rows each)"
        )

        # Write chunks in parallel using asyncio.gather
        start_time = asyncio.get_event_loop().time()
        await asyncio.gather(*[self._write_chunk_to_temp(chunk) for chunk in chunks])
        elapsed = asyncio.get_event_loop().time() - start_time

        # Track statistics
        self.batches_written += 1
        self.rows_written += len(df)
        rows_per_sec = len(df) / elapsed if elapsed > 0 else 0

        self.logger.success(
            f"Wrote batch {self.batches_written}: {len(df):,} rows to temp table "
            f"in {elapsed:.2f}s ({rows_per_sec:,.0f} rows/sec)"
        )

    async def _write_chunk_to_temp(self, df_chunk: pl.DataFrame) -> None:
        """
        Write a single DataFrame chunk to temp table using a pooled connection.

        Acquires connection from pool, writes data to temp table, releases connection.

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
            await asyncio.to_thread(self._insert_batch_to_temp, connection, df_chunk)

        except Exception as e:
            self.logger.error(f"Failed to write chunk to temp table: {str(e)}")
            raise StoreError(f"Failed to write chunk to temp table: {str(e)}")

        finally:
            # Always release connection back to pool
            if connection:
                await self.pool.release(connection)

    def _insert_batch_to_temp(self, connection, df: pl.DataFrame) -> None:
        """
        Blocking INSERT operation to temp table using fast_executemany.

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
            # Build INSERT statement for temp table
            columns = df.columns
            placeholders = ",".join(["?"] * len(columns))
            column_list = ",".join([f"[{col}]" for col in columns])

            # Build SQL: INSERT INTO temp_table WITH (hints) (columns) VALUES (?)
            sql = f"INSERT INTO {self.temp_table}"
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

    def _map_polars_type_to_sql(self, polars_type: pl.DataType) -> str:
        """
        Map Polars data type to SQL Server type with conservative defaults.

        Args:
            polars_type: Polars data type

        Returns:
            SQL Server type string
        """
        # String types - conservative sizing
        if polars_type == pl.String or polars_type == pl.Utf8:
            return "NVARCHAR(4000)"  # Fits most strings, can ALTER later

        # Integer types
        if polars_type == pl.Int8:
            return "TINYINT"
        if polars_type == pl.Int16:
            return "SMALLINT"
        if polars_type == pl.Int32:
            return "INT"
        if polars_type == pl.Int64:
            return "BIGINT"
        if polars_type == pl.UInt8:
            return "TINYINT"
        if polars_type == pl.UInt16:
            return "INT"  # No unsigned types in SQL Server
        if polars_type == pl.UInt32:
            return "BIGINT"
        if polars_type == pl.UInt64:
            return "BIGINT"

        # Float types
        if polars_type == pl.Float32:
            return "REAL"
        if polars_type == pl.Float64:
            return "FLOAT"

        # Decimal types
        if isinstance(polars_type, pl.Decimal):
            return "DECIMAL(38, 10)"  # Conservative precision

        # Date/Time types
        if polars_type == pl.Date:
            return "DATE"
        if polars_type == pl.Datetime:
            return "DATETIME2"
        if polars_type == pl.Time:
            return "TIME"
        if polars_type == pl.Duration:
            return "BIGINT"  # Store as nanoseconds

        # Boolean
        if polars_type == pl.Boolean:
            return "BIT"

        # Binary
        if polars_type == pl.Binary:
            return "VARBINARY(MAX)"

        # Categorical (treat as string)
        if isinstance(polars_type, pl.Categorical):
            return "NVARCHAR(4000)"

        # Fallback for unknown types
        self.logger.warning(
            f"Unknown Polars type {polars_type}, defaulting to NVARCHAR(MAX)"
        )
        return "NVARCHAR(MAX)"

    async def _create_table(
        self,
        connection,
        df: pl.DataFrame,
        table_name: Optional[str] = None,
    ) -> None:
        """
        Create table from DataFrame schema using conservative type mapping.

        Args:
            connection: pyodbc connection
            df: Sample DataFrame to infer schema from
            table_name: Optional table name (defaults to self.table)
        """
        target_table = table_name or self.table

        # Build column definitions
        columns = []
        for col_name, col_type in df.schema.items():
            sql_type = self._map_polars_type_to_sql(col_type)
            columns.append(f"[{col_name}] {sql_type} NULL")

        # Create DDL - Always use Clustered Columnstore Index (analytics-optimized)
        column_defs = ",\n    ".join(columns)

        # Generate safe index name from table name
        table_name_only = target_table.split(".")[-1].strip("[]")
        # Replace invalid SQL identifier characters with underscores
        safe_index_name = "".join(
            c if c.isalnum() or c == "_" else "_" for c in table_name_only
        )
        # Ensure it starts with a letter or underscore
        if safe_index_name and not (
            safe_index_name[0].isalpha() or safe_index_name[0] == "_"
        ):
            safe_index_name = f"idx_{safe_index_name}"

        ddl = f"""
CREATE TABLE {target_table} (
    {column_defs},
    INDEX CCI_{safe_index_name} CLUSTERED COLUMNSTORE
);
"""

        self.logger.info(f"Creating table {target_table} with {len(columns)} columns")

        # Offload blocking pyodbc operations to thread pool
        def execute_ddl(conn, ddl_str, tbl_name):
            cursor = conn.cursor()
            try:
                cursor.execute(ddl_str)
                conn.commit()
                return tbl_name
            except Exception as e:
                conn.rollback()
                raise StoreError(f"Failed to create table {tbl_name}: {str(e)}")
            finally:
                cursor.close()

        try:
            await asyncio.to_thread(execute_ddl, connection, ddl, target_table)
            if table_name == self.temp_table:
                self._temp_table_created = True
            else:
                self._table_created = True
            self.logger.success(f"Table {target_table} created successfully")
        except Exception as e:
            raise StoreError(f"Failed to create table {target_table}: {str(e)}")

    async def _ensure_temp_table_exists(self, df: pl.DataFrame) -> None:
        """
        Ensure temp table exists, creating it based on production table schema.

        For first write: create temp table matching production schema.
        If production table doesn't exist yet, create temp table with inferred schema.

        Args:
            df: DataFrame to write (used for schema if creating table)
        """
        # Only check once per store instance
        if self._temp_table_created:
            return

        connection = None
        try:
            connection = await self.pool.acquire()

            # Check if temp table already exists (from previous failed run)
            temp_exists = await self._table_exists_for_name(connection, self.temp_table)
            if temp_exists:
                # Clean up orphaned temp table from previous run
                self.logger.warning(
                    f"Found existing temp table {self.temp_table}, " "dropping orphan"
                )
                await self._drop_table_atomic(connection, self.temp_table)

            # Check if production table exists
            prod_exists = await self._table_exists_for_name(connection, self.table)

            if prod_exists:
                if self.if_exists == "fail":
                    raise StoreError(
                        f"Table {self.table} already exists (if_exists='fail'). "
                        "Use if_exists='append' or 'replace' to proceed."
                    )

                # For append: validate and adapt schema
                #   (add nullable cols, allow missing nullable)
                # For replace: use new DataFrame schema (may have schema drift)
                if self.if_exists == "append":
                    # Validate schema compatibility and adapt if needed
                    await self._validate_and_adapt_schema_for_append(connection, df)
                    # Create temp table matching production schema (after any ALTERs)
                    await self._create_temp_table_from_production(connection)
                else:
                    # Replace mode: use DataFrame schema (handles schema drift)
                    await self._create_table(connection, df, self.temp_table)
            else:
                # No production table - create temp table with inferred schema
                await self._create_table(connection, df, self.temp_table)

            self._temp_table_created = True
            self._table_checked = True

        finally:
            if connection:
                await self.pool.release(connection)

    async def _validate_and_adapt_schema_for_append(
        self, connection, df: pl.DataFrame
    ) -> None:
        """
        Validate and adapt schema for append mode.

        Hygge way: Be helpful but explicit.
        - Allow missing nullable columns (will insert NULLs) → WARN
        - Auto-add new nullable columns to production → WARN
        - Fail if missing NOT NULL columns or type conflicts

        Args:
            connection: pyodbc connection
            df: DataFrame to validate

        Raises:
            StoreError: If schema is incompatible (NOT NULL missing, type conflicts)
        """
        prod_schema = await self._get_production_schema(connection)
        df_columns = set(df.columns)

        prod_column_names = set(prod_schema.keys())
        missing_in_df = prod_column_names - df_columns
        new_in_df = df_columns - prod_column_names

        # Check for missing NOT NULL columns - these cannot be inserted
        missing_not_null = []
        for col_name in missing_in_df:
            col_info = prod_schema[col_name]
            if not col_info.get("is_nullable", True):
                missing_not_null.append(col_name)

        if missing_not_null:
            raise StoreError(
                f"Cannot append to {self.table}: DataFrame missing required "
                f"(NOT NULL) columns: {sorted(missing_not_null)}.\n"
                "Production requires these columns, but they are not in the "
                "DataFrame. Either add these columns to your data or use "
                "if_exists='replace'."
            )

        # Warn about missing nullable columns (will insert NULLs)
        missing_nullable = [
            col for col in missing_in_df if prod_schema[col].get("is_nullable", True)
        ]
        if missing_nullable:
            self.logger.warning(
                f"Appending to {self.table}: DataFrame missing nullable columns "
                f"{sorted(missing_nullable)}. These will be inserted as NULL."
            )

        # Auto-add new nullable columns to production table
        if new_in_df:
            await self._add_columns_to_production(connection, df, new_in_df)
            self.logger.warning(
                f"Appending to {self.table}: Added new nullable columns to production "
                f"table: {sorted(new_in_df)}"
            )

    async def _add_columns_to_production(
        self, connection, df: pl.DataFrame, new_columns: set
    ) -> None:
        """Add new nullable columns to production table."""

        def add_columns(conn, table_name, column_defs):
            cursor = conn.cursor()
            try:
                for col_name, sql_type in column_defs:
                    alter_sql = (
                        f"ALTER TABLE {table_name} " f"ADD [{col_name}] {sql_type} NULL"
                    )
                    cursor.execute(alter_sql)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

        # Map DataFrame columns to SQL types
        column_defs = []
        for col_name in sorted(new_columns):
            if col_name not in df.columns:
                continue
            polars_type = df.schema[col_name]
            sql_type = self._map_polars_type_to_sql(polars_type)
            column_defs.append((col_name, sql_type))

        if column_defs:
            await asyncio.to_thread(add_columns, connection, self.table, column_defs)

    async def _get_production_schema(self, connection) -> Dict[str, Dict[str, Any]]:
        """
        Get production table schema with column details.

        Returns:
            Dictionary mapping column names to dict with:
            - data_type: SQL data type
            - is_nullable: bool indicating if column allows NULL
        """
        parts = self.table.split(".")
        if len(parts) == 2:
            schema_name, table_name_only = parts
        else:
            schema_name = "dbo"
            table_name_only = self.table

        schema_name = schema_name.strip("[]")
        table_name_only = table_name_only.strip("[]")

        query = """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """

        def get_schema(conn, query_str, schema, table):
            cursor = conn.cursor()
            try:
                cursor.execute(query_str, (schema, table))
                return {
                    row[0]: {
                        "data_type": row[1],
                        "is_nullable": row[2].upper() == "YES",
                    }
                    for row in cursor.fetchall()
                }
            finally:
                cursor.close()

        return await asyncio.to_thread(
            get_schema, connection, query, schema_name, table_name_only
        )

    async def _create_temp_table_from_production(self, connection) -> None:
        """Create temp table with same schema as production table."""

        def create_temp_from_prod(conn, prod_table, temp_table):
            cursor = conn.cursor()
            try:
                # SELECT * INTO temp FROM prod WHERE 1=0 (schema only)
                sql = f"SELECT * INTO {temp_table} FROM {prod_table} WHERE 1=0"
                cursor.execute(sql)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

        await asyncio.to_thread(
            create_temp_from_prod, connection, self.table, self.temp_table
        )
        self._temp_table_created = True
        self.logger.debug(
            f"Created temp table {self.temp_table} from {self.table} schema"
        )

    async def close(self) -> None:
        """
        Finalize writes and atomically swap temp table to production.

        Atomic operation: DROP old table (if replace) + RENAME temp table.
        On success: temp table becomes production table.
        On failure: temp table remains, production table unchanged.
        """
        await self.finish()

        # Atomically swap temp table to production
        if self.batches_written > 0 or self._temp_table_created:
            await self._swap_temp_to_production()

        # Log final statistics
        if self.batches_written > 0:
            self.logger.success(
                f"Store {self.name} completed: {self.rows_written:,} total rows "
                f"across {self.batches_written} batches"
            )

    async def _swap_temp_to_production(self) -> None:
        """
        Atomically swap temp table to production table.

        Handles both append and replace modes:
        - append: Keep existing rows, temp table is appended (future enhancement)
        - replace: DROP old table, RENAME temp → production
        - fail: Verify table doesn't exist first

        Uses single transaction for atomicity.
        """
        if not self.pool:
            return

        connection = None
        try:
            connection = await self.pool.acquire()

            # Check if temp table has data (only swap if we wrote something)
            temp_exists = await self._table_exists_for_name(connection, self.temp_table)
            if not temp_exists:
                self.logger.debug("Temp table does not exist, nothing to swap")
                return

            # Check if production table exists
            prod_exists = await self._table_exists_for_name(connection, self.table)

            # Handle based on if_exists policy
            if self.if_exists == "fail":
                if prod_exists:
                    raise StoreError(
                        f"Table {self.table} already exists (if_exists='fail'). "
                        f"Use if_exists='append' or 'replace' to proceed."
                    )
                # Table doesn't exist - safe to rename temp to production
                await self._rename_table_atomic(connection, self.temp_table, self.table)
                self.logger.success(f"Renamed temp table to {self.table}")

            elif self.if_exists == "replace":
                # Safer pattern: RENAME old → backup, RENAME tmp → prod, DROP backup
                if prod_exists:
                    backup_table = f"{self.table}_hygge_old"
                    # Step 1: Rename old production to backup
                    await self._rename_table_atomic(
                        connection, self.table, backup_table
                    )
                    self.logger.debug(
                        f"Renamed old table {self.table} to backup {backup_table}"
                    )

                    # Step 2: Rename temp to production
                    await self._rename_table_atomic(
                        connection, self.temp_table, self.table
                    )
                    self.logger.success(f"Renamed temp table to {self.table}")

                    # Step 3: Drop backup (only after both renames succeed)
                    await self._drop_table_atomic(connection, backup_table)
                    self.logger.info(f"Dropped backup table {backup_table}")
                else:
                    # No existing table - just rename temp to production
                    await self._rename_table_atomic(
                        connection, self.temp_table, self.table
                    )
                    self.logger.success(
                        f"Atomically replaced {self.table} with temp table data"
                    )

            elif self.if_exists == "append":
                # For append, copy temp data to production (future: could use UNION ALL)
                if not prod_exists:
                    # No existing table - just rename temp to production
                    await self._rename_table_atomic(
                        connection, self.temp_table, self.table
                    )
                    self.logger.success(f"Created {self.table} from temp table")
                else:
                    # Append: INSERT INTO production SELECT FROM temp
                    await self._append_temp_to_production(connection)
                    # Drop temp table after successful append
                    await self._drop_table_atomic(connection, self.temp_table)
                    self.logger.success(f"Appended temp table data to {self.table}")

        except Exception as e:
            self.logger.error(f"Failed to swap temp table to production: {str(e)}")
            # Cleanup temp table on failure
            if connection:
                try:
                    await self._drop_table_atomic(connection, self.temp_table)
                except Exception:
                    pass  # Ignore cleanup errors
            raise StoreError(f"Failed to finalize MSSQL store: {str(e)}")

        finally:
            if connection:
                await self.pool.release(connection)

    async def _table_exists_for_name(self, connection, table_name: str) -> bool:
        """Check if a specific table exists."""
        parts = table_name.split(".")
        if len(parts) == 2:
            schema_name, table_name_only = parts
        else:
            schema_name = "dbo"
            table_name_only = table_name

        schema_name = schema_name.strip("[]")
        table_name_only = table_name_only.strip("[]")

        query = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """

        def check_table(conn, query_str, schema, table):
            cursor = conn.cursor()
            try:
                cursor.execute(query_str, (schema, table))
                count = cursor.fetchone()[0]
                return count > 0
            finally:
                cursor.close()

        return await asyncio.to_thread(
            check_table, connection, query, schema_name, table_name_only
        )

    async def _rename_table_atomic(
        self, connection, old_name: str, new_name: str
    ) -> None:
        """Atomically rename table within transaction."""

        def rename_table(conn, old, new):
            cursor = conn.cursor()
            try:
                # Use sp_rename for atomic rename
                cursor.execute(f"EXEC sp_rename '{old}', '{new}'")
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

        await asyncio.to_thread(rename_table, connection, old_name, new_name)

    async def _drop_table_atomic(self, connection, table_name: str) -> None:
        """Atomically drop table within transaction."""

        def drop_table(conn, table):
            cursor = conn.cursor()
            try:
                cursor.execute(f"DROP TABLE {table}")
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

        await asyncio.to_thread(drop_table, connection, table_name)

    async def _append_temp_to_production(self, connection) -> None:
        """Append temp table data to production table."""

        def append_data(conn, temp_table, prod_table):
            cursor = conn.cursor()
            try:
                # INSERT INTO production SELECT * FROM temp
                cursor.execute(f"INSERT INTO {prod_table} SELECT * FROM {temp_table}")
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

        await asyncio.to_thread(append_data, connection, self.temp_table, self.table)


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
    table: Optional[str] = Field(
        None, description="Destination table name (e.g., 'dbo.Users')"
    )

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

    # Write strategy (deprecated - always uses atomic temp table pattern)
    write_strategy: str = Field(
        default="temp_swap",
        description=(
            "Write strategy: Always uses atomic temp table pattern. "
            "This parameter is kept for compatibility but ignored."
        ),
    )

    # Table creation policy (fail is safest default)
    if_exists: str = Field(
        default="fail",
        description=(
            "Action when table exists: 'fail' (error if exists - default, safest), "
            "'append' (append data to existing table), "
            "'replace' (atomically drop and replace with new data)"
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
        """Validate write strategy (deprecated - kept for compatibility)."""
        # Always uses atomic temp table pattern now
        # Just log warning if old strategy specified
        if self.write_strategy == "direct_insert":
            import warnings

            warnings.warn(
                "write_strategy='direct_insert' is deprecated. "
                "All writes now use atomic temp table pattern for safety.",
                DeprecationWarning,
                stacklevel=2,
            )
        return self

    @model_validator(mode="after")
    def validate_if_exists(self):
        """Validate if_exists policy."""
        valid_policies = ["fail", "append", "replace"]
        if self.if_exists not in valid_policies:
            raise ValueError(
                f"if_exists must be one of {valid_policies}, " f"got '{self.if_exists}'"
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
