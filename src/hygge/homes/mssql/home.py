"""
MS SQL Server home implementation.

Reads data from MS SQL Server databases using connection pooling
and efficient batching with Polars.
"""
import asyncio
import re
from typing import Any, AsyncIterator, Dict, Optional

import polars as pl
from pydantic import Field, model_validator

from hygge.connections import (
    ConnectionPool,
    MssqlConnection,
    get_engine,
)
from hygge.connections.constants import (
    MSSQL_CONNECTION_DEFAULTS,
    MSSQL_HOME_BATCHING_DEFAULTS,
)
from hygge.core.home import BaseHomeConfig, Home, HomeConfig
from hygge.utility.exceptions import HomeError


class MssqlHome(Home, home_type="mssql"):
    """
    MS SQL Server data home.

    Features:
    - Connection pooling for efficient concurrent access
    - Azure AD authentication via MssqlConnection
    - Efficient batching with Polars
    - Support for table names or custom SQL queries
    - Entity support for parameterized queries

    Example:
        ```python
        config = MssqlHomeConfig(
            connection="salesforce_db",  # Named pool
            table="dbo.Account"
        )
        home = MssqlHome("accounts", config, pool=pool)

        async for df in home.read():
            print(f"Batch: {len(df)} rows")
        ```
    """

    _IDENTIFIER_PATTERN = re.compile(
        r"^(?:\[?[A-Za-z_][A-Za-z0-9_]*\]?)(?:\.(?:\[?[A-Za-z_][A-Za-z0-9_]*\]?))*$"
    )

    def __init__(
        self,
        name: str,
        config: "MssqlHomeConfig",
        pool: Optional[ConnectionPool] = None,
        entity_name: Optional[str] = None,
    ):
        """
        Initialize MSSQL home.

        Args:
            name: Name for this home instance
            config: MSSQL home configuration
            pool: Optional connection pool (if None, creates dedicated connection)
            entity_name: Optional entity name for table/query substitution
        """
        # Get merged options from config
        merged_options = config.get_merged_options()

        super().__init__(name, merged_options)
        self.config = config
        self.pool = pool
        self.entity_name = entity_name

        # Connection management
        self._connection = None
        self._owned_connection = False  # Track if we created the connection

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """
        Get data batches from MS SQL Server.

        Handles:
        - Connection acquisition from pool (or creates dedicated)
        - Query building (table or custom SQL)
        - Entity name substitution
        - Efficient batching with Polars
        - Connection cleanup

        Yields:
            Polars DataFrame batches
        """
        async for batch in self._stream_query(self._build_query()):
            yield batch

    async def read_with_watermark(
        self, watermark: Dict[str, Any]
    ) -> AsyncIterator[pl.DataFrame]:
        """
        Read data using watermark information for incremental loads.

        Args:
            watermark: Watermark information retrieved from the journal.
                Expected keys: watermark, watermark_type, watermark_column, primary_key
        """
        if not watermark or not watermark.get("watermark"):
            self.logger.debug(
                "Watermark information missing or empty - performing full load"
            )
            async for batch in self._get_batches():
                yield batch
            return

        if self.config.query:
            self.logger.warning(
                "Watermark-based incremental loads require table configuration. "
                "Custom query detected - performing full load."
            )
            async for batch in self._get_batches():
                yield batch
            return

        filter_clause = self._build_watermark_filter(watermark)
        if not filter_clause:
            self.logger.warning(
                "Unable to construct watermark filter - performing full load."
            )
            async for batch in self._get_batches():
                yield batch
            return

        incremental_query = self._append_filter_to_query(
            self._build_query(), filter_clause
        )
        self.logger.debug(f"Applying watermark filter: {filter_clause}")

        async for batch in self._stream_query(incremental_query):
            yield batch

    async def _stream_query(self, query: str) -> AsyncIterator[pl.DataFrame]:
        """
        Execute a query and yield batches using streaming extraction.

        Args:
            query: SQL query string to execute.
        """
        try:
            await self._acquire_connection()

            self.logger.debug(f"Executing query: {query[:150]}...")

            batch_size = self.options.get("batch_size", 25_000)
            batch_num = 0
            total_rows = 0

            self.logger.debug(
                f"Starting batched extraction with batch_size={batch_size:,}"
            )

            engine = get_engine("thread_pool")

            async for batch_df, batch_rows in engine.execute_streaming(
                self._extract_batches_sync, query, batch_size
            ):
                batch_num += 1
                total_rows += batch_rows

                if len(batch_df) > 0:
                    yield batch_df

            if batch_num == 0:
                self.logger.warning(f"No data returned from query: {query}")

        except Exception as e:
            raise HomeError(f"Failed to read from MSSQL: {str(e)}")
        finally:
            await self._cleanup_connection()

    async def _acquire_connection(self) -> None:
        """Acquire connection from pool or create dedicated connection."""
        if self._connection:
            return

        if self.pool:
            self.logger.debug(f"Acquiring connection from pool '{self.pool.name}'")
            self._connection = await self.pool.acquire()
            self._owned_connection = False
        else:
            self.logger.debug("Creating dedicated connection (no pool)")
            factory = MssqlConnection(
                server=self.config.server,
                database=self.config.database,
                options=self.config.get_connection_options(),
            )
            self._connection = await factory.get_connection()
            self._owned_connection = True

    def _extract_batches_sync(self, query: str, batch_size: int):
        """
        Synchronous generator that yields batches from the database.

        This runs entirely in a thread pool, allowing multiple extractions to run
        in parallel (one per thread) without blocking the event loop.

        Yields batches as they're extracted (not buffered), enabling true streaming
        when used with ThreadPoolEngine.execute_streaming().

        Args:
            query: SQL query to execute
            batch_size: Number of rows per batch

        Yields:
            Tuples of (batch_df, row_count) as batches are extracted
        """
        for batch_df in pl.read_database(
            query,
            self._connection,
            iter_batches=True,
            batch_size=batch_size,
        ):
            yield (batch_df, len(batch_df))

    def _build_query(self) -> str:
        """
        Build SQL query from configuration.

        Supports:
        - table: "dbo.TableName" -> SELECT * FROM dbo.TableName
        - query: "SELECT * FROM {entity}" -> substitutes entity name

        Returns:
            SQL query string
        """
        # If custom query provided, use it
        if self.config.query:
            query = self.config.query

            # Substitute entity name if provided
            if self.entity_name:
                query = query.replace("{entity}", self.entity_name)

            return query

        # Otherwise build from table name
        table = self.config.table

        # Substitute entity name in table if provided
        if self.entity_name:
            table = table.replace("{entity}", self.entity_name)

        return f"SELECT * FROM {table}"

    def _build_watermark_filter(self, watermark: Dict[str, Any]) -> Optional[str]:
        """
        Build SQL filter clause for incremental watermark loading.

        Args:
            watermark: Watermark information from journal.
        """
        watermark_value = watermark.get("watermark")
        watermark_type = watermark.get("watermark_type")
        watermark_column = watermark.get("watermark_column")
        primary_key = watermark.get("primary_key")

        if not watermark_value or not watermark_type or not watermark_column:
            return None

        safe_watermark_column = self._validate_identifier(
            watermark_column, "watermark_column"
        )
        if not safe_watermark_column:
            return None

        if watermark_type == "datetime":
            safe_value = watermark_value.replace("'", "''")
            return f"{safe_watermark_column} > '{safe_value}'"

        if watermark_type == "int":
            try:
                numeric_value = int(watermark_value)
            except ValueError:
                self.logger.warning(
                    f"Invalid integer watermark value: {watermark_value}"
                )
                return None

            safe_primary_key = (
                self._validate_identifier(primary_key, "primary_key")
                if primary_key
                else None
            )
            column = safe_primary_key or safe_watermark_column
            if not column:
                return None
            return f"{column} > {numeric_value}"

        if watermark_type == "string":
            safe_value = watermark_value.replace("'", "''")
            return f"{safe_watermark_column} > '{safe_value}'"

        self.logger.warning(f"Unsupported watermark type: {watermark_type}")
        return None

    def _validate_identifier(
        self, identifier: Optional[str], field_name: str
    ) -> Optional[str]:
        """Validate SQL identifier (column name) to prevent injection."""
        if identifier is None:
            return None

        candidate = identifier.strip()
        if not candidate:
            self.logger.warning(f"Empty {field_name} provided for watermark filter")
            return None

        if not self._IDENTIFIER_PATTERN.fullmatch(candidate):
            self.logger.warning(
                f"Unsafe {field_name} '{identifier}' detected; "
                "falling back to full data load"
            )
            return None

        return candidate

    def _append_filter_to_query(self, query: str, filter_clause: str) -> str:
        """
        Append a WHERE/AND clause to the base query safely.

        Args:
            query: Base SQL query.
            filter_clause: SQL filter to append.
        """
        stripped_query = query.strip()
        suffix = ""
        if stripped_query.endswith(";"):
            stripped_query = stripped_query[:-1]
            suffix = ";"

        if " where " in stripped_query.lower():
            return f"{stripped_query} AND {filter_clause}{suffix}"

        return f"{stripped_query} WHERE {filter_clause}{suffix}"

    async def _cleanup_connection(self) -> None:
        """Clean up connection based on ownership."""
        if self._connection is None:
            return

        try:
            if self.pool and not self._owned_connection:
                # Return to pool
                self.logger.debug(f"Releasing connection to pool '{self.pool.name}'")
                await self.pool.release(self._connection)
            elif self._owned_connection:
                # Close dedicated connection
                self.logger.debug("Closing dedicated connection")
                await asyncio.to_thread(self._connection.close)
        except Exception as e:
            self.logger.warning(f"Error cleaning up connection: {str(e)}")
        finally:
            self._connection = None


class MssqlHomeConfig(HomeConfig, BaseHomeConfig, config_type="mssql"):
    """
    Configuration for MS SQL Server home.

    Supports both named connection pools and direct connection parameters.

    Example with named connection:
        ```yaml
        home:
          type: mssql
          connection: salesforce_db  # References connections: section
          table: dbo.Account
        ```

    Example with direct connection:
        ```yaml
        home:
          type: mssql
          server: myserver.database.windows.net
          database: mydatabase
          table: dbo.Account
        ```
    """

    # Connection options (mutually exclusive with server/database)
    connection: Optional[str] = Field(
        None, description="Named connection pool reference"
    )

    # Direct connection parameters
    server: Optional[str] = Field(None, description="SQL Server address")
    database: Optional[str] = Field(None, description="Database name")

    # Query configuration
    table: Optional[str] = Field(
        None, description="Table name (e.g., 'dbo.Users' or 'dbo.{entity}')"
    )
    query: Optional[str] = Field(None, description="Custom SQL query (overrides table)")

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

    # Batching
    batch_size: int = Field(
        default=MSSQL_HOME_BATCHING_DEFAULTS.batch_size,
        ge=1,
        description="Number of rows to read at once",
    )
    row_multiplier: int = Field(
        default=MSSQL_HOME_BATCHING_DEFAULTS.row_multiplier,
        ge=1000,
        description="Progress logging interval (rows)",
    )

    # Additional options
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional MSSQL-specific options"
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
    def validate_query_params(self):
        """Validate that either table OR query is provided."""
        if not self.table and not self.query:
            raise ValueError("Must provide either 'table' or 'query'")

        return self

    def get_merged_options(self) -> Dict[str, Any]:
        """Get all options including defaults."""
        options = {
            "batch_size": self.batch_size,
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
