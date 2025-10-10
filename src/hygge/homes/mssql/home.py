"""
MS SQL Server home implementation.

Reads data from MS SQL Server databases using connection pooling
and efficient batching with Polars.
"""
import asyncio
from typing import Any, AsyncIterator, Dict, Optional

import polars as pl
from pydantic import Field, model_validator

from hygge.connections import ConnectionPool, MssqlConnection
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
        try:
            # Acquire connection
            if self.pool:
                self.logger.debug(f"Acquiring connection from pool '{self.pool.name}'")
                self._connection = await self.pool.acquire()
                self._owned_connection = False
            else:
                # No pool - create dedicated connection for testing/simple cases
                self.logger.debug("Creating dedicated connection (no pool)")
                factory = MssqlConnection(
                    server=self.config.server,
                    database=self.config.database,
                    options=self.config.get_connection_options(),
                )
                self._connection = await factory.get_connection()
                self._owned_connection = True

            # Build query
            query = self._build_query()
            self.logger.debug(f"Executing query: {query[:100]}...")

            # Let Polars handle the batching efficiently
            batch_size = self.options.get("batch_size", 10_000)
            row_multiplier = self.options.get(
                "row_multiplier", 100_000
            )  # Progress logging interval
            batch_num = 0
            total_rows = 0
            start_time = asyncio.get_event_loop().time()

            self.logger.debug(
                f"Starting batched extraction with batch_size={batch_size:,}"
            )

            # Use Polars' built-in batching - much cleaner than manual OFFSET/FETCH
            for batch_df in await asyncio.to_thread(
                pl.read_database,
                query,
                self._connection,
                iter_batches=True,
                batch_size=batch_size,
            ):
                batch_rows = len(batch_df)
                batch_num += 1
                total_rows += batch_rows

                # Progress logging every N rows (like ELK)
                if total_rows % row_multiplier == 0:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    rows_per_sec = total_rows / elapsed if elapsed > 0 else 0
                    self.logger.info(
                        f"Extracted {total_rows:,} rows in {elapsed:.1f}s "
                        f"({rows_per_sec:.0f} rows/sec)"
                    )
                else:
                    self.logger.debug(f"Batch {batch_num}: {batch_rows:,} rows")

                if len(batch_df) > 0:
                    yield batch_df

            # Final stats
            if batch_num == 0:
                self.logger.warning(f"No data returned from query: {query}")
            else:
                total_time = asyncio.get_event_loop().time() - start_time
                rows_per_sec = total_rows / total_time if total_time > 0 else 0
                self.logger.success(
                    f"Completed: {total_rows:,} total rows in {total_time:.1f}s "
                    f"({rows_per_sec:.0f} rows/sec) across {batch_num} batches"
                )

        except Exception as e:
            raise HomeError(f"Failed to read from MSSQL: {str(e)}")

        finally:
            # Clean up connection
            await self._cleanup_connection()

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
        default="ODBC Driver 18 for SQL Server", description="ODBC driver name"
    )
    encrypt: str = Field(default="Yes", description="Enable encryption")
    trust_cert: str = Field(default="Yes", description="Trust server certificate")
    timeout: int = Field(default=30, ge=1, description="Connection timeout in seconds")

    # Batching
    batch_size: int = Field(
        default=10_000, ge=1, description="Number of rows to read at once"
    )
    row_multiplier: int = Field(
        default=100_000, ge=1000, description="Progress logging interval (rows)"
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
