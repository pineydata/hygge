"""
SQLite store implementation.

Uses Polars native SQLite support for efficient data writing.
Simple, file-based database store - perfect for local development
and small-scale deployments.
"""

import asyncio
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl
from pydantic import BaseModel, Field, field_validator

from hygge.core.store import Store, StoreConfig
from hygge.utility.exceptions import StoreError


class SqliteStore(Store, store_type="sqlite"):
    """
    A SQLite database store for data.

    Features:
    - Uses Polars native SQLite support (df.write_database())
    - Automatic table creation
    - Simple file-based storage
    - No connection pooling needed (SQLite is single-file)

    Example:
        ```python
        config = SqliteStoreConfig(
            path="data/database.db",
            table="users"
        )
        store = SqliteStore("users", config)
        ```
    """

    def __init__(
        self,
        name: str,
        config: "SqliteStoreConfig",
        flow_name: Optional[str] = None,
        entity_name: Optional[str] = None,
    ):
        # Get merged options from config
        merged_options = config.get_merged_options(flow_name or name)

        super().__init__(name, merged_options)
        self.config = config
        self.entity_name = entity_name

        # Build database path
        self.db_path = Path(config.path)

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Table name (can be overridden by entity_name via table substitution)
        self.table = config.table
        if entity_name and "{entity}" in self.table:
            self.table = self.table.replace("{entity}", entity_name)

        # SQLite connection string (sqlite:///path/to/database.db)
        # Use absolute path for reliability
        absolute_path = str(self.db_path.resolve())
        self.connection_string = f"sqlite:///{absolute_path}"

    async def _save(self, df: pl.DataFrame, staging_path: Optional[str] = None) -> None:
        """
        Save data to SQLite table using Polars write_database().

        Polars will automatically create the table if it doesn't exist.

        Args:
            df: Polars DataFrame to write
            staging_path: Unused for SQLite stores (database writes directly)

        Raises:
            StoreError: If write fails
        """
        try:
            # Skip empty DataFrames
            if len(df) == 0:
                self.logger.debug("Skipping empty DataFrame")
                return

            # Validate table is set
            if not self.table:
                raise StoreError(
                    f"Table name not set for store {self.name}. "
                    f"Ensure table is specified in configuration."
                )

            # Write data using Polars native SQLite support via ADBC engine
            # ADBC engine doesn't require pandas (unlike sqlalchemy engine)
            # Note: ADBC 'append' mode requires table to exist, so we try append first
            # and fall back to creating the table if it doesn't exist
            # Wrap in asyncio.to_thread since write_database is blocking
            try:
                # Try append first (for existing tables)
                await asyncio.to_thread(
                    df.write_database,
                    table_name=self.table,
                    connection=self.connection_string,
                    engine="adbc",
                    if_table_exists="append",
                )
            except Exception as append_error:
                # If table doesn't exist, create it using 'fail' mode
                # ADBC 'fail' mode creates table if it doesn't exist
                error_msg = str(append_error).lower()
                if "no such table" in error_msg:
                    # Table doesn't exist - create it
                    await asyncio.to_thread(
                        df.write_database,
                        table_name=self.table,
                        connection=self.connection_string,
                        engine="adbc",
                        if_table_exists="fail",  # Creates table if it doesn't exist
                    )
                else:
                    # Re-raise if it's a different error
                    raise

            # Log write progress using base class method with table context
            table_path = f"{self.db_path}::{self.table}"
            self._log_write_progress(len(df), path=table_path)

            self.logger.debug(
                f"Wrote {len(df):,} rows to SQLite table {self.table} "
                f"in database {self.db_path}"
            )

        except Exception as e:
            self.logger.error(f"Failed to write to SQLite table {self.table}: {str(e)}")
            raise StoreError(f"Failed to write to SQLite: {str(e)}")


class SqliteStoreConfig(BaseModel, StoreConfig, config_type="sqlite"):
    """
    Configuration for a SqliteStore.

    Example:
        ```yaml
        store:
          type: sqlite
          path: data/database.db
          table: users
          batch_size: 50000
        ```
    """

    type: str = Field(default="sqlite", description="Store type")
    path: str = Field(..., description="Path to SQLite database file")
    table: str = Field(..., description="Target table name")
    batch_size: int = Field(
        default=100_000,
        ge=1,
        description="Number of rows to accumulate before writing",
    )
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional SQLite store options"
    )

    @field_validator("path")
    @classmethod
    def validate_path(cls, v):
        """Validate path is provided."""
        if not v:
            raise ValueError("Path is required for SQLite stores")
        return v

    @field_validator("table")
    @classmethod
    def validate_table(cls, v):
        """Validate table name is provided."""
        if not v:
            raise ValueError("Table name is required for SQLite stores")
        return v

    def get_merged_options(self, flow_name: str = None) -> Dict[str, Any]:
        """Get all options including defaults."""
        options = {
            "batch_size": self.batch_size,
        }
        # Add any additional options
        options.update(self.options)
        return options
