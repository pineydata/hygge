"""
Base Home class and configuration for all data homes.

A Home is a data source that can provide data in batches.
This is an abstract base class that defines the interface
that all specific Home implementations must follow.

Example:
    ```python
    class MyHome(Home):
        async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
            # Implementation specific to your data source
            pass
    ```
"""
import asyncio
from typing import Any, AsyncIterator, Dict, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from hygge.utility.logger import get_logger


class Home:
    """
    Base class for all data homes.

    A Home is a data source that can provide data in batches.
    This is an abstract base class that defines the interface
    that all specific Home implementations must follow.

    Example:
        ```python
        class MyHome(Home):
            async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
                # Implementation specific to your data source
                pass
        ```
    """

    def __init__(
        self,
        name: str,
        options: Optional[Dict[str, Any]] = None
    ):
        self.name = name
        self.options = options or {}
        self.batch_size = self.options.get('batch_size', 10_000)
        self.row_multiplier = self.options.get('row_multiplier', 300_000)
        self.start_time = None
        self.logger = get_logger(f"hygge.home.{self.__class__.__name__}")

    async def read(self) -> AsyncIterator[Any]:
        """
        Read data from this home.

        This method orchestrates the reading process, including:
        - Progress tracking
        - Error handling
        - Performance logging

        Yields:
            Data batches from the underlying data source
        """
        try:
            total_rows = 0
            self.start_time = asyncio.get_event_loop().time()

            async for df in self._get_batches():
                total_rows += len(df)
                self._log_progress(total_rows)
                yield df

            self._log_completion(total_rows)

        except Exception as e:
            self.logger.error(f"Error reading from {self.name}: {str(e)}")
            raise

    async def _get_batches(self) -> AsyncIterator[Any]:
        """
        Get data batches from the underlying data source.

        This method must be implemented by subclasses to provide
        the actual data reading logic.

        Yields:
            Data batches from the underlying data source
        """
        # This is a placeholder implementation that raises NotImplementedError
        # Subclasses must override this method
        raise NotImplementedError("Subclasses must implement _get_batches")

        # This line is unreachable but makes the method a proper async generator
        # Python requires at least one yield to make it an async generator
        if False:
            yield  # type: ignore

    def _log_progress(self, total_rows: int) -> None:
        """Log progress at regular intervals."""
        if total_rows % self.row_multiplier == 0:
            duration = asyncio.get_event_loop().time() - self.start_time
            rate = total_rows / duration if duration > 0 else 0
            self.logger.info(
                f"Read {total_rows:,} rows in {duration:.1f}s "
                f"({rate:.0f} rows/s)"
            )

    def _log_completion(self, total_rows: int) -> None:
        """Log completion summary."""
        if self.start_time:
            duration = asyncio.get_event_loop().time() - self.start_time
            rate = total_rows / duration if duration > 0 else 0
            self.logger.success(
                f"Completed reading {total_rows:,} rows in {duration:.1f}s "
                f"({rate:.0f} rows/s)"
            )


class HomeConfig(BaseModel):
    """Configuration for a data home."""
    type: str = Field(..., description="Type of home (parquet, sql)")
    path: Optional[str] = Field(None, description="Path to data source")
    connection: Optional[str] = Field(None, description="Database connection string")
    table: Optional[str] = Field(None, description="Table name for SQL homes")
    batch_size: int = Field(
        default=10_000,
        ge=1,
        description="Number of rows to read at once"
    )
    row_multiplier: int = Field(
        default=300_000,
        ge=1,
        description="Progress logging interval"
    )
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional home-specific options"
    )

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        """Validate home type."""
        valid_types = ['parquet', 'sql']
        if v not in valid_types:
            raise ValueError(f"Home type must be one of {valid_types}, got '{v}'")
        return v

    @model_validator(mode='after')
    def validate_required_fields(self):
        """Validate that required fields are present based on type."""
        if self.type == 'parquet':
            if self.path is None:  # Only fail if explicitly None, not empty string
                raise ValueError("Path is required for parquet homes")
        elif self.type == 'sql':
            if self.connection is None:  # Only fail if explicitly None, not empty
                raise ValueError("Connection is required for SQL homes")
        return self

    def get_merged_options(self) -> Dict[str, Any]:
        """Get all options including defaults."""
        # Start with the config fields
        options = {
            'batch_size': self.batch_size,
            'row_multiplier': self.row_multiplier,
        }
        # Add any additional options
        options.update(self.options)
        return options
