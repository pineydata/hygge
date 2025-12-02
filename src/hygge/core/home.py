"""
Base Home class for comfortable data reading.

A Home is where your data lives - a data source that provides data in
comfortable batches. This abstract base class defines the interface that
all Home implementations follow, ensuring consistent, reliable data access.

hygge is built on Polars + PyArrow for efficient data movement. All homes
yield Polars DataFrames for fast, columnar data processing that feels natural.

Following hygge's philosophy, Homes prioritize:
- **Comfort**: Simple, intuitive interface for reading data
- **Reliability**: Consistent batch processing, error handling, progress tracking
- **Natural flow**: Data reads smoothly in batches that feel right-sized

Example:
    ```python
    class MyHome(Home, home_type="my_type"):
        async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
            # Your implementation reads from your data source
            # and yields Polars DataFrames in comfortable batches
            pass
    ```
"""
import asyncio
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, Optional, Type, Union

import polars as pl
from pydantic import BaseModel, Field, field_validator, model_validator

from hygge.messages import get_logger


class Home(ABC):
    """
    Base class for all data homes - where your data lives.

    A Home is a data source that provides data in comfortable batches.
    This abstract base class defines the interface that all Home implementations
    follow, ensuring consistent, reliable data access across different sources.

    Following hygge's philosophy, Homes make data feel at home wherever it lives.
    They handle the complexity of reading from different sources (parquet files,
    databases, APIs) while providing a simple, consistent interface.

    Example:
        ```python
        class MyHome(Home, home_type="my_type"):
            async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
                # Your implementation reads from your data source
                # and yields Polars DataFrames in comfortable batches
                pass
        ```
    """

    _registry: Dict[str, Type["Home"]] = {}

    def __init_subclass__(cls, home_type: str = None):
        super().__init_subclass__()
        if home_type:
            cls._registry[home_type] = cls

    @classmethod
    def create(
        cls, name: str, config: "HomeConfig", entity_name: Optional[str] = None
    ) -> "Home":
        """
        Create a Home instance using the registry pattern.

        Args:
            name: Name for the home instance
            config: Home configuration
            entity_name: Optional entity name for subdirectory organization

        Returns:
            Home instance of the appropriate type

        Raises:
            ValueError: If home type is not registered
        """
        home_type = config.type
        if home_type not in cls._registry:
            raise ValueError(f"Unknown home type: {home_type}")

        # Try to pass entity_name if the home constructor accepts it
        home_class = cls._registry[home_type]
        try:
            return home_class(name, config, entity_name)
        except TypeError:
            # Fallback for homes that don't support entity_name yet
            return home_class(name, config)

    def __init__(self, name: str, options: Optional[Dict[str, Any]] = None):
        self.name = name
        self.options = options or {}
        self.batch_size = self.options.get("batch_size", 10_000)
        self.row_multiplier = self.options.get("row_multiplier", 300_000)
        self.start_time = None
        self.logger = get_logger(f"hygge.home.{self.__class__.__name__}")

    async def read(self) -> AsyncIterator[pl.DataFrame]:
        """
        Read data from this home.

        This method orchestrates the reading process, including:
        - Progress tracking
        - Error handling
        - Performance logging

        Yields:
            Polars DataFrame batches from the underlying data source
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

    @abstractmethod
    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """
        Get data batches from the underlying data source.

        This method must be implemented by subclasses to provide
        the actual data reading logic.

        Yields:
            Polars DataFrame batches from the underlying data source
        """
        pass

    def _log_progress(self, total_rows: int) -> None:
        """Log progress at regular intervals (DEBUG level)."""
        if total_rows % self.row_multiplier == 0:
            self.logger.debug(f"READ {total_rows:,} rows")

    def _log_completion(self, total_rows: int) -> None:
        """Log completion summary at DEBUG level (coordinator shows OK line)."""
        if self.start_time:
            duration = asyncio.get_event_loop().time() - self.start_time
            rate = total_rows / duration if duration > 0 else 0
            self.logger.debug(
                f"Completed reading {total_rows:,} rows in {duration:.1f}s "
                f"({rate:.0f} rows/s)"
            )


class HomeConfig(ABC):
    """Base configuration for a data home."""

    _registry: Dict[str, Type["HomeConfig"]] = {}

    def __init_subclass__(cls, config_type: str = None):
        super().__init_subclass__()
        if config_type:
            cls._registry[config_type] = cls
            # Set default type field value
            if hasattr(cls, "model_fields") and "type" in cls.model_fields:
                cls.model_fields["type"].default = config_type

    @classmethod
    def create(cls, data: Union[str, Dict]) -> "HomeConfig":
        """
        Create a HomeConfig instance using the registry pattern.

        Args:
            data: Configuration data (string path or dict)

        Returns:
            HomeConfig instance of the appropriate type

        Raises:
            ValueError: If config type is not registered or data is invalid
        """
        if isinstance(data, str):
            # Simple path - assume parquet type
            config_type = "parquet"
            config_data = {"type": config_type, "path": data}
        elif isinstance(data, dict):
            config_type = data.get("type", "parquet")  # Default to parquet
            config_data = data
        else:
            raise ValueError(f"Invalid config data type: {type(data)}")

        if config_type not in cls._registry:
            raise ValueError(f"Unknown home config type: {config_type}")

        return cls._registry[config_type](**config_data)

    @abstractmethod
    def get_merged_options(self) -> Dict[str, Any]:
        """Get all options including defaults."""
        pass


class BaseHomeConfig(BaseModel):
    """Base configuration for a data home."""

    type: str = Field(default="", description="Type of home (parquet, sql)")
    path: Optional[str] = Field(None, description="Path to data source")
    connection: Optional[str] = Field(None, description="Database connection string")
    table: Optional[str] = Field(None, description="Table name for SQL homes")
    batch_size: int = Field(
        default=10_000, ge=1, description="Number of rows to read at once"
    )
    row_multiplier: int = Field(
        default=300_000, ge=1, description="Progress logging interval"
    )
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional home-specific options"
    )

    @field_validator("type")
    @classmethod
    def validate_type(cls, v):
        """Validate home type."""
        valid_types = ["parquet", "sql", "mssql"]
        if v not in valid_types:
            raise ValueError(f"Home type must be one of {valid_types}, got '{v}'")
        return v

    @model_validator(mode="after")
    def validate_required_fields(self):
        """Validate that required fields are present based on type."""
        if self.type == "parquet":
            if self.path is None:  # Only fail if explicitly None, not empty string
                raise ValueError("Path is required for parquet homes")
        elif self.type == "sql":
            if self.connection is None:  # Only fail if explicitly None, not empty
                raise ValueError("Connection is required for SQL homes")
        return self

    def get_merged_options(self) -> Dict[str, Any]:
        """Get all options including defaults."""
        # Start with the config fields
        options = {
            "batch_size": self.batch_size,
            "row_multiplier": self.row_multiplier,
        }
        # Add any additional options
        options.update(self.options)
        return options
