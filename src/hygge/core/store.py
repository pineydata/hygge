"""
Base Store class and configuration for all data stores.

A Store is a data destination that can receive and persist data.
This is an abstract base class that defines the interface
that all specific Store implementations must follow.

Example:
    ```python
    class MyStore(Store):
        async def write(self, data: pl.DataFrame) -> None:
            # Implementation specific to your data destination
            pass
    ```
"""
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator

from hygge.utility.logger import get_logger


class Store:
    """
    Base class for all data stores.

    A Store is a data destination that can receive and persist data.
    This is an abstract base class that defines the interface
    that all specific Store implementations must follow.

    Example:
        ```python
        class MyStore(Store):
            async def write(self, data: pl.DataFrame) -> None:
                # Implementation specific to your data destination
                pass
        ```
    """

    def __init__(self, name: str, options: Optional[Dict[str, Any]] = None):
        self.name = name
        self.options = options or {}
        self.batch_size = self.options.get("batch_size", 100_000)
        self.row_multiplier = self.options.get("row_multiplier", 300_000)
        self.data_buffer = []
        self.buffer_size = 0
        self.current_df = None  # Current accumulated data
        self.total_rows = 0
        self.transfers = []  # Track file transfers
        self.temp_pattern = self.options.get("temp_pattern", "temp/{name}/{filename}")
        self.final_pattern = self.options.get(
            "final_pattern", "final/{name}/{filename}"
        )
        self.start_time = None
        self.logger = get_logger(f"hygge.store.{self.__class__.__name__}")

    async def write(self, data: Any) -> None:
        """
        Write data to this store.

        This method handles buffering and batch writing:
        - Accumulates data until batch_size is reached
        - Writes batches to the underlying store
        - Tracks progress and performance

        Args:
            data: Data to write (typically a polars DataFrame)
        """
        try:
            if self.start_time is None:
                self.start_time = asyncio.get_event_loop().time()

            # Handle None data
            if data is None:
                from hygge.utility.exceptions import StoreError

                raise StoreError("Cannot write None data")

            # Add data to buffer and update tracking
            self.data_buffer.append(data)
            self.buffer_size += len(data)
            self.total_rows += len(data)

            # Update current_df (accumulate or replace)
            if self.current_df is None:
                self.current_df = data
            else:
                # Combine with existing data
                import polars as pl

                if isinstance(data, pl.DataFrame) and isinstance(
                    self.current_df, pl.DataFrame
                ):
                    self.current_df = pl.concat([self.current_df, data])

            # Write if buffer is full
            result = None
            while self.buffer_size >= self.batch_size:
                result = await self._flush_buffer()

            return result

        except Exception as e:
            self.logger.error(f"Error writing to {self.name}: {str(e)}")
            raise

    async def finish(self) -> None:
        """
        Finish writing any remaining buffered data.

        This method should be called when all data has been written
        to ensure any remaining buffered data is persisted.
        """
        if self.data_buffer:
            await self._flush_buffer()

        # Move staged files to final location for file-based stores
        if hasattr(self, "_move_staged_files_to_final"):
            await self._move_staged_files_to_final()
        elif hasattr(self, "saved_paths") and hasattr(self, "_move_to_final"):
            # Move staged files to final location
            for staging_path_str in self.saved_paths:
                if staging_path_str:
                    from pathlib import Path

                    staging_path = Path(staging_path_str)
                    # Generate final path
                    if hasattr(self, "get_final_directory"):
                        final_dir = self.get_final_directory()
                        final_path = final_dir / staging_path.name
                        await self._move_to_final(staging_path, final_path)

        if self.start_time:
            duration = asyncio.get_event_loop().time() - self.start_time
            self.logger.success(f"Store {self.name} completed in {duration:.1f}s")

    async def _flush_buffer(self) -> None:
        """
        Flush one batch from the current buffer to the underlying store.

        This method must be implemented by subclasses to provide
        the actual data writing logic.
        """
        if not self.data_buffer:
            return

        try:
            # Combine all buffered data
            combined_data = self._combine_buffered_data()

            # If data exceeds batch_size, only flush one batch
            if len(combined_data) > self.batch_size:
                batch_data = combined_data.slice(0, self.batch_size)
                remaining_data = combined_data.slice(self.batch_size)

                # Update current_df with remaining data
                self.current_df = remaining_data

                # Clear buffer and add back remaining data
                self.data_buffer.clear()
                if len(remaining_data) > 0:
                    self.data_buffer.append(remaining_data)

                # Update buffer size
                self.buffer_size = len(remaining_data)

                # Use batch data for writing
                data_to_write = batch_data
            else:
                # Flush all data
                data_to_write = combined_data
                self.data_buffer.clear()
                self.buffer_size = 0
                # Only clear current_df if there's no remaining data
                if len(data_to_write) == len(combined_data):
                    self.current_df = None

            # Generate path for file-based stores
            path = None
            if hasattr(self, "get_next_filename"):
                filename = await self.get_next_filename()
                if hasattr(self, "get_staging_directory"):
                    staging_dir = self.get_staging_directory()
                    path = str(staging_dir / filename)

            # Write to underlying store
            await self._save(data_to_write, path)

            self.logger.debug(
                f"Flushed batch: {len(data_to_write):,} rows to {self.name}"
            )

            # Return path if staging occurred
            return path

        except Exception as e:
            self.logger.error(f"Failed to flush buffer for {self.name}: {str(e)}")
            raise

    def _combine_buffered_data(self) -> Any:
        """
        Combine buffered data into a single dataset.

        This method can be overridden by subclasses to provide
        custom data combination logic.

        Returns:
            Combined data ready for writing
        """
        # Default implementation assumes polars DataFrames
        if len(self.data_buffer) == 1:
            return self.data_buffer[0]
        else:
            # Combine multiple DataFrames
            import polars as pl

            return pl.concat(self.data_buffer)

    async def _save(self, data: Any, path: Optional[str] = None) -> None:
        """
        Save data to the underlying store.

        This method must be implemented by subclasses to provide
        the actual data persistence logic.

        Args:
            data: Combined data to save
            path: Optional path for the data (for file-based stores)
        """
        raise NotImplementedError("Subclasses must implement _save")

    def ensure_directories_exist(self) -> None:
        """
        Ensure that required directories exist.

        This method can be overridden by subclasses to create
        any necessary directory structure.
        """
        pass

    def get_staging_directory(self) -> "Path":
        """
        Get the staging directory for temporary files.

        This method must be implemented by subclasses for file-based stores.
        """
        raise NotImplementedError("Subclasses must implement get_staging_directory")

    def get_final_directory(self) -> "Path":
        """
        Get the final directory for completed files.

        This method must be implemented by subclasses for file-based stores.
        """
        raise NotImplementedError("Subclasses must implement get_final_directory")

    async def _stage(self) -> None:
        """
        Stage the current data buffer.

        This method is called when data needs to be staged to temporary storage.
        """
        # If no data_buffer but current_df exists, use it
        if not self.data_buffer and self.current_df is not None:
            self.data_buffer = [self.current_df]
            self.buffer_size = len(self.current_df)

        if not self.data_buffer:
            return

        combined_data = self._combine_buffered_data()

        # Generate path for file-based stores
        path = None
        if hasattr(self, "get_next_filename"):
            filename = await self.get_next_filename()
            if hasattr(self, "get_staging_directory"):
                staging_dir = self.get_staging_directory()
                path = str(staging_dir / filename)

        await self._save(combined_data, path)

        # Clear buffer after staging
        self.data_buffer.clear()
        self.buffer_size = 0
        self.current_df = None


class StoreConfig(BaseModel):
    """Configuration for a data store."""

    type: str = Field(..., description="Type of store (parquet)")
    path: str = Field(..., description="Path to destination")
    batch_size: int = Field(
        default=100_000, ge=1, description="Number of rows to accumulate before writing"
    )
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional store-specific options"
    )

    @field_validator("type")
    @classmethod
    def validate_type(cls, v):
        """Validate store type."""
        valid_types = ["parquet"]
        if v not in valid_types:
            raise ValueError(f"Store type must be one of {valid_types}, got '{v}'")
        return v

    def get_merged_options(self) -> Dict[str, Any]:
        """Get all options including defaults."""
        # Start with the config fields
        options = {
            "batch_size": self.batch_size,
        }
        # Add any additional options
        options.update(self.options)
        return options
