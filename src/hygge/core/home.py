"""
Home provides a comfortable place for data to start its journey.
"""
import asyncio
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

import polars as pl

from hygge.utility.exceptions import HomeError
from hygge.utility.logger import get_logger


class Home:
    """
    Base class for all data homes.

    A home is where data lives before starting its journey. Each home provides
    a comfortable way to read data with built-in conveniences like:
    - Batch reading for memory efficiency
    - Progress tracking
    - Error handling
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

    async def read(self) -> AsyncIterator[pl.DataFrame]:
        """
        Read data in batches with progress tracking and error handling.

        This is the main public interface for data access. It wraps the
        underlying stream with:
        - Progress tracking
        - Error handling
        - Resource cleanup
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
            raise HomeError(f"Failed to read from {self.name}: {str(e)}")

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """
        Get data in batches from the source.

        Each home type implements its own batch reading logic with:
        - Appropriate batch sizing
        - Memory management
        - Source-specific optimizations
        """
        raise NotImplementedError(
            f"_get_batches() must be implemented in {self.__class__.__name__}"
        )

    def _log_progress(self, total_rows: int) -> None:
        """Log progress at regular intervals."""
        if total_rows % self.row_multiplier == 0:
            elapsed = asyncio.get_event_loop().time() - self.start_time
            rate = total_rows / elapsed if elapsed > 0 else 0
            self.logger.info(
                f"Read {total_rows:,} rows in {elapsed:.1f}s ({rate:.0f} rows/s)"
            )

    def _log_completion(self, total_rows: int) -> None:
        """Log completion statistics."""
        total_time = asyncio.get_event_loop().time() - self.start_time
        self.logger.info(
            f"Completed reading from {self.name}: "
            f"{total_rows:,} total rows in {total_time:.1f}s"
        )

    async def close(self) -> None:
        """Clean up when done."""
        pass

    # Path Management
    def get_data_path(self) -> Path:
        """
        Get the primary data path for this home.

        Each home type implements its own path resolution logic.
        This could be a single file, directory, or connection string.

        Returns:
            Path: The primary data location

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError(
            f"get_data_path() must be implemented in {self.__class__.__name__}"
        )