"""
Store provides a comfortable place for data to rest.
"""
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl

from hygge.utility.exceptions import StoreError
from hygge.utility.logger import get_logger
from hygge.utility.retry import with_retry
from hygge.utility.settings import Settings, settings as default_settings


class Store:
    """Base class for storing data.

    A store collects data and manages its journey to final storage. It handles:
    - Collecting data until ready to stage
    - Staging data safely before saving
    - Moving data to its final location
    - Tracking progress and handling errors

    Args:
        name (str): Name of the store
        options (Dict[str, Any], optional): Configuration options
            - batch_size (int): Data batch size before staging (default: 10,000)
            - row_multiplier (int): Progress logging interval (default: 300,000)

    Raises:
        StoreError: If any storage operation fails
    """

    def __init__(
        self,
        name: str,
        options: Optional[Dict[str, Any]] = None
    ):
        self.name = name
        self.options = options or {}

        # Create settings with defaults and any overrides
        self.settings = Settings(
            **{
                'paths': {
                    'temp': self.options.get(
                        'temp_pattern', default_settings.paths.temp
                    ),
                    'final': self.options.get(
                        'final_pattern', default_settings.paths.final
                    )
                },
                'batching': {
                    'size': self.options.get(
                        'batch_size', default_settings.batching.size
                    ),
                    'row_multiplier': self.options.get(
                        'row_multiplier', default_settings.batching.row_multiplier
                    )
                }
            }
        )

        # Core settings
        self.batch_size = self.settings.batching.size
        self.row_multiplier = self.settings.batching.row_multiplier

        # Path patterns
        self.temp_pattern = self.settings.paths.temp
        self.final_pattern = self.settings.paths.final
        self.start_time = None
        self.logger = get_logger(f"hygge.store.{self.__class__.__name__}")

        # Buffering state
        self.current_df = None
        self.total_rows = 0
        self.transfers = []

    # Public Interface
    async def write(self, df: pl.DataFrame, is_recursive: bool = False) -> Optional[str]:
        """Write data to the store."""
        if df is None:
            raise StoreError("Cannot write None data")

        if self.start_time is None:
            self.start_time = asyncio.get_event_loop().time()

        # Only count rows at the top level, not in recursive calls
        if not is_recursive:
            self.total_rows += len(df)
            self._log_progress(self.total_rows)

        if len(df) <= self.batch_size:
            # Accumulate data
            if self.current_df is not None:
                self.current_df = pl.concat([self.current_df, df])
            else:
                self.current_df = df
            return None
        else:
            # Slice and stage
            batch = df.slice(0, self.batch_size)
            remainder = df.slice(self.batch_size, len(df))

            self.current_df = batch
            staged_path = await self._stage()

            # Recurse on complement of slice
            if len(remainder) > 0:
                return await self.write(remainder, is_recursive=True)
            return staged_path

    async def finish(self) -> None:
        """Complete the storage process.

        Stages any remaining data, moves all staged files to their final
        locations, and resets the store state.
        """
        # Stage any remaining data
        if self.current_df is not None and len(self.current_df) > 0:
            self.logger.info(f"Staging final batch of {len(self.current_df):,} rows")
            await self._stage()

        # Move all staged files to final location
        if self.transfers:
            self.logger.start(f"Moving {len(self.transfers)} files to final location")
            for staging_path_str in self.transfers:
                staging_path = Path(staging_path_str)
                filename = staging_path.name
                final_path = self.get_final_path(filename)
                await self._move_to_final(staging_path, final_path)
            self.logger.success(f"Completed storage of {len(self.transfers)} files")

    # Core Storage Operations
    async def _stage(self) -> Optional[str]:
        """Prepare collected data for storage.

        Args:
            None - Uses internally collected data

        Awaits:
            str or None: Path to staged data if successful, None if no data to stage
        """
        if self.current_df is None or len(self.current_df) == 0:
            self.logger.debug("No data to write")
            return None

        try:
            # Get path for this batch
            filename = await self.get_next_filename()
            staging_path = self.get_staging_path(filename)

            # Write the data
            await self._save(self.current_df, staging_path)

            # Add write to journal
            await self._add_to_journal(staging_path)

            # Reset buffer
            self.current_df = None

            return str(staging_path)

        except Exception as e:
            self.logger.error(f"Failed to write batch: {str(e)}")
            if 'staging_path' in locals():
                await self._cleanup_temp(staging_path)
            raise

    @with_retry(
        timeout=300,  # 5 minutes
        retries=3,
        delay=2,
        exceptions=(StoreError, IOError, OSError),
        logger_name="hygge.store.save"
    )
    async def _save(self, df: pl.DataFrame, path: str) -> None:
        """Save data in the store's specific format.

        Each store type provides its own implementation for writing data
        (e.g., Parquet files, SQL tables). This operation is retried on failure
        as it is idempotent - writing the same data to the same path multiple
        times has the same effect as writing it once.

        Args:
            df: Data to save
            path: Where to save the data

        Awaits:
            None

        Raises:
            StoreError: If saving fails after all retries
            TimeoutError: If operation times out
        """
        raise NotImplementedError("Each store type must implement _save()")

    # Path Management
    def get_staging_directory(self) -> Path:
        """
        Get the staging directory for temporary storage.

        This is where data is written before being moved to final location.
        Each store type implements its own staging directory logic.

        Returns:
            Path: The staging directory path

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError(
            f"get_staging_directory() must be implemented in {self.__class__.__name__}"
        )

    def get_final_directory(self) -> Path:
        """
        Get the final directory for permanent storage.

        This is where data is moved after successful staging.
        Each store type implements its own final directory logic.

        Returns:
            Path: The final directory path

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError(
            f"get_final_directory() must be implemented in {self.__class__.__name__}"
        )

    def get_staging_path(self, filename: str) -> Path:
        """
        Get the full staging path for a filename.

        Args:
            filename (str): The filename to stage

        Returns:
            Path: Full staging path including directory and filename
        """
        return self.get_staging_directory() / filename

    def get_final_path(self, filename: str) -> Path:
        """
        Get the full final path for a filename.

        Args:
            filename (str): The filename for final storage

        Returns:
            Path: Full final path including directory and filename
        """
        return self.get_final_directory() / filename

    async def get_next_filename(self) -> str:
        """
        Generate the next filename for a batch.

        Each store type implements its own filename generation logic.
        This should include appropriate naming conventions and uniqueness.

        Returns:
            str: The next filename to use

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError(
            f"get_next_filename() must be implemented in {self.__class__.__name__}"
        )

    def ensure_directories_exist(self) -> None:
        """
        Ensure that both staging and final directories exist.

        Creates directories if they don't exist, with proper error handling.

        Raises:
            StoreError: If directory creation fails
        """
        try:
            staging_dir = self.get_staging_directory()
            final_dir = self.get_final_directory()

            staging_dir.mkdir(parents=True, exist_ok=True)
            final_dir.mkdir(parents=True, exist_ok=True)

            self.logger.debug(
                f"Ensured directories exist: staging={staging_dir}, final={final_dir}"
            )

        except Exception as e:
            raise StoreError(f"Failed to create directories: {str(e)}")

    def validate_paths(self) -> None:
        """
        Validate that staging and final directories are accessible.

        Checks that directories exist and are writable.

        Raises:
            StoreError: If paths are invalid or inaccessible
        """
        try:
            staging_dir = self.get_staging_directory()
            final_dir = self.get_final_directory()

            # Check staging directory
            if not staging_dir.exists():
                raise StoreError(f"Staging directory does not exist: {staging_dir}")
            if not staging_dir.is_dir():
                raise StoreError(f"Staging path is not a directory: {staging_dir}")

            # Check final directory
            if not final_dir.exists():
                raise StoreError(f"Final directory does not exist: {final_dir}")
            if not final_dir.is_dir():
                raise StoreError(f"Final path is not a directory: {final_dir}")

        except Exception as e:
            if isinstance(e, StoreError):
                raise
            raise StoreError(f"Failed to validate paths: {str(e)}")

    # File Operations
    @with_retry(
        timeout=300,  # 5 minutes
        retries=3,
        delay=2,
        exceptions=(StoreError, IOError, OSError),
        logger_name="hygge.store.move"
    )
    async def _move_to_final(self, staging_path: Path, final_path: Path) -> None:
        """
        Move file from temp to final location.

        This operation is retried on failure as it is idempotent - moving a file
        to its final location multiple times has the same effect as moving it once,
        assuming the source file exists and hasn't changed.

        This should be implemented by specific store types.

        Raises:
            StoreError: If moving fails after all retries
            TimeoutError: If operation times out
        """
        raise NotImplementedError("Each store type must implement _move_to_final()")

    @with_retry(
        timeout=60,  # 1 minute
        retries=3,
        delay=1,
        exceptions=(StoreError, IOError, OSError),
        logger_name="hygge.store.cleanup"
    )
    async def _cleanup_temp(self, path: Path) -> None:
        """
        Clean up temporary data.

        This operation is retried on failure as it is idempotent - deleting a file
        multiple times has the same effect as deleting it once.

        This should be implemented by specific store types.

        Raises:
            StoreError: If cleanup fails after all retries
            TimeoutError: If operation times out
        """
        raise NotImplementedError("Each store type must implement _cleanup_temp()")

    # Tracking and Logging
    async def _add_to_journal(self, staging_path: Path) -> None:
        """Add the write operation to the journal for tracking and recovery."""
        self.transfers.append(str(staging_path))
        self.logger.debug(f"Journal: transfer: {staging_path}")

    def _log_progress(self, total_rows: int) -> None:
        """Log progress at regular intervals."""
        if total_rows % self.row_multiplier == 0:
            elapsed = asyncio.get_event_loop().time() - self.start_time
            rate = total_rows / elapsed if elapsed > 0 else 0
            self.logger.info(
                self.logger.WRITE_TEMPLATE.format(total_rows, elapsed, rate)
            )