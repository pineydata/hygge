"""
Store provides a comfortable place for data to rest.
"""
import asyncio
from typing import Any, Dict, Optional

import polars as pl

from hygge.utility.settings import Settings, settings as default_settings
from hygge.utility.exceptions import StoreError
from hygge.utility.logger import get_logger
from hygge.utility.retry import with_retry


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
    async def write(
        self, df: pl.DataFrame, is_last_batch: bool = False
    ) -> Optional[str]:
        """Write data to the store.

        Args:
            df (pl.DataFrame): Data to write
            is_last_batch (bool): Whether this is the final batch from Flow

        Awaits:
            str or None: Location of staged data, or None if still collecting

        Raises:
            StoreError: If writing fails, with details about what was written
        """
        if df is None:
            raise StoreError("Cannot write None data")

        # Save original state for rollback
        original_df = self.current_df
        original_rows = self.total_rows
        staged_path = None

        try:
            # Initialize timing on first write
            if self.start_time is None:
                self.start_time = asyncio.get_event_loop().time()

            self.total_rows += len(df)
            self._log_progress(self.total_rows)

            # Handle explicit case first: df fits in a batch
            if len(df) <= self.batch_size:
                if self.current_df is not None:
                    self.logger.debug(f"Accumulating: current={len(self.current_df)}, new={len(df)}, total={len(self.current_df) + len(df)}, batch_size={self.batch_size}")
                    if len(self.current_df) + len(df) > self.batch_size:
                        try:
                            staged_path = await self._stage()
                            self.current_df = df
                            return staged_path
                        except Exception as e:
                            # Stage failed but current_df was valid
                            self.logger.error(
                                f"Failed to stage data. Current buffer preserved. Error: {str(e)}"
                            )
                            raise StoreError(
                                "Failed to stage current buffer",
                                last_staged=self.transfers[-1] if self.transfers else None,
                                current_buffer_size=len(self.current_df)
                            ) from e
                    else:
                        try:
                            self.current_df = pl.concat([self.current_df, df])
                        except Exception as e:
                            # Concat failed, restore original
                            self.current_df = original_df
                            self.total_rows = original_rows
                            raise StoreError(
                                "Failed to concatenate data",
                                last_staged=self.transfers[-1] if self.transfers else None,
                                current_buffer_size=len(original_df) if original_df is not None else 0
                            ) from e
                else:
                    self.current_df = df

                # Stage if this is the last batch
                if is_last_batch:
                    self.logger.debug(f"Last batch detected, staging {len(self.current_df)} rows")
                    try:
                        return await self._stage()
                    except Exception as e:
                        raise StoreError(
                            "Failed to stage final batch",
                            last_staged=self.transfers[-1] if self.transfers else None,
                            current_buffer_size=len(self.current_df)
                        ) from e
                self.logger.debug(f"Not last batch, accumulating {len(self.current_df)} rows")
                return None

            # Handle overflow: slice and recurse
            batch = df.slice(0, self.batch_size)
            remainder = df.slice(self.batch_size, len(df))

            try:
                self.current_df = batch
                staged_path = await self._stage()

                # Only continue to remainder if batch was successful
                return await self.write(remainder, is_last_batch)
            except Exception as e:
                # If staging failed, we can retry with the same batch
                raise StoreError(
                    "Failed to process oversized batch",
                    last_staged=self.transfers[-1] if self.transfers else None,
                    failed_batch_size=len(batch),
                    remaining_size=len(remainder)
                ) from e

        except Exception as e:
            if not isinstance(e, StoreError):
                # Wrap unknown errors with context
                raise StoreError(
                    f"Unexpected error in write: {str(e)}",
                    last_staged=self.transfers[-1] if self.transfers else None,
                    current_buffer_size=len(self.current_df) if self.current_df is not None else 0
                ) from e
            raise

        except Exception as e:
            raise StoreError(f"Failed to write to {self.name}: {str(e)}")

    async def finish(self) -> None:
        """Complete the storage process.

        Stages any remaining data, moves all staged files to their final
        locations, and resets the store state.

        Awaits:
            None

        Raises:
            StoreError: If moving files or cleanup fails
        """
        try:
            # Write any remaining data
            if self.current_df is not None and len(self.current_df) > 0:
                await self._stage()

            # Move all files to final location
            for temp_path in self.transfers:
                final_path = self._get_final_path(temp_path)
                await self._move_to_final(temp_path, final_path)

            # Reset state
            self.current_df = None
            self.transfers = []
            self.total_rows = 0
            self.start_time = None

        except Exception as e:
            raise StoreError(f"Failed to finish {self.name}: {str(e)}")

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
            file_name = await self._get_next_filename()
            temp_path = self._get_temp_path(file_name)

            # Write the data
            await self._save(self.current_df, temp_path)

            # Add write to journal
            await self._add_to_journal(temp_path)

            # Reset buffer
            self.current_df = None

            return temp_path

        except Exception as e:
            self.logger.error(f"Failed to write batch: {str(e)}")
            if temp_path:
                await self._cleanup_temp(temp_path)
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
    def _get_table_name(self, schema: str, table_name: str) -> str:
        """
        Get standardized table name format.

        This should be implemented by specific store types.
        """
        msg = "Each store type must implement _get_table_name()"
        raise NotImplementedError(msg)

    def _get_temp_directory(self, schema: str, table_name: str) -> str:
        """
        Get standardized temp directory.

        This should be implemented by specific store types.
        """
        msg = "Each store type must implement _get_temp_directory()"
        raise NotImplementedError(msg)

    def _get_final_directory(self, schema: str, table_name: str) -> str:
        """
        Get standardized final directory.

        This should be implemented by specific store types.
        """
        msg = "Each store type must implement _get_final_directory()"
        raise NotImplementedError(msg)

    def _get_temp_path(self, filename: str) -> str:
        """Get path for temporary storage using configured pattern."""
        return self.temp_pattern.format(name=self.name, filename=filename)

    def _get_final_path(self, filename: str) -> str:
        """Get path for final storage using configured pattern."""
        return self.final_pattern.format(name=self.name, filename=filename)

    async def _get_next_filename(self) -> str:
        """
        Get next filename for batch.

        This should be implemented by specific store types.
        """
        raise NotImplementedError("Each store type must implement _get_next_filename()")

    # File Operations
    @with_retry(
        timeout=300,  # 5 minutes
        retries=3,
        delay=2,
        exceptions=(StoreError, IOError, OSError),
        logger_name="hygge.store.move"
    )
    async def _move_to_final(self, temp_path: str, final_path: str) -> None:
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
    async def _cleanup_temp(self, path: str) -> None:
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
    async def _add_to_journal(self, temp_path: str) -> None:
        """Add the write operation to the journal for tracking and recovery."""
        self.transfers.append(temp_path)
        self.logger.debug(f"Journal: transfer: {temp_path}")

    def _log_progress(self, total_rows: int) -> None:
        """Log progress at regular intervals."""
        if total_rows % self.row_multiplier == 0:
            elapsed = asyncio.get_event_loop().time() - self.start_time
            rate = total_rows / elapsed if elapsed > 0 else 0
            self.logger.info(
                f"Wrote {total_rows:,} rows in {elapsed:.1f}s ({rate:.0f} rows/s)"
            )