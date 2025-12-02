"""
Parquet file home implementation.
"""
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

import polars as pl
from pydantic import Field, field_validator

from hygge.core.home import BaseHomeConfig, Home, HomeConfig
from hygge.utility.exceptions import HomeError, HomeReadError
from hygge.utility.path_helper import PathHelper


class ParquetHome(Home, home_type="parquet"):
    """
    A Parquet file home for data.

    Features:
    - Efficient batch reading with polars
    - Progress tracking
    - Uses centralized configuration system

    Example:
        ```python
        config = ParquetHomeConfig(
            path="data/users.parquet",
            options={'batch_size': 10_000}
        )
        home = ParquetHome("users", config)
        ```
    """

    def __init__(
        self, name: str, config: "ParquetHomeConfig", entity_name: Optional[str] = None
    ):
        # Get merged options from config
        merged_options = config.get_merged_options()

        super().__init__(name, merged_options)
        self.config = config
        self.entity_name = entity_name

        # If entity_name provided, append to base path using PathHelper
        if entity_name:
            merged_path = PathHelper.merge_paths(config.path, entity_name)
            self.data_path = Path(merged_path)
        else:
            self.data_path = Path(config.path)

    def get_data_path(self) -> Path:
        """Get the primary data path for this parquet home."""
        return self.data_path

    def get_batch_paths(self) -> list[Path]:
        """
        Get list of parquet files that will be read.

        This is a convenience method for inspection. The actual reading
        delegates to Polars which handles all path types intelligently.

        Returns:
            List of Path objects to parquet files
        """
        # Verify path exists
        if not self.data_path.exists():
            raise HomeError(f"Path does not exist: {self.data_path}")

        # If it's a file, return it
        if self.data_path.is_file():
            return [self.data_path]

        # If it's a directory, find all parquet files (including in subdirectories)
        if self.data_path.is_dir():
            # Look for .parquet files recursively
            parquet_files = sorted(self.data_path.rglob("*.parquet"))
            if not parquet_files:
                raise HomeError(
                    f"No parquet files found in directory: {self.data_path}"
                )
            return parquet_files

        raise HomeError(f"Path is neither file nor directory: {self.data_path}")

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """
        Get data iterator for parquet file(s).

        Handles:
        - Single .parquet files
        - Parquet dataset directories (with or without .parquet extension)
        - Partitioned parquet datasets
        - Directories containing multiple parquet files

        Relies on Polars' scan_parquet() to handle path detection.
        """
        try:
            batch_size = self.options.get("batch_size", 10_000)

            # Let Polars handle path detection - it's smarter than us!
            # Polars can handle: files, directories, datasets, partitions
            self.logger.debug(f"Reading parquet from: {self.data_path}")

            # Polars' scan_parquet handles both files and directories
            lf = pl.scan_parquet(self.data_path)

            # Get total rows to determine number of batches
            total_rows = lf.select(pl.len()).collect().item()

            if total_rows == 0:
                self.logger.warning(f"No data found in {self.data_path}")
                return

            # Calculate number of batches needed
            num_batches = (total_rows + batch_size - 1) // batch_size
            self.logger.debug(f"{total_rows:,} rows, running {num_batches} batches")

            # Yield data in batches
            for batch_idx in range(num_batches):
                offset = batch_idx * batch_size
                batch_df = lf.slice(offset, batch_size).collect(engine="streaming")

                if len(batch_df) > 0:
                    self.logger.debug(f"Yielding batch {batch_idx + 1})")
                    yield batch_df

        except HomeError:
            # Other home errors - preserve and re-raise
            raise
        except Exception as e:
            # Unexpected errors - wrap in HomeReadError
            # CRITICAL: Use 'from e' to preserve exception context
            raise HomeReadError(
                f"Failed to read parquet from {self.data_path}: {str(e)}"
            ) from e


class ParquetHomeConfig(HomeConfig, BaseHomeConfig, config_type="parquet"):
    """Configuration for a ParquetHome."""

    path: str = Field(..., description="Path to parquet file or directory")
    batch_size: int = Field(
        default=10_000, ge=1, description="Number of rows to read at once"
    )
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional parquet home options"
    )

    @field_validator("path")
    @classmethod
    def validate_path(cls, v):
        """Validate path is provided."""
        if not v:
            raise ValueError("Path is required for parquet homes")
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
