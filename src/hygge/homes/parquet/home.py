"""
Parquet file home implementation.
"""
from pathlib import Path
from typing import Any, AsyncIterator, Dict

import polars as pl
from pydantic import Field, field_validator

from hygge.core.home import BaseHomeConfig, Home, HomeConfig
from hygge.utility.exceptions import HomeError


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

    def __init__(self, name: str, config: "ParquetHomeConfig"):
        # Get merged options from config
        merged_options = config.get_merged_options()

        super().__init__(name, merged_options)
        self.config = config
        self.data_path = Path(config.path)

    def get_data_path(self) -> Path:
        """Get the primary data path for this parquet home."""
        return self.data_path

    def get_batch_paths(self) -> list[Path]:
        """
        Get all available data paths for batch processing.

        For parquet files, this could be a single file or a directory of files.
        """
        if self.data_path.is_file():
            return [self.data_path]
        elif self.data_path.is_dir():
            # Find all parquet files in the directory
            parquet_files = sorted(self.data_path.glob("*.parquet"))
            if not parquet_files:
                raise HomeError(
                    f"No parquet files found in directory: {self.data_path}"
                )
            return parquet_files
        else:
            raise HomeError(
                f"Data path is neither file nor directory: {self.data_path}"
            )

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """Get data iterator for parquet file(s)."""
        try:
            paths = self.get_batch_paths()
            batch_size = self.options.get("batch_size", 10_000)
            self.logger.debug(f"Read from {self.name} at {len(paths)} path(s)")

            for path in paths:
                self.logger.debug(f"Processing parquet file: {path}")

                # Use polars' streaming capabilities with batching
                lf = pl.scan_parquet(path)

                # Get total rows to determine number of batches
                total_rows = lf.select(pl.len()).collect().item()

                if total_rows == 0:
                    continue

                # Calculate number of batches needed
                num_batches = (total_rows + batch_size - 1) // batch_size
                self.logger.debug(
                    f"File has {total_rows} rows, will create {num_batches} batches"
                )

                # Yield data in batches
                for batch_idx in range(num_batches):
                    offset = batch_idx * batch_size
                    batch_df = lf.slice(offset, batch_size).collect(engine="streaming")

                    if len(batch_df) > 0:
                        self.logger.debug(
                            f"Yielding batch {batch_idx + 1}/{num_batches}"
                        )
                        yield batch_df

        except Exception as e:
            raise HomeError(f"Failed to read parquet from {self.data_path}: {str(e)}")


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
