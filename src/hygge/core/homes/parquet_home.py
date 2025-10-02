"""
Parquet file home implementation.
"""
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

import polars as pl

from hygge.core.home import Home
from hygge.utility.exceptions import HomeError


class ParquetHome(Home):
    """
    A Parquet file home for data.

    Features:
    - Efficient batch reading with polars
    - Progress tracking

    Example:
        ```python
        home = ParquetHome(
            "users",
            path="data/users.parquet",
            options={
                'batch_size': 10_000
            }
        )
        ```
    """

    def __init__(
        self,
        name: str,
        path: str,
        options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(name, options)
        self.data_path = Path(path)

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
            parquet_files = list(self.data_path.glob("*.parquet"))
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
            self.logger.debug(f"Reading from {self.name} at {len(paths)} path(s)")

            for path in paths:
                self.logger.debug(f"Processing parquet file: {path}")

                # Use polars' streaming capabilities
                lf = pl.scan_parquet(path)
                df = lf.collect(engine='streaming')

                # Yield the DataFrame as a batch
                if len(df) > 0:
                    yield df

        except Exception as e:
            raise HomeError(f"Failed to read parquet from {self.data_path}: {str(e)}")
