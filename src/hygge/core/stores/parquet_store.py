"""
Parquet store implementation.
"""
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl

from hygge.core.store import Store
from hygge.utility.exceptions import StoreError


class ParquetStore(Store):
    """
    A Parquet file store for data.

    Features:
    - Efficient batch writing with polars
    - Data buffering and accumulation
    - Atomic writes with temp files
    - Progress tracking

    Example:
        ```python
        store = ParquetStore(
            "users",
            path="data/users",
            options={
                'file_pattern': "{name}_{timestamp}.parquet",
                'compression': 'snappy'
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
        self.base_path = Path(path)

        self.temp_path = self.base_path
        self.file_pattern = self.options.get(
            'file_pattern',
            "{name}_{timestamp}.parquet"
        )
        self.compression = self.options.get('compression', 'snappy')

        # Ensure directories exist
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.temp_path.mkdir(parents=True, exist_ok=True)

    async def _save(self, df: pl.DataFrame, path: str) -> None:
        """Save data to parquet file."""
        try:
            full_path = self.temp_path / path
            self.logger.debug(f"Saving {len(df)} rows to {full_path}")
            full_path.parent.mkdir(parents=True, exist_ok=True)

            # Write using standard parquet write
            df.write_parquet(
                full_path,
                compression=self.compression
            )

            self.logger.debug(f"Successfully wrote batch to {full_path}")

        except Exception as e:
            self.logger.error(f"Failed to write parquet to {path}: {str(e)}")
            raise StoreError(f"Failed to write parquet to {path}: {str(e)}")

    async def _get_next_filename(self) -> str:
        """Get next filename using pattern."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.file_pattern.format(
            name=self.name,
            timestamp=timestamp
        )

    async def _cleanup_temp(self, path: str) -> None:
        """Clean up temporary file."""
        try:
            temp_file = self.temp_path / path
            if temp_file.exists():
                temp_file.unlink()
        except Exception as e:
            self.logger.warning(f"Failed to cleanup temp file {path}: {str(e)}")

    async def _move_to_final(self, temp_path: str, final_path: str) -> None:
        """Move file from temp to final location."""
        try:
            # temp_path is relative to temp directory, so construct full path
            source = self.temp_path / temp_path
            # Create table subdirectory in final location
            dest = self.base_path / self.name / Path(final_path).name

            self.logger.debug(f"Moving from {source} to {dest}")
            self.logger.debug(f"Source exists: {source.exists()}")
            self.logger.debug(f"Source size: {source.stat().st_size if source.exists() else 'N/A'}")

            # Ensure parent directory exists
            dest.parent.mkdir(parents=True, exist_ok=True)

            # Move file
            source.rename(dest)
            self.logger.debug(f"Successfully moved {source} to {dest}")

        except Exception as e:
            self.logger.error(f"Failed to finalize transfer {temp_path}: {str(e)}")
            raise StoreError(f"Failed to finalize transfer {temp_path}: {str(e)}")

    async def close(self) -> None:
        """Finalize any remaining writes and cleanup."""
        await self.finalize()

        # Cleanup temp directory
        try:
            for file in self.temp_path.glob("**/*"):
                if file.is_file():
                    file.unlink()
        except Exception as e:
            self.logger.warning(f"Failed to cleanup temp directory: {str(e)}")