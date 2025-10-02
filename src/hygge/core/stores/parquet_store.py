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

        self.file_pattern = self.options.get(
            'file_pattern',
            "{sequence:020d}.parquet"
        )
        self.compression = self.options.get('compression', 'snappy')
        self.sequence_counter = 0

        # Ensure directories exist
        self.ensure_directories_exist()

    def get_staging_directory(self) -> Path:
        """Get the staging directory for temporary storage."""
        return self.base_path / "tmp" / self.name

    def get_final_directory(self) -> Path:
        """Get the final directory for permanent storage."""
        return self.base_path / self.name

    async def get_next_filename(self) -> str:
        """Generate the next filename using pattern."""
        self.sequence_counter += 1
        return self.file_pattern.format(
            name=self.name,
            sequence=self.sequence_counter
        )

    async def _save(self, df: pl.DataFrame, staging_path: Path) -> None:
        """Save data to parquet file."""
        try:
            # Ensure directory exists
            staging_path.parent.mkdir(parents=True, exist_ok=True)

            # Write using standard parquet write
            df.write_parquet(
                staging_path,
                compression=self.compression
            )

            # Verify the file was actually created
            if not staging_path.exists():
                self.logger.error(f"Failed to create parquet file: {staging_path}")
                raise StoreError(f"File was not created after write: {staging_path}")

            file_size = staging_path.stat().st_size
            self.logger.success(f"Wrote {len(df):,} rows to {staging_path.name} ({file_size:,} bytes)")

        except Exception as e:
            self.logger.error(f"Failed to write parquet to {staging_path}: {str(e)}")
            raise StoreError(f"Failed to write parquet to {staging_path}: {str(e)}")

    async def _cleanup_temp(self, staging_path: Path) -> None:
        """Clean up temporary file."""
        try:
            if staging_path.exists():
                staging_path.unlink()
                self.logger.debug(f"Cleaned up staging file: {staging_path}")
        except Exception as e:
            self.logger.warning(
                f"Failed to cleanup staging file {staging_path}: {str(e)}"
            )

    async def _move_to_final(self, staging_path: Path, final_path: Path) -> None:
        """Move file from staging to final location."""
        try:
            if not staging_path.exists():
                raise StoreError(f"Staging file does not exist: {staging_path}")

            # Ensure parent directory exists
            final_path.parent.mkdir(parents=True, exist_ok=True)

            # Move file
            staging_path.rename(final_path)
            self.logger.success(f"Moved {staging_path.name} to final location")

        except Exception as e:
            self.logger.error(f"Failed to move file to final location: {str(e)}")
            raise StoreError(f"Failed to finalize transfer {staging_path}: {str(e)}")

    async def close(self) -> None:
        """Finalize any remaining writes and cleanup."""
        await self.finish()

        # Cleanup staging directory
        try:
            staging_dir = self.get_staging_directory()
            if staging_dir.exists():
                for file in staging_dir.glob("**/*"):
                    if file.is_file():
                        file.unlink()
                self.logger.debug(f"Cleaned up staging directory: {staging_dir}")
        except Exception as e:
            self.logger.warning(f"Failed to cleanup staging directory: {str(e)}")