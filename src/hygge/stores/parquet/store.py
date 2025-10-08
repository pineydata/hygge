"""
Parquet store implementation.
"""
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl
from pydantic import Field, field_validator

from hygge.core.store import BaseStoreConfig, Store, StoreConfig
from hygge.utility.exceptions import StoreError


class ParquetStore(Store, store_type="parquet"):
    """
    A Parquet file store for data.

    Features:
    - Efficient batch writing with polars
    - Data buffering and accumulation
    - Atomic writes with temp files
    - Progress tracking
    - Uses centralized configuration system

    Example:
        ```python
        config = ParquetStoreConfig(
            path="data/users",
            options={
                'file_pattern': "{name}_{timestamp}.parquet",
                'compression': 'snappy'
            }
        )
        store = ParquetStore("users", config)
        ```
    """

    def __init__(
        self,
        name: str,
        config: "ParquetStoreConfig",
        flow_name: Optional[str] = None,
        entity_name: Optional[str] = None,
    ):
        # Get merged options from config (with flow_name for file_pattern)
        merged_options = config.get_merged_options(flow_name or name)

        super().__init__(name, merged_options)
        self.config = config
        self.entity_name = entity_name

        # If entity_name provided, append to base path
        if entity_name:
            self.base_path = Path(config.path) / entity_name
        else:
            self.base_path = Path(config.path)

        self.file_pattern = self.options.get("file_pattern")
        self.compression = self.options.get("compression")
        self.sequence_counter = 0
        self.saved_paths = []  # Track staged file paths for moving to final

        # Ensure directories exist
        self.ensure_directories_exist()

    def ensure_directories_exist(self) -> None:
        """Create staging and final directories if they don't exist."""
        try:
            staging_dir = self.get_staging_directory()
            final_dir = self.get_final_directory()

            staging_dir.mkdir(parents=True, exist_ok=True)
            final_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, FileNotFoundError) as e:
            from hygge.utility.exceptions import StoreError

            raise StoreError(f"Failed to create directories: {str(e)}")

    def get_staging_directory(self) -> Path:
        """Get the staging directory for temporary storage."""
        # Create tmp at the same level as base_path, not nested under it
        return self.base_path.parent / "tmp" / self.name

    def get_final_directory(self) -> Path:
        """Get the final directory for permanent storage."""
        return self.base_path

    async def get_next_filename(self) -> str:
        """Generate the next filename using pattern."""
        self.sequence_counter += 1

        # Check for existing files and continue from highest sequence number
        final_dir = self.get_final_directory()
        if final_dir.exists():
            existing_files = list(final_dir.glob("*.parquet"))
            if existing_files:
                # Extract sequence numbers from existing files
                max_sequence = 0
                for file in existing_files:
                    try:
                        filename = file.stem
                        sequence = int(filename)
                        max_sequence = max(max_sequence, sequence)
                    except ValueError:
                        # Skip files that don't match the sequence pattern
                        continue

                # Start from the next sequence number
                self.sequence_counter = max_sequence + 1

        return self.file_pattern.format(name=self.name, sequence=self.sequence_counter)

    async def _save(self, df: pl.DataFrame, staging_path: str) -> None:
        """Save data to parquet file."""
        try:
            # Skip empty DataFrames
            if len(df) == 0:
                self.logger.debug("Skipping empty DataFrame")
                return

            # Convert string path to Path object
            staging_path = Path(staging_path)

            # Ensure directory exists
            staging_path.parent.mkdir(parents=True, exist_ok=True)

            # Write using standard parquet write
            df.write_parquet(staging_path, compression=self.compression)

            # Verify the file was actually created
            if not staging_path.exists():
                self.logger.error(f"Failed to create parquet file: {staging_path}")
                raise StoreError(f"File was not created after write: {staging_path}")

            file_size = staging_path.stat().st_size
            self.logger.success(
                f"Wrote {len(df):,} rows to {staging_path.name} ({file_size:,} bytes)"
            )

            # Track path for moving to final location
            self.saved_paths.append(str(staging_path))

        except Exception as e:
            self.logger.error(f"Failed to write parquet to {staging_path}: {str(e)}")
            raise StoreError(f"Failed to write parquet to {staging_path}: {str(e)}")

    async def _cleanup_temp(self, staging_path: str) -> None:
        """Clean up temporary file."""
        try:
            # Convert string path to Path object
            staging_path = Path(staging_path)

            if staging_path.exists():
                staging_path.unlink()
                self.logger.debug(f"Cleaned up staging file: {staging_path}")
        except Exception as e:
            self.logger.warning(
                f"Failed to cleanup staging file {staging_path}: {str(e)}"
            )

    async def _move_to_final(self, staging_path: str, final_path: str) -> None:
        """Move file from staging to final location."""
        try:
            # Convert string paths to Path objects
            staging_path = Path(staging_path)
            final_path = Path(final_path)

            # If staging file doesn't exist, it may have already been moved
            if not staging_path.exists():
                if final_path.exists():
                    # File already moved, skip silently
                    self.logger.debug(f"File already moved: {staging_path.name}")
                    return
                else:
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


class ParquetStoreConfig(StoreConfig, BaseStoreConfig, config_type="parquet"):
    """Configuration for a ParquetStore."""

    path: str = Field(..., description="Path to destination directory")
    batch_size: int = Field(
        default=100_000, ge=1, description="Number of rows to accumulate before writing"
    )
    compression: str = Field(default="snappy", description="Compression algorithm")
    file_pattern: str = Field(
        default="{sequence:020d}.parquet", description="File naming pattern"
    )
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional parquet store options"
    )

    @field_validator("path")
    @classmethod
    def validate_path(cls, v):
        """Validate path is provided."""
        if not v:
            raise ValueError("Path is required for parquet stores")
        return v

    @field_validator("compression")
    @classmethod
    def validate_compression(cls, v):
        """Validate compression type."""
        valid_compressions = ["snappy", "gzip", "lz4", "brotli", "zstd"]
        if v not in valid_compressions:
            raise ValueError(
                f"Compression must be one of {valid_compressions}, got '{v}'"
            )
        return v

    def get_merged_options(self, flow_name: str = None) -> Dict[str, Any]:
        """Get all options including defaults."""
        # Start with the config fields
        options = {
            "batch_size": self.batch_size,
            "compression": self.compression,
            "file_pattern": self.file_pattern,
        }
        # Add any additional options
        options.update(self.options)

        # Set flow-specific file pattern if flow_name provided
        if flow_name:
            pattern = options["file_pattern"]
            # Only format flow_name if the pattern contains {flow_name}
            if "{flow_name}" in pattern:
                # Simple string replacement for flow_name only
                options["file_pattern"] = pattern.replace("{flow_name}", flow_name)

        return options
