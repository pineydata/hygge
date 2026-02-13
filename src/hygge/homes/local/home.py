"""
Local file home: reads from local paths using the format layer.
"""

from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

import polars as pl
from pydantic import Field, field_validator

from hygge.core.formats import VALID_FORMATS, format_to_suffix
from hygge.core.formats import read as format_read
from hygge.core.home import BaseHomeConfig, Home, HomeConfig
from hygge.utility.exceptions import HomeError, HomeReadError
from hygge.utility.path_helper import PathHelper


class LocalHome(Home, home_type="local"):
    """
    A local file home that reads by format (parquet, csv, ndjson).

    Delegates actual I/O to the format layer. Supports single file or directory
    (glob by format extension). Compatible with ParquetHomeConfig for backward
    compatibility (type: parquet → LocalHome with format=parquet).
    """

    def __init__(
        self,
        name: str,
        config: "LocalHomeConfig",
        entity_name: Optional[str] = None,
    ):
        merged_options = config.get_merged_options()
        super().__init__(name, merged_options)
        self.config = config
        self.entity_name = entity_name

        self._format = getattr(config, "format", "parquet")
        self._format_options = getattr(config, "format_options", None) or {}

        if entity_name:
            merged_path = PathHelper.merge_paths(config.path, entity_name)
            self.data_path = Path(merged_path)
        else:
            self.data_path = Path(config.path)

    def get_data_path(self) -> Path:
        """Get the primary data path for this local home."""
        return self.data_path

    def get_batch_paths(self) -> list[Path]:
        """
        Get list of files to read (by format extension).
        Single file → [path]; directory → sorted glob by format suffix.
        """
        if not self.data_path.exists():
            raise HomeError(f"Path does not exist: {self.data_path}")

        suffix = format_to_suffix(self._format)
        if self.data_path.is_file():
            return [self.data_path]
        if self.data_path.is_dir():
            pattern = f"*{suffix}"
            files = sorted(self.data_path.rglob(pattern))
            if not files:
                raise HomeError(
                    f"No {self._format} files found in directory: {self.data_path}"
                )
            return files

        raise HomeError(f"Path is neither file nor directory: {self.data_path}")

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """Yield batches from the format layer (read per file)."""
        try:
            batch_size = self.options.get("batch_size", 50_000)

            for path in self.get_batch_paths():
                self.logger.debug(f"Reading {self._format} from: {path}")
                for batch_df in format_read(
                    path,
                    self._format,
                    batch_size=batch_size,
                    **self._format_options,
                ):
                    yield batch_df

        except HomeError:
            raise
        except Exception as e:
            raise HomeReadError(
                f"Failed to read {self._format} from {self.data_path}: {str(e)}"
            ) from e


class LocalHomeConfig(HomeConfig, BaseHomeConfig, config_type="local"):
    """Configuration for a LocalHome (path + format + optional format_options)."""

    path: str = Field(..., description="Path to file or directory")
    format: str = Field(
        default="parquet",
        description="File format: parquet, csv, or ndjson",
    )
    format_options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Options passed to the format layer (e.g. compression, encoding)",
    )
    batch_size: int = Field(
        default=50_000,
        ge=1,
        description="Number of rows to read per batch",
    )
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional home options",
    )

    @field_validator("path")
    @classmethod
    def validate_path(cls, v: str) -> str:
        if not v:
            raise ValueError("Path is required for local homes")
        return v

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        if v.lower() not in VALID_FORMATS:
            raise ValueError(f"Format must be one of: {', '.join(VALID_FORMATS)}")
        return v.lower()

    def get_merged_options(self) -> Dict[str, Any]:
        options = {
            "batch_size": self.batch_size,
        }
        options.update(self.options)
        return options
