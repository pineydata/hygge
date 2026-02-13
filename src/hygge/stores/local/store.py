"""
Local file store: writes to local paths using the format layer.
"""

from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl
from pydantic import Field, field_validator

from hygge.core.formats import (
    VALID_FORMATS,
    default_file_pattern,
    format_to_suffix,
)
from hygge.core.formats import (
    write as format_write,
)
from hygge.core.polish import PolishConfig, Polisher
from hygge.core.store import BaseStoreConfig, Store, StoreConfig
from hygge.utility.exceptions import StoreError, StoreWriteError
from hygge.utility.path_helper import PathHelper


class LocalStore(Store, store_type="local"):
    """
    A local file store that writes by format (parquet, csv, ndjson).

    Delegates actual I/O to the format layer. Staging + move-to-final same as
    ParquetStore. File pattern and extension are format-aware.
    """

    def __init__(
        self,
        name: str,
        config: "LocalStoreConfig",
        flow_name: Optional[str] = None,
        entity_name: Optional[str] = None,
    ):
        merged_options = config.get_merged_options(flow_name or name)
        super().__init__(name, merged_options)
        self.config = config
        self._polisher = (
            Polisher(config.polish) if getattr(config, "polish", None) else None
        )
        self.entity_name = entity_name

        self._format = getattr(config, "format", "parquet")
        self._format_options = dict(getattr(config, "format_options", None) or {})
        # ParquetStoreConfig backward compat: pass compression into format_options
        if hasattr(config, "compression") and config.compression:
            self._format_options.setdefault("compression", config.compression)

        if entity_name:
            merged_path = PathHelper.merge_paths(config.path, entity_name)
            self.base_path = Path(merged_path)
        else:
            self.base_path = Path(config.path)

        # Default file_pattern from single source (formats.default_file_pattern)
        self.file_pattern = self.options.get("file_pattern")
        if not self.file_pattern:
            self.file_pattern = default_file_pattern(self._format)

        self.sequence_counter = 0
        self.saved_paths: list[str] = []
        self.ensure_directories_exist()

    def ensure_directories_exist(self) -> None:
        try:
            self.get_staging_directory().mkdir(parents=True, exist_ok=True)
            self.get_final_directory().mkdir(parents=True, exist_ok=True)
        except (OSError, FileNotFoundError) as e:
            raise StoreError(f"Failed to create directories: {str(e)}") from e

    def get_staging_directory(self) -> Path:
        parent_str = str(self.base_path.parent)
        merged_path = PathHelper.merge_paths(parent_str, "tmp", self.name)
        result = Path(merged_path)
        if self.base_path.is_absolute() and not result.is_absolute():
            result = self.base_path.parent / "tmp" / self.name
        return result

    def get_final_directory(self) -> Path:
        return self.base_path

    async def get_next_filename(self) -> str:
        self.sequence_counter += 1
        final_dir = self.get_final_directory()
        suffix = format_to_suffix(self._format)
        if final_dir.exists():
            existing = list(final_dir.glob(f"*{suffix}"))
            if existing:
                max_seq = 0
                for f in existing:
                    try:
                        n = int(f.stem)
                        max_seq = max(max_seq, n)
                    except ValueError:
                        continue
                self.sequence_counter = max_seq + 1
        return self.file_pattern.format(
            name=self.name,
            sequence=self.sequence_counter,
            flow_name=getattr(self, "_flow_name", self.name),
        )

    async def _save(self, df: pl.DataFrame, staging_path: str) -> None:
        try:
            if len(df) == 0:
                self.logger.debug("Skipping empty DataFrame")
                return
            staging_path = Path(staging_path)
            staging_path.parent.mkdir(parents=True, exist_ok=True)
            format_write(
                df,
                staging_path,
                self._format,
                **self._format_options,
            )
            if not staging_path.exists():
                raise StoreError(f"File was not created after write: {staging_path}")
            self._log_write_progress(len(df), path=str(staging_path))
            self.saved_paths.append(str(staging_path))
        except StoreError:
            raise
        except Exception as e:
            self.logger.error(f"Failed to write {self._format} to {staging_path}: {e}")
            raise StoreWriteError(
                f"Failed to write {self._format} to {staging_path}: {e}"
            ) from e

    async def _cleanup_temp(self, staging_path: str) -> None:
        try:
            p = Path(staging_path)
            if p.exists():
                p.unlink()
                self.logger.debug(f"Cleaned up staging file: {p}")
        except OSError as e:
            self.logger.warning(f"Failed to cleanup staging file {staging_path}: {e}")

    async def _move_to_final(self, staging_path: str, final_path: str) -> None:
        try:
            st, fin = Path(staging_path), Path(final_path)
            if not st.exists():
                if fin.exists():
                    return
                raise StoreError(f"Staging file does not exist: {st}")
            fin.parent.mkdir(parents=True, exist_ok=True)
            st.rename(fin)
            self.logger.debug("Moved file to final location")
        except Exception as e:
            raise StoreError(f"Failed to finalize transfer {staging_path}: {e}") from e

    async def cleanup_staging(self) -> None:
        try:
            staging_dir = self.get_staging_directory()
            if staging_dir.exists():
                for f in staging_dir.glob("**/*"):
                    if f.is_file():
                        f.unlink()
                self.logger.debug(f"Cleaned up staging directory: {staging_dir}")
        except OSError as e:
            self.logger.warning(f"Failed to cleanup staging directory: {e}")

    async def reset_retry_sensitive_state(self) -> None:
        await super().reset_retry_sensitive_state()
        self.sequence_counter = 0
        self.saved_paths.clear()
        self.logger.debug("Reset sequence counter and saved paths for retry")

    async def close(self) -> None:
        await self.finish()
        await self.cleanup_staging()


class LocalStoreConfig(StoreConfig, BaseStoreConfig, config_type="local"):
    """Configuration for a LocalStore (path + format + optional format_options)."""

    path: str = Field(..., description="Path to destination directory")
    format: str = Field(
        default="parquet",
        description="File format: parquet, csv, or ndjson",
    )
    format_options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Options passed to the format layer (e.g. compression for parquet)",
    )
    batch_size: int = Field(
        default=100_000,
        ge=1,
        description="Number of rows to accumulate before writing",
    )
    file_pattern: str = Field(
        default="",
        description="Output file naming pattern. Default: {sequence:020d}.<format>.",
    )
    polish: Optional[PolishConfig] = Field(
        default=None,
        description="Optional Polisher configuration for last-mile transforms.",
    )
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional store options",
    )

    @field_validator("path")
    @classmethod
    def validate_path(cls, v: str) -> str:
        if not v:
            raise ValueError("Path is required for local stores")
        return v

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        if v.lower() not in VALID_FORMATS:
            raise ValueError(f"Format must be one of: {', '.join(VALID_FORMATS)}")
        return v.lower()

    def get_merged_options(self, flow_name: str = "") -> Dict[str, Any]:
        pattern = self.file_pattern or default_file_pattern(self.format)
        options = {
            "batch_size": self.batch_size,
            "file_pattern": pattern,
        }
        options.update(self.options)
        if flow_name and "{flow_name}" in options["file_pattern"]:
            options["file_pattern"] = options["file_pattern"].replace(
                "{flow_name}", flow_name
            )
        return options
