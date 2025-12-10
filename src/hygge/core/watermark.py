"""
Watermark tracking for incremental data loads.

Watermarks track the "last processed value" for incremental data loads,
enabling efficient incremental processing by filtering data based on
previously processed watermarks.

Following hygge's philosophy:
- **Comfort**: Clear validation and error messages
- **Reliability**: Fail fast on configuration errors
- **Natural flow**: Simple API that feels natural to use
"""
from typing import Any, Optional

import polars as pl

from hygge.utility.exceptions import ConfigError


class Watermark:
    """
    Tracks watermark values across batches for incremental data loads.

    Watermarks track the maximum value of a specified column across all
    processed batches, enabling incremental loads that only process new
    or updated data.

    Example:
        ```python
        watermark = Watermark(
            {"primary_key": "user_id", "watermark_column": "updated_at"},
            logger
        )
        watermark.validate_schema(schema)  # Fail fast if columns missing
        watermark.update(batch)  # Track max value from batch
        watermark_value = watermark.serialize_watermark()  # For journal storage
        ```

    Args:
        watermark_config: Dictionary with 'primary_key' and 'watermark_column'
        logger: Logger instance for warnings and errors
    """

    def __init__(
        self,
        watermark_config: dict[str, str],
        logger: Any,  # Logger type
    ):
        self.primary_key = watermark_config.get("primary_key")
        self.watermark_column = watermark_config.get("watermark_column")
        self.logger = logger

        self._watermark_candidate: Optional[Any] = None
        self._watermark_type: Optional[str] = None

    def validate_schema(self, schema: pl.Schema) -> None:
        """
        Validate that watermark columns exist in schema.

        Fails fast with clear error messages if:
        - Primary key column is missing
        - Watermark column is missing
        - Watermark column type is unsupported

        Args:
            schema: Polars schema to validate against

        Raises:
            ConfigError: If validation fails
        """
        if self.primary_key and self.primary_key not in schema:
            raise ConfigError(
                f"Primary key '{self.primary_key}' not found in data schema. "
                f"Available columns: {list(schema.keys())}"
            )

        if self.watermark_column and self.watermark_column not in schema:
            raise ConfigError(
                f"Watermark column '{self.watermark_column}' not found in data schema. "
                f"Available columns: {list(schema.keys())}"
            )

        # Validate watermark column type is supported
        if self.watermark_column:
            watermark_dtype = schema[self.watermark_column]
            if not self._is_supported_type(watermark_dtype):
                raise ConfigError(
                    f"Unsupported watermark type: {watermark_dtype}. "
                    f"Supported types: datetime, integer, string"
                )

    def _is_supported_type(self, dtype: pl.DataType) -> bool:
        """
        Check if dtype is supported for watermark tracking.

        Args:
            dtype: Polars data type to check

        Returns:
            True if the type is supported for watermark tracking
        """
        # Check datetime (can be instance like Datetime("us") or class Datetime)
        if dtype == pl.Datetime or isinstance(dtype, pl.Datetime):
            return True
        if dtype in (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        ):
            return True
        if dtype == pl.Utf8:
            return True
        return False

    def update(self, batch: pl.DataFrame) -> None:
        """
        Update watermark value from batch.

        Tracks the maximum value of the watermark column across all batches.
        Handles type detection, null values, and type consistency checks.

        Args:
            batch: Polars DataFrame to extract watermark value from

        Raises:
            ConfigError: If watermark column is missing after validation
                (indicates schema mismatch or validation bug)
        """
        if not self.watermark_column:
            return

        if self.watermark_column not in batch.columns:
            # This should never happen after validation, but if it does, fail loudly
            # rather than silently skipping. This indicates a schema mismatch
            # between validation and actual data, which is a serious issue.
            raise ConfigError(
                f"Watermark column '{self.watermark_column}' not found in batch. "
                f"This indicates a schema mismatch after validation. "
                f"Available columns: {list(batch.columns)}"
            )

        column_series = batch[self.watermark_column]
        if column_series.is_null().all():
            return

        dtype = column_series.dtype
        candidate_type: Optional[str] = None
        candidate_value: Optional[Any] = None

        # Check datetime (can be instance like Datetime("us") or class Datetime)
        if dtype == pl.Datetime or isinstance(dtype, pl.Datetime):
            candidate_type = "datetime"
            candidate_value = column_series.max()
        elif dtype in (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        ):
            candidate_type = "int"
            candidate_value = int(column_series.max())
        elif dtype == pl.Utf8:
            candidate_type = "string"
            candidate_value = str(column_series.max())
        else:
            # Unsupported type - this should have been caught by validation
            # Log a warning but don't raise - validation is the authoritative check
            self.logger.warning(
                f"Unsupported watermark type '{dtype}' in batch. "
                f"This should have been caught by validation. "
                f"Skipping watermark update for this batch."
            )
            return

        if candidate_value is None:
            return

        if self._watermark_candidate is None:
            self._watermark_candidate = candidate_value
            self._watermark_type = candidate_type
            return

        if self._watermark_type != candidate_type:
            self.logger.warning(
                f"Inconsistent watermark types across batches: "
                f"{self._watermark_type} vs {candidate_type}"
            )
            return

        if candidate_value > self._watermark_candidate:
            self._watermark_candidate = candidate_value

    def get_watermark_value(self) -> Optional[Any]:
        """
        Get current watermark value.

        Returns:
            Current watermark value (datetime, int, or string), or None if not set
        """
        return self._watermark_candidate

    def get_watermark_type(self) -> Optional[str]:
        """
        Get current watermark type.

        Returns:
            Current watermark type ("datetime", "int", or "string"), or None if not set
        """
        return self._watermark_type

    def serialize_watermark(self) -> Optional[str]:
        """
        Serialize watermark value for journal storage.

        Converts watermark value to string format suitable for storage:
        - datetime: ISO format string
        - int: string representation
        - string: as-is

        Returns:
            Serialized watermark value, or None if no watermark set
        """
        if self._watermark_candidate is None:
            return None

        if self._watermark_type == "datetime":
            return self._watermark_candidate.isoformat()
        elif self._watermark_type == "int":
            return str(self._watermark_candidate)
        elif self._watermark_type == "string":
            return self._watermark_candidate
        else:
            return None

    def reset(self) -> None:
        """
        Reset watermark state for a new run.

        Clears the tracked watermark value and type, allowing the
        watermark tracker to start fresh for a new flow execution.
        """
        self._watermark_candidate = None
        self._watermark_type = None
