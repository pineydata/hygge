"""
Journal implementation for tracking flow execution metadata.

Single-file parquet-based journal that tracks entity runs with denormalized
hierarchy information for efficient watermark queries.
"""
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4

import polars as pl
from pydantic import BaseModel, Field, field_validator

from hygge.utility.exceptions import ConfigError
from hygge.utility.logger import get_logger
from hygge.utility.run_id import generate_run_id


class JournalConfig(BaseModel):
    """
    Configuration for the journal.

    Supports path inference from store/home paths or explicit path configuration.
    """

    path: Optional[str] = Field(
        default=None,
        description=(
            "Explicit path to journal directory " "(overrides location inference)"
        ),
    )
    location: Optional[str] = Field(
        default="store",
        description=(
            "Location inference: 'store' (default), 'home', " "or None (requires path)"
        ),
    )

    @field_validator("location")
    @classmethod
    def validate_location(cls, v):
        """Validate location value."""
        if v is not None and v not in ["store", "home"]:
            raise ValueError(f"Location must be 'store', 'home', or None, got '{v}'")
        return v


class Journal:
    """
    Parquet-based journal for tracking flow execution metadata.

    Single-file design with denormalized entity runs for efficient
    watermark queries. Uses run-based architecture (one row per
    completed entity run).

    Example:
        ```python
        config = JournalConfig(path="/path/to/journal")
        journal = Journal("my_journal", config, "main_coordinator")

        # Record entity run
        await journal.record_entity_run(
            coordinator_run_id=coordinator_run_id,
            flow_run_id=flow_run_id,
            coordinator="main_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time,
            finish_time=finish_time,
            status="success",
            run_type="full_drop",
            row_count=1000,
            duration=5.0,
            primary_key="user_id",
            watermark_column="signup_date",
            watermark_type="datetime",
            watermark="2024-01-01T09:00:00Z",
            message=None,
        )

        # Query watermark
        watermark = await journal.get_watermark(
            "users_flow", entity="users"
        )
        ```
    """

    # Schema version for evolution compatibility
    SCHEMA_VERSION = "1.0"

    # Journal schema (Polars types)
    JOURNAL_SCHEMA = {
        "entity_run_id": pl.Utf8,
        "coordinator_run_id": pl.Utf8,
        "flow_run_id": pl.Utf8,
        "coordinator": pl.Utf8,
        "flow": pl.Utf8,
        "entity": pl.Utf8,
        "start_time": pl.Utf8,  # ISO format string
        "finish_time": pl.Utf8,  # ISO format string (nullable)
        "status": pl.Utf8,
        "run_type": pl.Utf8,
        "row_count": pl.Int64,  # Nullable
        "duration": pl.Float64,  # Nullable
        "primary_key": pl.Utf8,  # Nullable
        "watermark_column": pl.Utf8,  # Nullable
        "watermark_type": pl.Utf8,  # Nullable
        "watermark": pl.Utf8,  # Nullable
        "message": pl.Utf8,  # Nullable
        "schema_version": pl.Utf8,
    }

    def __init__(
        self,
        name: str,
        config: JournalConfig,
        coordinator_name: str,
        store_path: Optional[str] = None,
        home_path: Optional[str] = None,
    ):
        """
        Initialize journal instance.

        Args:
            name: Journal name (for logging)
            config: Journal configuration
            coordinator_name: Coordinator name (from hygge.yml name)
            store_path: Store path (for location inference)
            home_path: Home path (for location inference)
        """
        self.name = name
        self.config = config
        self.coordinator_name = coordinator_name
        self.logger = get_logger(f"hygge.journal.{name}")

        # Resolve journal path
        self.journal_path = self._resolve_journal_path(store_path, home_path)
        self._write_lock = asyncio.Lock()

    def _resolve_journal_path(
        self, store_path: Optional[str], home_path: Optional[str]
    ) -> Path:
        """
        Resolve journal path from config or inference.

        Args:
            store_path: Store path (for location inference)
            home_path: Home path (for location inference)

        Returns:
            Path to journal.parquet file
        """
        # Explicit path takes precedence
        if self.config.path:
            journal_dir = Path(self.config.path)
        # Infer from location
        elif self.config.location == "store":
            if not store_path:
                raise ConfigError(
                    "Journal location='store' requires store_path, " "but none provided"
                )
            # Infer: {store_path}/.hygge_journal/
            journal_dir = Path(store_path) / ".hygge_journal"
        elif self.config.location == "home":
            if not home_path:
                raise ConfigError(
                    "Journal location='home' requires home_path, " "but none provided"
                )
            # Infer: {home_path}/.hygge_journal/
            journal_dir = Path(home_path) / ".hygge_journal"
        else:
            raise ConfigError(
                "Journal config must specify either 'path' or 'location' (store/home)"
            )

        # Create journal directory
        journal_dir.mkdir(parents=True, exist_ok=True)

        # Return path to journal.parquet
        return journal_dir / "journal.parquet"

    async def record_entity_run(
        self,
        coordinator_run_id: str,
        flow_run_id: str,
        coordinator: str,
        flow: str,
        entity: str,
        start_time: datetime,
        finish_time: Optional[datetime],
        status: str,
        run_type: str,
        row_count: Optional[int],
        duration: float,
        primary_key: Optional[str] = None,
        watermark_column: Optional[str] = None,
        watermark_type: Optional[str] = None,
        watermark: Optional[str] = None,
        message: Optional[str] = None,
    ) -> str:
        """
        Record entity run and return entity_run_id.

        Creates or appends to journal.parquet file. Uses run-based architecture
        (one row per completed entity run).

        Args:
            coordinator_run_id: Coordinator run ID (hash)
            flow_run_id: Flow run ID (hash)
            coordinator: Coordinator name
            flow: Flow name (base name, e.g., "users_flow")
            entity: Entity name (e.g., "users")
            start_time: When entity run started
            finish_time: When entity run finished (None if still running)
            status: Final status: "success", "fail", or "skip"
            run_type: Run type: "full_drop", "incremental", etc.
            row_count: Number of rows processed (None if not available)
            duration: Duration in seconds
            primary_key: Column name used as primary key
                (for watermark, None if no watermark)
            watermark_column: Column name used for watermark
                (None if no watermark)
            watermark_type: Type for watermark coercion:
                "datetime", "int", "string", or None
            watermark: Watermark value (string representation, None if no watermark)
            message: Error message, skip reason, config mismatch message, or None

        Returns:
            entity_run_id (deterministic hash)
        """
        # Generate entity_run_id
        entity_run_id = generate_run_id(
            [coordinator, flow, entity, start_time.isoformat()]
        )

        # Create row data
        row_data = {
            "entity_run_id": entity_run_id,
            "coordinator_run_id": coordinator_run_id,
            "flow_run_id": flow_run_id,
            "coordinator": coordinator,
            "flow": flow,
            "entity": entity,
            "start_time": start_time.isoformat(),
            "finish_time": finish_time.isoformat() if finish_time else None,
            "status": status,
            "run_type": run_type,
            "row_count": row_count,
            "duration": duration,
            "primary_key": primary_key,
            "watermark_column": watermark_column,
            "watermark_type": watermark_type,
            "watermark": watermark,
            "message": message,
            "schema_version": self.SCHEMA_VERSION,
        }

        # Create DataFrame from row
        new_row_df = pl.DataFrame([row_data], schema=self.JOURNAL_SCHEMA)

        # Append to journal file (thread-safe write)
        async with self._write_lock:
            await asyncio.to_thread(self._append_to_journal, new_row_df)

        self.logger.debug(
            f"Recorded entity run: {coordinator}/{flow}/{entity} "
            f"(status={status}, rows={row_count})"
        )  # noqa: E501

        return entity_run_id

    def _append_to_journal(self, new_row_df: pl.DataFrame) -> None:
        """
        Append row to journal.parquet file (synchronous, called from async context).

        Args:
            new_row_df: DataFrame with single row to append
        """
        try:
            if self.journal_path.exists():
                # Read existing journal
                existing_df = pl.read_parquet(self.journal_path)
                # Concatenate with new row
                combined_df = pl.concat([existing_df, new_row_df])
            else:
                # First write - just use new row
                combined_df = new_row_df

            # Write to a temporary file in the same directory, then atomically replace
            temp_path = self.journal_path.with_name(
                f"{self.journal_path.name}.tmp_{uuid4().hex}"
            )
            combined_df.write_parquet(temp_path)
            temp_path.replace(self.journal_path)

        except Exception as e:
            self.logger.error(f"Failed to append to journal: {str(e)}")
            raise
        finally:
            try:
                if "temp_path" in locals() and temp_path.exists():
                    temp_path.unlink()
            except Exception:
                # Best-effort cleanup; leave temp file if removal fails
                pass

    async def get_watermark(
        self,
        flow: str,
        entity: str,
        primary_key: Optional[str] = None,
        watermark_column: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Get the most recent watermark for a flow/entity.

        Validates that the requested config matches the stored config to prevent
        using incompatible watermarks (e.g., if watermark_column changed).

        Args:
            flow: Flow name
            entity: Entity name
            primary_key: Expected primary_key
                (optional, validates if provided)
            watermark_column: Expected watermark_column
                (optional, validates if provided)

        Returns:
            Dict with watermark info, or None if no watermark exists or config mismatch
            {
                "watermark": "2024-01-02T09:00:00Z",
                "watermark_type": "datetime",
                "watermark_column": "signup_date",
                "primary_key": "user_id"
            }

        Raises:
            ValueError: If provided config doesn't match stored config
        """
        if not self.journal_path.exists():
            return None

        # Read journal (thread-safe read)
        journal_df = await asyncio.to_thread(
            pl.read_parquet, self.journal_path, schema=self.JOURNAL_SCHEMA
        )

        # Filter for flow + entity + successful runs + has watermark
        watermark_records = journal_df.filter(
            (pl.col("flow") == flow)
            & (pl.col("entity") == entity)
            & (pl.col("status") == "success")
            & (pl.col("watermark").is_not_null())
        )

        if len(watermark_records) == 0:
            return None

        # Get most recent (sorted by finish_time DESC)
        most_recent = watermark_records.sort("finish_time", descending=True).head(1)

        stored_primary_key = most_recent["primary_key"][0]
        stored_watermark_column = most_recent["watermark_column"][0]

        # Validate config matches (if provided)
        if primary_key is not None and stored_primary_key != primary_key:
            raise ValueError(
                f"Watermark config mismatch for {flow}/{entity}: "
                f"stored primary_key='{stored_primary_key}' "
                f"but requested '{primary_key}'. "
                f"This indicates the watermark configuration changed. "
                f"Consider resetting watermark tracking or using a full load."
            )

        if watermark_column is not None and stored_watermark_column != watermark_column:
            raise ValueError(
                f"Watermark config mismatch for {flow}/{entity}: "
                f"stored watermark_column='{stored_watermark_column}' "
                f"but requested '{watermark_column}'. "
                f"This indicates the watermark configuration changed. "
                f"Consider resetting watermark tracking or using a full load."
            )

        return {
            "watermark": most_recent["watermark"][0],
            "watermark_type": most_recent["watermark_type"][0],
            "watermark_column": stored_watermark_column,
            "primary_key": stored_primary_key,
        }

    async def get_flow_summary(self, flow_run_id: str) -> Dict[str, Any]:
        """
        Get flow aggregation (n_entities, n_success, etc.) - computed on-demand.

        Args:
            flow_run_id: Flow run ID to aggregate

        Returns:
            Dict with flow summary statistics
        """
        if not self.journal_path.exists():
            return {
                "n_entities": 0,
                "n_success": 0,
                "n_fail": 0,
                "n_skip": 0,
                "start_time": None,
                "end_time": None,
            }

        # Read journal (thread-safe read)
        journal_df = await asyncio.to_thread(
            pl.read_parquet, self.journal_path, schema=self.JOURNAL_SCHEMA
        )

        # Filter for flow_run_id
        flow_records = journal_df.filter(pl.col("flow_run_id") == flow_run_id)

        if len(flow_records) == 0:
            return {
                "n_entities": 0,
                "n_success": 0,
                "n_fail": 0,
                "n_skip": 0,
                "start_time": None,
                "end_time": None,
            }

        # Aggregate using select with aggregation expressions
        summary = flow_records.select(
            [
                pl.len().alias("n_entities"),
                (pl.col("status") == "success").cast(pl.Int32).sum().alias("n_success"),
                (pl.col("status") == "fail").cast(pl.Int32).sum().alias("n_fail"),
                (pl.col("status") == "skip").cast(pl.Int32).sum().alias("n_skip"),
                pl.col("start_time").min().alias("start_time"),
                pl.col("finish_time").max().alias("end_time"),
            ]
        )

        # Convert to dict
        row = summary.to_dicts()[0]
        return {
            "n_entities": row["n_entities"],
            "n_success": row["n_success"],
            "n_fail": row["n_fail"],
            "n_skip": row["n_skip"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
        }

    async def get_coordinator_summary(self, coordinator_run_id: str) -> Dict[str, Any]:
        """
        Get coordinator aggregation (n_flows, etc.) - computed on-demand.

        Args:
            coordinator_run_id: Coordinator run ID to aggregate

        Returns:
            Dict with coordinator summary statistics
        """
        if not self.journal_path.exists():
            return {
                "n_flows": 0,
                "n_entities": 0,
                "start_time": None,
                "end_time": None,
            }

        # Read journal (thread-safe read)
        journal_df = await asyncio.to_thread(
            pl.read_parquet, self.journal_path, schema=self.JOURNAL_SCHEMA
        )

        # Filter for coordinator_run_id
        coordinator_records = journal_df.filter(
            pl.col("coordinator_run_id") == coordinator_run_id
        )

        if len(coordinator_records) == 0:
            return {
                "n_flows": 0,
                "n_entities": 0,
                "start_time": None,
                "end_time": None,
            }

        # Aggregate using select with aggregation expressions
        summary = coordinator_records.select(
            [
                pl.col("flow").n_unique().alias("n_flows"),
                pl.col("entity").n_unique().alias("n_entities"),
                pl.col("start_time").min().alias("start_time"),
                pl.col("finish_time").max().alias("end_time"),
            ]
        )

        # Convert to dict
        row = summary.to_dicts()[0]
        return {
            "n_flows": row["n_flows"],
            "n_entities": row["n_entities"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
        }
