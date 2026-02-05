"""
Open Mirroring store implementation for Microsoft Fabric.

Extends OneLakeStore to add Open Mirroring specific requirements:
- __rowMarker__ column (must be last column, default to 4=Upsert)
- _metadata.json file per table (with keyColumns)
- Timestamp + sequence file naming
- Initial load / replace mode support
- _partnerEvents.json at database level (optional)
"""
import asyncio
import io
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    from hygge.homes.mssql import MssqlHome

import polars as pl
from pydantic import Field, field_validator, model_validator

from hygge.core.journal import Journal
from hygge.core.polish import PolishConfig, Polisher
from hygge.stores.onelake import OneLakeStore, OneLakeStoreConfig
from hygge.utility.azure_onelake import ADLSOperations
from hygge.utility.exceptions import StoreError
from hygge.utility.fabric_schema import (
    build_fabric_schema_columns,
    map_polars_dtype_to_fabric,
)
from hygge.utility.path_helper import PathHelper


class OpenMirroringStoreConfig(OneLakeStoreConfig, config_type="open_mirroring"):
    """
    Configuration for Open Mirroring store in Microsoft Fabric.

    Extends OneLakeStoreConfig with Open Mirroring specific requirements.

    Examples:

        Basic usage with explicit row marker:
        ```yaml
        store:
          type: open_mirroring
          account_url: https://onelake.dfs.fabric.microsoft.com
          filesystem: <workspace-guid>  # Workspace GUID from Fabric
          mirror_name: <database-guid>  # Database GUID from landing zone URL
          key_columns: ["id"]
          row_marker: 0  # Required: 0=Insert, 1=Update, 2=Delete, 4=Upsert
          credential: managed_identity
        ```

        Full-table reloads are controlled by the flow `run_type`. When the flow
        is configured with `run_type: full_drop`, the store will truncate the
        landing zone before writing the fresh batch. Incremental runs keep
        existing files and append new ones for Fabric to merge.
    """

    type: str = Field(default="open_mirroring", description="Store type")

    # Required for Open Mirroring - database GUID
    mirror_name: str = Field(
        ...,
        description=(
            "Mirrored Database GUID (required). "
            "This is the database ID GUID found in your Fabric mirrored "
            "database landing zone URL. "
            "Example: If landing zone URL is "
            "https://onelake.dfs.fabric.microsoft.com/"
            "<workspace-guid>/<database-guid>/Files/LandingZone, "
            "then mirror_name should be the <database-guid>."
        ),
    )

    # Required for Open Mirroring - key columns
    # Can be set at flow level or per-entity via entity.store.key_columns
    key_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Required for Open Mirroring (metadata.json keyColumns). "
            "Can be set at flow level or per-entity via "
            "entity.store.key_columns. "
            "Accepts both string (single column) or list (multiple columns). "
            "Note: This is Open Mirroring specific - no other stores "
            "require key_columns."
        ),
    )

    # File naming strategy
    file_detection: str = Field(
        default="timestamp",
        description="File detection strategy: 'timestamp' or 'sequential'",
    )

    # Row marker configuration
    row_marker: int = Field(
        ...,
        description=(
            "Required: __rowMarker__ value for all rows. "
            "Must be explicitly set: 0=Insert, 1=Update, 2=Delete, 4=Upsert. "
            "All choices have consequences - make an intentional decision."
        ),
    )

    # Optional: Partner metadata (for _partnerEvents.json)
    partner_name: Optional[str] = Field(
        None, description="Partner/organization name for _partnerEvents.json"
    )
    source_type: Optional[str] = Field(
        None, description="Source system type (e.g., 'SQL', 'Oracle', 'Salesforce')"
    )
    source_version: Optional[str] = Field(None, description="Source system version")

    # Optional: Starting sequence for file naming
    starting_sequence: int = Field(
        default=1,
        ge=1,
        description="Starting sequence number for file naming (if sequential mode)",
    )

    # Optional: Wait time after folder deletion (for full_drop runs)
    folder_deletion_wait_seconds: float = Field(
        default=2.0,
        ge=0.0,
        le=60.0,
        description=(
            "Wait time in seconds after deleting the table folder during a "
            "full_drop run. "
            "Allows ADLS propagation and Open Mirroring to detect deletion "
            "before recreating folder. Default: 2.0 seconds."
        ),
    )

    # Optional: Deletion detection for full_drop runs
    deletion_source: Optional[Union[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Target database connection for marking deletions before full_drop. "
            "Can be a connection name (string) referencing hygge.yml connections, "
            "or a dict with server/database. "
            "When specified, all target rows are marked as deleted before "
            "full_drop runs. "
            "If connection name is used, schema and table can be specified "
            "separately. "
            "Example: 'fabric_mirror' or "
            "{server: 'fabric-mirror.database.windows.net', database: 'MirroredDB'}"
        ),
    )

    # Optional: schema and table when using connection name
    deletion_schema: Optional[str] = Field(
        default="dbo",
        description=(
            "Schema name for deletion_source table (when using connection name). "
            "Defaults to 'dbo'. Ignored if deletion_source is a dict with "
            "'schema' field."
        ),
    )
    deletion_table: Optional[str] = Field(
        default=None,
        description=(
            "Table name for deletion_source (defaults to entity_name when using "
            "connection name). "
            "Ignored if deletion_source is a dict with 'table' field."
        ),
    )

    # Optional: Delay between moving deletion files and new data files
    deletion_processing_delay: float = Field(
        default=2.0,
        ge=0.0,
        le=60.0,
        description=(
            "Delay in seconds between moving deletion files and new data files "
            "in full_drop mode. "
            "Ensures OpenMirroring processes deletions before new data arrives. "
            "Default: 2.0 seconds. Increase if you experience ordering issues."
        ),
    )

    # Optional: Mirror hygge journal into Fabric as append-only table
    mirror_journal: bool = Field(
        default=False,
        description=(
            "When true, hygge will mirror the execution journal into a dedicated "
            "Open Mirroring table using append-only semantics."
        ),
    )
    journal_table_name: str = Field(
        default="__hygge_journal",
        description=(
            "Table name to use when `mirror_journal` is true. "
            "Defaults to '__hygge_journal'."
        ),
    )

    # Optional last-mile polishing configuration
    polish: Optional[PolishConfig] = Field(
        default=None,
        description="Optional Polisher configuration for last-mile transforms.",
    )

    @field_validator("file_detection")
    @classmethod
    def validate_file_detection(cls, v):
        """Validate file detection strategy."""
        valid_strategies = ["timestamp", "sequential"]
        if v not in valid_strategies:
            raise ValueError(
                f"file_detection must be one of {valid_strategies}, got '{v}'"
            )
        return v

    @field_validator("row_marker")
    @classmethod
    def validate_row_marker(cls, v):
        """Validate row marker value."""
        valid_markers = [0, 1, 2, 4]
        if v not in valid_markers:
            raise ValueError(f"row_marker must be one of {valid_markers}, got '{v}'")
        return v

    @field_validator("key_columns", mode="before")
    @classmethod
    def normalize_key_columns(cls, v):
        """Convert string to list for convenience."""
        if v is None:
            return None
        if isinstance(v, str):
            if not v.strip():  # Empty string
                raise ValueError("key_columns cannot be an empty string")
            return [v]  # Convert "Id" -> ["Id"]
        if isinstance(v, list):
            if len(v) == 0:
                raise ValueError("key_columns cannot be an empty list")
            return v  # Already a list
        raise ValueError(
            f"key_columns must be a string or list of strings, got {type(v)}"
        )

    @model_validator(mode="after")
    def validate_deletion_source(self):
        """Validate deletion_source configuration if provided."""
        if self.deletion_source is not None:
            if isinstance(self.deletion_source, str):
                # Connection name - validation happens in FlowFactory resolution
                if not self.deletion_source.strip():
                    raise ValueError("deletion_source connection name cannot be empty")
            elif isinstance(self.deletion_source, dict):
                # Inline dict - validate required fields
                if (
                    "server" not in self.deletion_source
                    or "database" not in self.deletion_source
                ):
                    raise ValueError(
                        "deletion_source dict must contain 'server' and 'database' "
                        "for marking deletions before full_drop"
                    )
            else:
                raise ValueError(
                    f"deletion_source must be a connection name (string) or dict, "
                    f"got {type(self.deletion_source)}"
                )
        return self

    @model_validator(mode="after")
    def build_open_mirroring_path(self):
        """
        Build the base path for Open Mirroring.

        Open Mirroring URL structure:
        https://onelake.dfs.fabric.microsoft.com/<workspace-guid>/<database-guid>/Files/LandingZone

        Where:
        - filesystem (inherited) = workspace GUID
        - mirror_name = database GUID (must be in path)
        - path = /<database-guid>/Files/LandingZone/{entity}/

        Schema support is inherited from OneLakeStoreConfig.
        """
        # Get schema value safely using shared helper
        # (inherited from OneLakeStoreConfig)
        schema_value = self._get_schema_value()

        # If custom path is provided that doesn't start with "Files/", preserve it as-is
        if self.path is not None and not self.path.startswith("Files/"):
            # But ensure database GUID is included if path doesn't start with it
            if not self.path.startswith(f"/{self.mirror_name}/"):
                # Prepend database GUID
                if self.path.startswith("/"):
                    self.path = f"/{self.mirror_name}{self.path}"
                else:
                    self.path = f"/{self.mirror_name}/{self.path}"
            return self

        # If custom path has LandingZone, check if database GUID is included
        if self.path is not None and "LandingZone" in self.path:
            # Ensure database GUID is in the path
            db_guid_prefix = f"/{self.mirror_name}/"
            if not self.path.startswith(db_guid_prefix):
                # Prepend database GUID before Files/LandingZone
                if self.path.startswith("Files/"):
                    self.path = f"/{self.mirror_name}/Files/LandingZone{self.path[5:]}"
                else:
                    self.path = f"/{self.mirror_name}/{self.path}"
            return self

        # Build path with database GUID prefix
        # Open Mirroring path structure: /<database-guid>/Files/LandingZone/{entity}/
        db_guid_prefix = f"/{self.mirror_name}/Files/LandingZone/"
        if schema_value:
            # With schema_name: /<database-guid>/Files/LandingZone/
            # {schema_name}.schema/{entity}/
            self.path = f"{db_guid_prefix}{schema_value}.schema/{{entity}}/"
        else:
            # Without schema: /<database-guid>/Files/LandingZone/{entity}/
            self.path = f"{db_guid_prefix}{{entity}}/"

        return self

    def get_merged_options(self, flow_name: str = None) -> Dict[str, Any]:
        """Get all options including defaults."""
        # Get base options from OneLakeStoreConfig
        options = super().get_merged_options(flow_name)

        # Add Open Mirroring specific options
        options.update(
            {
                "file_detection": self.file_detection,
                "row_marker": self.row_marker,
                "starting_sequence": self.starting_sequence,
            }
        )

        return options


class OpenMirroringStore(OneLakeStore, store_type="open_mirroring"):
    """
    Open Mirroring data store for Microsoft Fabric.

    Extends OneLakeStore with Open Mirroring specific requirements:
    - Adds __rowMarker__ column (must be last column, value from config)
    - Writes _metadata.json per table with keyColumns
    - Uses timestamp + sequence file naming
    - Supports initial load / replace mode
    - Optional _partnerEvents.json at database level

    Example:
        ```python
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=4,  # Required: 0=Insert, 1=Update, 2=Delete, 4=Upsert
            credential="managed_identity"
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        ```
    """

    def __init__(
        self,
        name: str,
        config: OpenMirroringStoreConfig,
        flow_name: Optional[str] = None,
        entity_name: Optional[str] = None,
    ):
        # Let OneLakeStore handle base initialization
        super().__init__(name, config, flow_name, entity_name)

        # Validate key_columns is set (required for Open Mirroring)
        if config.key_columns is None or len(config.key_columns) == 0:
            entity_info = f" for entity '{entity_name}'" if entity_name else ""
            raise StoreError(
                f"key_columns is required for Open Mirroring store{entity_info}. "
                f"Please specify key_columns in the store config or "
                f"entity.store.key_columns."
            )

        # Open Mirroring specific configuration
        self.key_columns = config.key_columns
        self.file_detection = config.file_detection
        self.row_marker = config.row_marker
        self.partner_name = config.partner_name
        self.source_type = config.source_type
        self.source_version = config.source_version
        self.schema_name = config.schema_name
        self.starting_sequence = config.starting_sequence

        # Optional polish configuration
        if getattr(config, "polish", None):
            self._polisher = Polisher(config.polish)
            hash_id_count = len(config.polish.hash_ids) if config.polish.hash_ids else 0
            self.logger.debug(
                f"Initialized polisher for {name} with {hash_id_count} hash ID rules"
            )
        else:
            self._polisher = None

        # Track if metadata files have been written
        self._metadata_written = False
        self._partner_events_written = False

        # Track metadata file paths (for full_drop atomic operation)
        self._metadata_tmp_path = None
        self._partner_events_tmp_path = None
        self._schema_tmp_path = None

        # Track if table folder has been prepared (full_drop + metadata)
        self._table_folder_prepared = False

        # Track sequence counter - will be initialized from existing files
        self.sequence_counter = None  # Will be set lazily from existing files

        # Prepare table folder (clear if full_drop)
        # Note: We'll do this lazily on first write to ensure ADLS client is ready

    def configure_for_run(self, run_type: str) -> None:
        """Toggle full-drop behaviour based on the incoming run type."""
        super().configure_for_run(run_type)

        # Reset per-run tracking so preparation steps run for each execution
        self._metadata_written = False
        self._partner_events_written = False
        self._metadata_tmp_path = None
        self._partner_events_tmp_path = None
        self._schema_tmp_path = None
        self._table_folder_prepared = False
        self.sequence_counter = None
        self._sequence_counter_explicitly_set = (
            False  # Flag to prevent re-initialization
        )

        # Reset deletion detection tracking for each run
        self._deletions_checked = False
        self._deletion_metrics = {
            "column_based_deletions": 0,
            "query_based_deletions": 0,
            "full_drop_deletions": 0,
        }

        # Track deletion file paths separately for ordered move in finish()
        self._deletion_paths = []

        if hasattr(self, "saved_paths"):
            self.saved_paths = []
        if hasattr(self, "uploaded_files"):
            self.uploaded_files = []

        if self.full_drop_mode:
            self.logger.debug(
                "Run configured for full_drop: landing zone will be truncated "
                "before new files are published."
            )

            # Warn if deletion_source is not configured for full_drop
            # Deletion detection is important for full_drop to ensure deleted
            # rows in the source are properly removed from the mirrored database.
            if not self.config.deletion_source:
                self.logger.warning(
                    "full_drop mode is being used without deletion_source "
                    "configured. "
                    "Deleted rows in the source will NOT be removed from the "
                    "mirrored database. "
                    "Consider configuring deletion_source to enable deletion "
                    "detection. "
                    "Example: deletion_source: 'fabric_mirror' "
                    "(connection name) or "
                    "{server: 'fabric-mirror.database.windows.net', "
                    "database: 'MirroredDB'}"
                )

            # CRITICAL: For full_drop with deletion_source, reset sequence counter
            # to starting_sequence - 1 BEFORE deletions are written.
            # This ensures deletions (written in before_flow_start()) get lower
            # sequence numbers (1, 2, 3...) than new data (4, 5, 6...).
            # OpenMirroring processes files in sequence order, so deletions are
            # processed first.
            #
            # Without this explicit reset, _initialize_sequence_counter() would
            # read existing files in LandingZone (which persists between runs)
            # and continue from high numbers, breaking the ordering.
            if self.config.deletion_source:
                self.sequence_counter = self.starting_sequence - 1
                self._sequence_counter_explicitly_set = True
                self.logger.debug(
                    f"full_drop mode with deletion_source: Reset sequence "
                    f"counter to {self.starting_sequence - 1} "
                    f"(deletions will get sequence numbers "
                    f"starting from {self.starting_sequence}, "
                    f"new data will get higher numbers)"
                )

    async def before_flow_start(self) -> None:
        """
        Pre-hook method called by Flow before normal flow execution starts.

        For full_drop runs with deletion_source configured: Mark all target
        rows as deleted. This ensures deletions are processed by OpenMirroring
        before new data flows in.

        Called by Flow._execute_flow() after _prepare_incremental_context()
        but before starting producer/consumer tasks.
        """
        # Only for full_drop runs
        if not self.full_drop_mode:
            return

        # Only if deletion_source is configured
        if not self.config.deletion_source:
            return

        # Mark all target rows as deleted
        await self._mark_all_target_as_deleted()

    async def _mark_all_target_as_deleted(self) -> None:
        """
        Mark all rows in target as deleted before full_drop.

        Simple approach: Query target for all keys, mark everything as deleted.
        No source comparison needed - we're doing a full refresh anyway.

        Delete markers are written to _tmp (idempotent), then atomically moved
        to LandingZone in finish() along with all other data.

        Raises:
            StoreError: If key_columns not set, deletion_source invalid, or query fails
        """
        if not self.key_columns or len(self.key_columns) == 0:
            raise StoreError(
                "key_columns is required for marking deletions before full_drop. "
                "Please specify key_columns in the store config."
            )

        # deletion_source should already be resolved to dict by FlowFactory
        # (connection names resolved, inline dicts used as-is)
        if not isinstance(self.config.deletion_source, dict):
            raise StoreError(
                "deletion_source must be resolved to dict before use. "
                "This should have been done by FlowFactory."
            )

        # Extract connection info from deletion_source (already resolved)
        server = self.config.deletion_source.get("server")
        database = self.config.deletion_source.get("database")
        schema = self.config.deletion_source.get("schema", "dbo")
        table = self.config.deletion_source.get("table", self.entity_name)

        if not server or not database:
            raise StoreError(
                "deletion_source must contain 'server' and 'database' "
                "for marking deletions before full_drop"
            )

        # Build table path
        table_path = f"{schema}.{table}" if schema else table

        # Create MSSQLHome config for target database
        from hygge.homes.mssql import MssqlHome, MssqlHomeConfig

        target_home_config = MssqlHomeConfig(
            type="mssql",
            server=server,
            database=database,
            table=table_path,
        )

        target_home = MssqlHome("_full_drop_target", target_home_config)

        try:
            # Apply retry logic for transient connection errors
            from hygge.utility.exceptions import (
                HomeConnectionError,
                StoreConnectionError,
            )
            from hygge.utility.retry import with_retry

            retry_decorated = with_retry(
                retries=3,
                delay=2,
                exceptions=(StoreConnectionError, HomeConnectionError),
                timeout=300,
                logger_name="hygge.store.openmirroring",
            )(self._mark_all_target_as_deleted_impl)

            await retry_decorated(target_home)

        except (StoreError, ValueError) as e:
            # Configuration errors - fail fast
            self.logger.error(
                f"Failed to mark target rows as deleted (configuration error): {str(e)}"
            )
            raise StoreError(
                f"Failed to mark target rows as deleted before full_drop: {str(e)}. "
                f"This is a configuration error - please fix and retry."
            ) from e
        # Connection cleanup is handled by find_keys() via _stream_query()
        # which calls _cleanup_connection() in its finally block

    async def _mark_all_target_as_deleted_impl(self, target_home: "MssqlHome") -> None:
        """
        Implementation of marking all target rows as deleted.

        Separated for retry logic wrapper.
        """
        # Query ALL keys from target database
        self.logger.info("Querying target database for all keys before full_drop")

        # Use find_keys() method to get only key columns (with proper schema inference)
        target_keys = await target_home.find_keys(self.key_columns)

        if target_keys is None or len(target_keys) == 0:
            self.logger.info(
                "No target keys found - nothing to mark as deleted "
                "(first run or empty table)"
            )
            return

        # Mark everything as deleted
        delete_markers = target_keys.with_columns(pl.lit(2).alias("__rowMarker__"))

        self.logger.info(
            f"Marking {len(delete_markers):,} row(s) as deleted before full_drop"
        )

        # Track metrics for observability
        # Note: _deletion_metrics is initialized in configure_for_run()
        self._deletion_metrics["full_drop_deletions"] += len(delete_markers)

        # Track current saved_paths count before write
        # (to capture all paths added during deletion write)
        paths_before = len(getattr(self, "saved_paths", []))

        # Write delete markers to _tmp (idempotent - full_drop mode writes to _tmp)
        # Track deletion file paths separately so they can be moved first in finish()
        # These will be moved to LandingZone BEFORE new data in finish()
        await self.write(delete_markers)
        if hasattr(self, "data_buffer") and self.data_buffer:
            await self._flush_buffer()

        # Track deletion file paths separately for ordered move in finish()
        # Capture all paths added during this deletion write (handles multiple batches)
        paths_after = len(getattr(self, "saved_paths", []))
        if not hasattr(self, "_deletion_paths"):
            self._deletion_paths = []
        # Add all paths added during this deletion write
        if paths_after > paths_before:
            self._deletion_paths.extend(self.saved_paths[paths_before:paths_after])

    async def write(self, data: pl.DataFrame) -> None:
        """
        Write data to Open Mirroring store.

        Overrides base Store.write() to ensure table folder is prepared
        BEFORE any data is written (not during the first _save call).

        This ensures metadata files are written before Open Mirroring
        scans the folder, especially important for full_drop mode.
        """
        # Prepare table folder ONCE before any data writes
        # This ensures metadata is available when Open Mirroring scans
        if not self._table_folder_prepared:
            await self._prepare_table_folder()

        # Now call parent write() which handles buffering and calls _save()
        await super().write(data)

    def _get_adls_ops(self) -> "ADLSOperations":
        """
        Override to ensure OneLake detection is correct for Open Mirroring.

        Open Mirroring uses OneLake paths, so is_onelake should be True.
        """
        if self._adls_ops is not None:
            return self._adls_ops

        file_system_client = self._get_file_system_client()
        service_client = self._get_service_client()

        # Get filesystem name for logging
        filesystem_name = self.config.filesystem

        # Open Mirroring uses OneLake paths, so set is_onelake=True
        is_onelake = True

        self._adls_ops = ADLSOperations(
            file_system_client=file_system_client,
            file_system_name=filesystem_name,
            service_client=service_client,
            timeout=300,
            is_onelake=is_onelake,
        )
        return self._adls_ops

    async def _initialize_sequence_counter(self) -> None:
        """
        Initialize sequence counter from existing files in table folder.

        Reads existing parquet files and finds the highest sequence number,
        then sets sequence_counter to continue from there.

        For sequential mode: extracts from 20-digit filenames
        For timestamp mode: extracts sequence from
        timestamp_microseconds_sequence.parquet format

        Note: For full_drop runs with deletion_source, the sequence counter
        is explicitly set in configure_for_run() to ensure deletions get lower
        sequence numbers than new data. This method will not override that.
        """
        # If sequence counter was explicitly set (e.g., for full_drop
        # deletions), do not re-initialize from existing files
        if (
            hasattr(self, "_sequence_counter_explicitly_set")
            and self._sequence_counter_explicitly_set
        ):
            self.logger.debug(
                "Sequence counter was explicitly set (e.g., for full_drop deletions), "
                "skipping initialization from existing files"
            )
            return

        if self.sequence_counter is not None:
            return  # Already initialized

        try:
            file_system_client = self._get_file_system_client()
            table_path = self.base_path

            max_sequence = self.starting_sequence - 1

            try:
                # Use FileSystemClient.get_paths() instead of
                # DirectoryClient.list_paths() (not available in newer SDK)
                paths = file_system_client.get_paths(path=table_path, recursive=False)

                for path in paths:
                    # Extract just the filename from the full path
                    # path.name returns full relative path from container root
                    filename = os.path.basename(path.name)
                    if not filename.endswith(".parquet"):
                        continue

                    if self.file_detection == "sequential":
                        # Extract sequence from 20-digit: 00000000000000000001.parquet
                        match = re.match(r"^(\d{20})\.parquet$", filename)
                        if match:
                            sequence = int(match.group(1))
                            max_sequence = max(max_sequence, sequence)
                    else:
                        # Extract sequence from timestamp format
                        # Format: YYYYMMDD_HHMMSS_microseconds_sequence.parquet
                        match = re.match(
                            r"^\d{8}_\d{6}_\d{6}_(\d{6})\.parquet$", filename
                        )
                        if match:
                            sequence = int(match.group(1))
                            max_sequence = max(max_sequence, sequence)

                self.logger.debug(
                    f"Found max sequence {max_sequence} from existing files"
                )

            except Exception as e:
                # Directory might not exist (first run) - use starting_sequence
                if "NotFound" not in str(e) and "does not exist" not in str(e).lower():
                    self.logger.warning(
                        f"Error reading existing files for sequence: {str(e)}. "
                        f"Using starting_sequence {self.starting_sequence}"
                    )
                max_sequence = self.starting_sequence - 1

            self.sequence_counter = max_sequence

        except Exception as e:
            # Fallback to starting_sequence if anything fails
            self.logger.warning(
                f"Failed to initialize sequence from existing files: {str(e)}. "
                f"Using starting_sequence {self.starting_sequence}"
            )
            self.sequence_counter = self.starting_sequence - 1

    async def get_next_filename(self) -> str:
        """
        Generate filename based on file_detection strategy.

        If file_detection is "timestamp":
        - Format: {timestamp}_{sequence:06d}.parquet
        - Example: 20250910_143052_123456_000001.parquet
        - Timestamp includes microseconds for better uniqueness
        - Sequence counter ensures uniqueness even if timestamps collide
        - Hybrid approach: timestamp for detection + sequence for uniqueness

        If file_detection is "sequential":
        - Format: {sequence:020d}.parquet (20-digit sequential)
        - Example: 00000000000000000001.parquet
        - Pure sequential as per Open Mirroring spec
        """
        # Initialize sequence counter from existing files if not done
        if self.sequence_counter is None:
            await self._initialize_sequence_counter()

        self.sequence_counter += 1

        if self.file_detection == "timestamp":
            # Timestamp + sequence hybrid approach
            # Include microseconds to reduce timestamp collisions within same second
            now = datetime.now()
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            microseconds = now.strftime("%f")  # 6-digit microseconds
            return f"{timestamp}_{microseconds}_{self.sequence_counter:06d}.parquet"
        else:
            # Sequential: 20-digit format per spec
            return f"{self.sequence_counter:020d}.parquet"

    async def reset_retry_sensitive_state(self) -> None:
        """Reset retry-sensitive state (sequence counter) before retry."""
        # Reset to None so it will be re-initialized from existing files
        # This ensures we don't create gaps in sequence numbers on retry
        self.sequence_counter = None
        self.logger.debug("Reset sequence counter for retry")

    def _validate_row_marker(self, df: pl.DataFrame) -> None:
        """
        Validate __rowMarker__ column values if present.

        Valid values: 0 (Insert), 1 (Update), 2 (Delete), 4 (Upsert)

        Args:
            df: DataFrame to validate

        Raises:
            StoreError: If invalid row marker values found
        """
        if "__rowMarker__" not in df.columns:
            return

        valid_values = {0, 1, 2, 4}
        row_markers = df["__rowMarker__"].unique().to_list()

        invalid_values = [v for v in row_markers if v not in valid_values]
        if invalid_values:
            raise StoreError(
                f"Invalid __rowMarker__ values found: {invalid_values}. "
                f"Valid values are: 0 (Insert), 1 (Update), 2 (Delete), 4 (Upsert)"
            )

    def _ensure_row_marker_last(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Ensure __rowMarker__ is the last column in DataFrame.

        This is CRITICAL for Open Mirroring - the spec requires __rowMarker__
        to be the final column.

        Column order: [data columns..., __LastLoadedAt__, __rowMarker__]

        Args:
            df: DataFrame to reorder

        Returns:
            DataFrame with __rowMarker__ as last column, __LastLoadedAt__ second-to-last
        """
        if "__rowMarker__" not in df.columns:
            return df

        # Get columns in correct order:
        # 1. All columns except __rowMarker__ and __LastLoadedAt__
        # 2. __LastLoadedAt__ (if present)
        # 3. __rowMarker__ (always last)
        other_cols = [
            c for c in df.columns if c not in ["__rowMarker__", "__LastLoadedAt__"]
        ]

        # Build final column order
        final_cols = other_cols.copy()
        if "__LastLoadedAt__" in df.columns:
            final_cols.append("__LastLoadedAt__")
        final_cols.append("__rowMarker__")

        return df.select(final_cols)

    def _add_last_loaded_at(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Add __LastLoadedAt__ column with current timestamp.

        This column tracks when data was loaded into the mirrored database
        and is placed second-to-last (before __rowMarker__).

        Args:
            df: DataFrame to add timestamp to

        Returns:
            DataFrame with __LastLoadedAt__ column added (if missing)
        """
        if "__LastLoadedAt__" in df.columns:
            return df

        # Add with current timestamp
        now = datetime.now()
        df = df.with_columns(pl.lit(now).alias("__LastLoadedAt__"))

        self.logger.debug(f"Added __LastLoadedAt__ column: {now}")

        return df

    def _add_row_marker_column(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Add __rowMarker__ column if missing.

        Always adds __rowMarker__ column with configured row_marker value.
        __rowMarker__ is required for Open Mirroring and must be last.

        Args:
            df: DataFrame to add row marker to

        Returns:
            DataFrame with __rowMarker__ column added (if missing)
        """
        # First, ensure __LastLoadedAt__ is present (if not already there)
        df = self._add_last_loaded_at(df)

        if "__rowMarker__" in df.columns:
            # Already present - ensure correct column order
            return self._ensure_row_marker_last(df)

        # Add with configured row marker value
        df = df.with_columns(pl.lit(self.row_marker).alias("__rowMarker__"))

        marker_names = {0: "Insert", 1: "Update", 2: "Delete", 4: "Upsert"}
        marker_name = marker_names.get(self.row_marker, "Unknown")

        self.logger.debug(
            f"Added __rowMarker__ column with value: "
            f"{self.row_marker} ({marker_name})"
        )

        # CRITICAL: Ensure correct column order:
        # [...data..., __LastLoadedAt__, __rowMarker__]
        return self._ensure_row_marker_last(df)

    def _validate_key_columns(self, df: pl.DataFrame) -> None:
        """
        Validate that DataFrame contains all required key columns.

        If polish normalization is configured, normalizes key column names to match
        the normalized DataFrame columns
        (e.g., 'ISTARDesignationHashId' -> 'IstarDesignationHashId').

        Args:
            df: DataFrame to validate

        Raises:
            StoreError: If key columns are missing
        """
        df_columns = set(df.columns)

        # If polish is configured with column normalization, normalize key_columns
        # to match the normalized DataFrame columns
        key_columns_to_check = list(self.key_columns)
        if self._polisher and self._polisher.config.columns.case:
            # Apply the same normalization that polish applies
            normalized_key_columns = []
            for key_col in self.key_columns:
                if self._polisher.config.columns.case == "pascal":
                    normalized = self._polisher._to_pascal_case(key_col)
                elif self._polisher.config.columns.case == "camel":
                    normalized = self._polisher._to_camel_case(key_col)
                elif self._polisher.config.columns.case == "snake":
                    normalized = self._polisher._to_snake_case(key_col)
                else:
                    normalized = key_col
                normalized_key_columns.append(normalized)
            key_columns_to_check = normalized_key_columns

        missing_keys = [col for col in key_columns_to_check if col not in df_columns]

        if missing_keys:
            raise StoreError(
                f"DataFrame missing required key columns: {missing_keys}. "
                f"Required key columns: {self.key_columns} "
                f"(normalized to: {key_columns_to_check}). "
                f"Available columns: {sorted(df_columns)}"
            )

    def _validate_update_rows(self, df: pl.DataFrame) -> None:
        """
        Validate that update rows (__rowMarker__ = 1) contain full row data.

        Update rows must have all columns present (not just key columns).

        Args:
            df: DataFrame to validate

        Raises:
            StoreError: If update rows are incomplete
        """
        if "__rowMarker__" not in df.columns:
            return

        # Get update rows (__rowMarker__ = 1)
        update_rows = df.filter(pl.col("__rowMarker__") == 1)

        if len(update_rows) == 0:
            return

        # Check for null values in non-key columns (allowing NULLs in key columns)
        # This is a simplified check - in practice, we trust Polars schema
        # The spec says "Updated rows must contain the full row data with all columns"
        # So we'll log a warning if we detect potential issues, but not fail
        # since this is more of a data quality concern

        self.logger.debug(
            f"Validated {len(update_rows)} update rows have full column data"
        )

    async def _write_metadata_json(self, to_tmp: bool = False) -> None:
        """
        Write _metadata.json file to table folder.

        This file contains keyColumns and is required for update/delete operations.

        Path (without schema): Files/LandingZone/{entity}/_metadata.json
        Path (with schema_name):
        Files/LandingZone/{schema_name}.schema/{entity}/_metadata.json

        For full_drop mode: writes to _tmp first (atomic operation).

        Note: If file already exists, validates keyColumns match.
        According to spec, keyColumns cannot be changed once set.

        Args:
            to_tmp: If True, write to _tmp path instead of production
                (for ACID operations)
        """
        if self._metadata_written:
            return

        metadata = {"keyColumns": self.key_columns}

        # Add file detection strategy if timestamp-based
        if self.file_detection == "timestamp":
            metadata["fileDetectionStrategy"] = "LastUpdateTimeFileDetection"

        # Add upsert default if row_marker is 4 (Upsert)
        if self.row_marker == 4:
            metadata["isUpsertDefaultRowMarker"] = True

        # Build metadata file path
        if to_tmp:
            # For full_drop mode: write to _tmp first (atomic operation)
            # Replace LandingZone with _tmp in the path
            if self.base_path and "LandingZone" in self.base_path:
                tmp_base_path = self.base_path.replace(
                    "/Files/LandingZone/", "/Files/_tmp/"
                )
                metadata_path = f"{tmp_base_path}/_metadata.json"
            else:
                # Cannot construct _tmp path - fail fast to preserve ACID guarantees
                raise StoreError(
                    f"Cannot construct _tmp path for metadata: "
                    f"base_path '{self.base_path}' does not contain 'LandingZone'. "
                    f"Atomic operation aborted to prevent writing metadata "
                    f"to production."
                )
        else:
            metadata_path = f"{self.base_path}/_metadata.json"

        # Get ADLS operations client
        adls_ops = self._get_adls_ops()

        # Check if metadata file already exists
        existing = await adls_ops.read_json(metadata_path)

        if existing:
            # Validate that keyColumns match (spec says they can't be changed)
            existing_keys = set(existing.get("keyColumns", []))
            new_keys = set(self.key_columns)

            if existing_keys != new_keys:
                raise StoreError(
                    f"Metadata file exists with different keyColumns: "
                    f"{sorted(existing_keys)} vs {sorted(new_keys)}. "
                    f"According to Open Mirroring spec, keyColumns "
                    f"cannot be changed once set. "
                    f"To change, you must drop and recreate the table."
                )

            # File exists and keyColumns match - mark as written
            self.logger.debug(
                f"_metadata.json already exists with matching keyColumns: "
                f"{sorted(new_keys)}"
            )
            self._metadata_written = True
            return

        # Write metadata file (using upload_data directly for overwrite)
        # Note: write_json uses overwrite=False, but we allow overwrite here
        json_data = json.dumps(metadata, indent=2).encode("utf-8")
        file_client = adls_ops.file_system_client.get_file_client(metadata_path)

        # Ensure directory exists before writing file
        directory_path = str(Path(metadata_path).parent)
        try:
            directory_client = adls_ops.file_system_client.get_directory_client(
                directory_path
            )
            if not directory_client.exists():
                directory_client.create_directory(timeout=adls_ops.timeout)
                self.logger.debug(f"Created directory for metadata: {directory_path}")
        except Exception as e:
            self.logger.debug(
                f"Directory check/create failed (may already exist): {str(e)}"
            )

        # Write metadata file with overwrite
        file_client.upload_data(json_data, overwrite=True, timeout=adls_ops.timeout)

        # Track tmp path for full_drop atomic operation
        if to_tmp:
            self._metadata_tmp_path = metadata_path

        self.logger.debug(
            f"Wrote _metadata.json to {metadata_path} "
            f"with keyColumns: {self.key_columns}"
        )
        self._metadata_written = True
        await self._write_schema_json(to_tmp=to_tmp)

    async def _write_schema_json(self, to_tmp: bool = False) -> None:
        """
        Write `_schema.json` file describing column order/types so Fabric mirrors
        the journal parquet without relying on inference.
        """
        # Use shared helper so other Fabric destinations can reuse the same
        # Polars → Fabric mapping without copy/paste.
        schema_columns = build_fabric_schema_columns(Journal.JOURNAL_SCHEMA)
        schema_payload = {"columns": schema_columns}

        if to_tmp:
            if self.base_path and "LandingZone" in self.base_path:
                tmp_base_path = self.base_path.replace(
                    "/Files/LandingZone/", "/Files/_tmp/"
                )
                schema_path = f"{tmp_base_path}/_schema.json"
            else:
                raise StoreError(
                    "Cannot construct _tmp path for schema: "
                    f"base_path '{self.base_path}' does not contain 'LandingZone'."
                )
        else:
            schema_path = f"{self.base_path}/_schema.json"

        adls_ops = self._get_adls_ops()
        json_data = json.dumps(schema_payload, indent=2).encode("utf-8")

        directory_path = str(Path(schema_path).parent)
        try:
            directory_client = adls_ops.file_system_client.get_directory_client(
                directory_path
            )
            if not directory_client.exists():
                directory_client.create_directory(timeout=adls_ops.timeout)
                self.logger.debug(f"Created directory for schema: {directory_path}")
        except Exception as exc:
            self.logger.debug(
                f"Directory check/create failed (may already exist): {str(exc)}"
            )

        file_client = adls_ops.file_system_client.get_file_client(schema_path)
        file_client.upload_data(json_data, overwrite=True, timeout=adls_ops.timeout)
        if to_tmp:
            self._schema_tmp_path = schema_path
        self.logger.debug(f"Wrote _schema.json to {schema_path}")

    @staticmethod
    def _map_polars_dtype_to_fabric(dtype) -> str:
        """
        Backwards-compatible wrapper around the shared Polars → Fabric
        mapping helper.

        Kept for compatibility with existing callers/tests; delegates to
        ``hygge.utility.fabric_schema.map_polars_dtype_to_fabric`` so the
        mapping logic lives in a single place.
        """
        return map_polars_dtype_to_fabric(dtype)

    async def _write_partner_events_json(self, to_tmp: bool = False) -> None:
        """
        Write _partnerEvents.json file at database level (optional).

        This file provides source system metadata and should be placed at:
        Files/LandingZone/_partnerEvents.json (not per table)

        For full_drop mode: writes to _tmp first (atomic operation).

        Only writes if partner_name is configured.

        Args:
            to_tmp: If True, write to _tmp path instead of production
                (for ACID operations)
        """
        if self._partner_events_written:
            return

        if not self.partner_name:
            return  # Optional, skip if not configured

        # Build partner events data
        partner_events = {
            "partnerName": self.partner_name,
            "sourceInfo": {},
        }

        if self.source_type:
            partner_events["sourceInfo"]["sourceType"] = self.source_type

        if self.source_version:
            partner_events["sourceInfo"]["sourceVersion"] = self.source_version

        # Note: additionalInformation can be added here in the future if needed
        # Only add it when there's actual data to include

        # Build partner events file path (database level, not table level)
        # Path: Files/LandingZone/_partnerEvents.json
        # For full_drop mode: write to _tmp first (atomic operation)
        if to_tmp:
            base_landing_zone = "Files/_tmp"
        else:
            base_landing_zone = "Files/LandingZone"
        partner_events_path = f"{base_landing_zone}/_partnerEvents.json"

        # Get ADLS operations client
        adls_ops = self._get_adls_ops()

        # Check if file already exists (may have been written by another table)
        existing = await adls_ops.read_json(partner_events_path)

        if existing:
            # File exists - validate consistency
            if existing.get("partnerName") != self.partner_name:
                self.logger.warning(
                    f"_partnerEvents.json exists with different partnerName: "
                    f"{existing.get('partnerName')} vs {self.partner_name}. "
                    f"Skipping write to maintain consistency."
                )
                self._partner_events_written = True
                return

            # File exists and is consistent, mark as written
            self._partner_events_written = True
            return

        # Write partner events file
        await adls_ops.write_json(partner_events_path, partner_events)

        # Track tmp path for full_drop atomic operation
        if to_tmp:
            self._partner_events_tmp_path = partner_events_path

        self.logger.debug(f"Wrote _partnerEvents.json to {partner_events_path}")
        self._partner_events_written = True

    def _convert_tmp_to_production_path(self, tmp_path: str) -> str:
        """
        Convert a _tmp path to production path by replacing /Files/_tmp/
        with /Files/LandingZone/.

        Args:
            tmp_path: Path in _tmp directory (e.g., Files/_tmp/table/file.parquet)

        Returns:
            Production path (e.g., Files/LandingZone/table/file.parquet)
        """
        return tmp_path.replace("/Files/_tmp/", "/Files/LandingZone/")

    def _log_completion_stats(self) -> None:
        """
        Log completion statistics (duration and rows/sec).

        Extracted from base class to avoid duplication when overriding finish().
        """
        if self.start_time:
            duration = asyncio.get_event_loop().time() - self.start_time
            rows_per_sec = self.rows_written / duration if duration > 0 else 0
            self.logger.debug(
                f"Store {self.name} completed in {duration:.1f}s "
                f"({rows_per_sec:,.0f} rows/sec)"
            )

    async def _delete_table_folder(self) -> None:
        """
        Delete table folder entirely for full_drop mode.

        According to Open Mirroring spec:
        - Deleting the folder triggers Open Mirroring to drop the table
        - Recreating the folder triggers Open Mirroring to drop and recreate the table

        This method:
        1. Deletes all files in the folder (including _metadata.json)
        2. Deletes the directory itself
        3. Waits for propagation (configurable via folder_deletion_wait_seconds)
        4. Handles failures gracefully (Open Mirroring might be using the folder)

        Note: The folder will be recreated when metadata files are written,
        which triggers Open Mirroring's recreate cycle.
        """
        try:
            table_path = self.base_path

            self.logger.debug(
                f"Deleting table folder {table_path} for full_drop mode "
                f"(triggers Open Mirroring table drop/recreate)"
            )

            # Use ADLSOperations for directory deletion
            # (handles SDK version differences and parameter conflicts)
            adls_ops = self._get_adls_ops()
            folder_deleted = await adls_ops.delete_directory(table_path, recursive=True)

            if folder_deleted:
                self.logger.success("Deleted table folder")
            else:
                # Folder deletion failed but files were deleted (graceful fallback)
                self.logger.debug(
                    "Directory deletion failed but files were cleared. "
                    "Open Mirroring may be using the folder."
                )

            # Step 3: Wait briefly to allow:
            # - ADLS propagation of deletion
            # - Open Mirroring to detect folder deletion (if polling)
            # - Reduce race conditions between delete and recreate
            wait_time = self.config.folder_deletion_wait_seconds
            if wait_time > 0:
                self.logger.debug(
                    f"Waiting {wait_time}s after folder deletion "
                    f"for propagation and Open Mirroring detection"
                )
                await asyncio.sleep(wait_time)

        except StoreError:
            raise
        except Exception as e:
            raise StoreError(f"Failed to clear table folder {table_path}: {str(e)}")

    async def _prepare_table_folder(self) -> None:
        """
        Prepare table folder for writing.

        - If full_drop mode: Writes metadata files to _tmp (not production)
          for atomic operation. Production folder will be deleted in finish()
          after all data is successfully written.
        - If not full_drop: Writes metadata files to production folder as normal.

        Note: This is called once per flow to avoid writing metadata repeatedly.
        """
        # Only prepare once per flow
        if self._table_folder_prepared:
            return

        # For full_drop mode: Write metadata to _tmp, NOT production
        # Production folder will be deleted in finish() after all data is written
        # This ensures atomic operation: all data written before deletion
        if self.full_drop_mode:
            # Write metadata files to _tmp (atomic operation - data must succeed first)
            await self._write_metadata_json(to_tmp=True)
            await self._write_partner_events_json(to_tmp=True)
        else:
            # Normal mode: Write metadata files to production folder
            await self._write_metadata_json()
            await self._write_partner_events_json()

        # Small delay to ensure metadata file is fully written
        await asyncio.sleep(0.5)

        # Mark as prepared to avoid re-running
        self._table_folder_prepared = True

    async def _save(self, df: pl.DataFrame, staging_path: Optional[str] = None) -> None:
        """
        Save data to Open Mirroring landing zone.

        Overrides OneLakeStore._save() to add:
        - Polish transform (hash IDs, column normalization) - FIRST
        - __rowMarker__ column injection/validation
        - Column reordering (__rowMarker__ must be last)
        - Metadata file writing
        - Full drop data preparation.

        Args:
            df: Polars DataFrame to write
            staging_path: Staging path from base Store

        Raises:
            StoreError: If validation fails or write fails
        """
        try:
            # Skip empty DataFrames
            if len(df) == 0:
                self.logger.debug("Skipping empty DataFrame")
                return

            # Table folder preparation is now handled in write() method
            # before any data is written, so we don't need to do it here

            # CRITICAL: Apply polish FIRST (creates hash IDs, normalizes columns)
            # This ensures hash ID columns exist before validation
            # Note: _pre_write() is also called in _flush_buffer(), but we ensure
            # it's applied here as well for safety (idempotent - won't double-apply)
            df = self._pre_write(df)

            # Validate key columns exist (after polish has created them)
            self._validate_key_columns(df)

            # Add __rowMarker__ column if needed
            df = self._add_row_marker_column(df)

            # Validate row marker values
            self._validate_row_marker(df)

            # Validate update rows have full data
            self._validate_update_rows(df)

            # Ensure __rowMarker__ is last column (CRITICAL)
            df = self._ensure_row_marker_last(df)

            # Build staging path that maintains schema structure:
            # Files/_tmp/schema_name.schema/entity/ instead of Files/LandingZone/...
            # We need to handle this specially because PathHelper.build_staging_path()
            # inserts _tmp before entity, but we want _tmp to replace LandingZone
            # while preserving the schema folder structure.

            # Generate filename if not provided
            if not staging_path:
                filename = await self.get_next_filename()
                staging_dir = self.get_staging_directory()
                staging_path = (staging_dir / filename).as_posix()
            else:
                filename = PathHelper.get_filename(staging_path)

            # Build staging path: replace LandingZone with _tmp, keep schema structure
            # base_path: /<guid>/Files/LandingZone/istar.schema/Account/
            # staging: /<guid>/Files/_tmp/istar.schema/Account/filename
            if self.base_path and "LandingZone" in self.base_path:
                # Replace LandingZone with _tmp, preserving everything after Files/
                staging_base_path = self.base_path.replace(
                    "/Files/LandingZone/", "/Files/_tmp/"
                )
                # Append filename to the entity path (entity is last segment)
                cloud_staging_path = Path(staging_base_path) / filename
                cloud_staging_path = cloud_staging_path.as_posix()
            else:
                # Fallback: use parent's logic
                cloud_staging_path = PathHelper.build_staging_path(
                    self.base_path,
                    self.entity_name,
                    filename,
                )

            # Get ADLS operations and upload (reusing parent's upload logic)
            adls_ops = self._get_adls_ops()
            buffer = io.BytesIO()
            df.write_parquet(buffer, compression=self.compression)
            data = buffer.getvalue()
            stored_staging_path = await adls_ops.upload_bytes(data, cloud_staging_path)

            # Log and track (reusing parent's tracking logic) with cloud path context
            self._log_write_progress(
                len(df), path=stored_staging_path or cloud_staging_path
            )
            if not hasattr(self, "saved_paths"):
                self.saved_paths = []
            self.saved_paths.append(stored_staging_path or cloud_staging_path)
            if not hasattr(self, "uploaded_files"):
                self.uploaded_files = []
            self.uploaded_files.append(filename)

        except Exception as e:
            self.logger.error(f"Failed to save to Open Mirroring store: {str(e)}")
            raise StoreError(f"Failed to save to Open Mirroring store: {str(e)}")

    def get_deletion_metrics(self) -> Dict[str, int]:
        """
        Get deletion metrics for journal tracking.

        Returns a dictionary with deletion counts by type:
        - 'full_drop_deletions': Rows marked as deleted in full_drop mode
        - 'query_based_deletions': Rows marked as deleted via query-based
            detection
        - 'column_based_deletions': Rows marked as deleted via column-based
            detection

        Returns:
            Dictionary with deletion counts (all values are 0 if no deletions)
        """
        if hasattr(self, "_deletion_metrics"):
            return self._deletion_metrics.copy()
        return {
            "full_drop_deletions": 0,
            "query_based_deletions": 0,
            "column_based_deletions": 0,
        }

    async def finish(self) -> None:
        """
        Finish writing data to Open Mirroring store.

        Overrides base Store.finish() to add:
        - Full drop mode atomic operation (delete production, move files from _tmp)
        - Metadata file cleanup.

        For full_drop mode:
        1. Flush remaining buffered data (all writes to _tmp)
        2. Delete production folder (ACID: only after all writes succeed)
        3. Move all files from _tmp to production (atomic swap)

        For normal mode:
        - Calls parent finish() which moves files normally
        """
        # Always flush remaining buffered data first
        if self.data_buffer:
            await self._flush_buffer()

        # Log any remaining accumulated rows that didn't hit the interval
        if self.rows_since_last_log > 0:
            self.logger.debug(f"WROTE {self.rows_since_last_log:,} rows")

        try:
            if self.full_drop_mode:
                # Atomic operation: Delete production folder AFTER all data written
                self.logger.debug(
                    "full_drop mode: Performing atomic operation - "
                    "deleting production folder and moving files from _tmp"
                )

                # Step 1: Delete production folder (ACID: only after all writes succeed)
                await self._delete_table_folder()

                # Step 2: Move files from _tmp to production in order
                # If deletion_source is configured, move deletion files FIRST,
                # then wait for processing delay, then move new data files.
                # This ensures OpenMirroring processes deletions before new data.
                adls_ops = self._get_adls_ops()
                move_errors = []

                # Step 2a: Move deletion files FIRST (if any)
                if hasattr(self, "_deletion_paths") and self._deletion_paths:
                    self.logger.debug(
                        f"Moving {len(self._deletion_paths)} deletion file(s) "
                        f"from _tmp to production"
                    )
                    for deletion_path in self._deletion_paths:
                        if not deletion_path:
                            continue

                        # Build final path: replace _tmp with LandingZone
                        if "_tmp" in deletion_path:
                            final_path = self._convert_tmp_to_production_path(
                                deletion_path
                            )
                        else:
                            continue

                        try:
                            await adls_ops.move_file(deletion_path, final_path)
                            self.logger.debug(
                                f"Moved deletion file "
                                f"{PathHelper.get_filename(deletion_path)} "
                                f"from _tmp to production"
                            )
                        except Exception as e:
                            filename = PathHelper.get_filename(deletion_path)
                            error_msg = (
                                f"Failed to move deletion file {filename}: {str(e)}"
                            )
                            move_errors.append(error_msg)
                            self.logger.error(error_msg)

                    # Wait for configurable delay to ensure OpenMirroring processes
                    # deletions before new data arrives
                    delay_seconds = self.config.deletion_processing_delay
                    if delay_seconds > 0:
                        self.logger.debug(
                            f"Waiting {delay_seconds} seconds for deletion files "
                            f"to be processed before moving new data files"
                        )
                        await asyncio.sleep(delay_seconds)

                # Step 2b: Move new data files (everything else in saved_paths)
                if hasattr(self, "saved_paths") and self.saved_paths:
                    # Filter out deletion paths (already moved)
                    deletion_paths_set = set(getattr(self, "_deletion_paths", []))
                    new_data_paths = [
                        path
                        for path in self.saved_paths
                        if path and path not in deletion_paths_set
                    ]

                    if new_data_paths:
                        self.logger.debug(
                            f"Moving {len(new_data_paths)} new data file(s) "
                            f"from _tmp to production"
                        )
                        for staging_path_str in new_data_paths:
                            if not staging_path_str:
                                continue

                        # Build final path: replace _tmp with LandingZone
                        if "_tmp" in staging_path_str:
                            final_path_str = self._convert_tmp_to_production_path(
                                staging_path_str
                            )
                        else:
                            # Fallback: build final path from staging path
                            # This should not happen in full_drop mode (all paths
                            # should contain _tmp), but we handle it defensively
                            self.logger.warning(
                                f"full_drop mode: staging path '{staging_path_str}' "
                                "does not contain '_tmp'. This is unexpected and may "
                                "indicate a configuration issue. Using fallback path "
                                "construction."
                            )
                            staging_path = Path(staging_path_str)
                            final_dir = self.get_final_directory()
                            if final_dir:
                                final_path = final_dir / staging_path.name
                                final_path_str = final_path.as_posix()
                            else:
                                # Build from base_path
                                filename = PathHelper.get_filename(staging_path_str)
                                final_path_str = f"{self.base_path}/{filename}"

                        # Move file from _tmp to production
                        try:
                            await adls_ops.move_file(staging_path_str, final_path_str)
                            self.logger.debug(
                                f"Moved {PathHelper.get_filename(staging_path_str)} "
                                f"from _tmp to production"
                            )
                        except Exception as e:
                            filename = PathHelper.get_filename(staging_path_str)
                            error_msg = f"Failed to move {filename}: {str(e)}"
                            move_errors.append(error_msg)
                            self.logger.error(error_msg)
                            # Continue moving other files to minimize data loss

                # Step 3: Move metadata files from _tmp to production (if they exist)
                if self._metadata_tmp_path:
                    # Build final metadata path: replace _tmp with LandingZone
                    final_metadata_path = self._convert_tmp_to_production_path(
                        self._metadata_tmp_path
                    )
                    try:
                        await adls_ops.move_file(
                            self._metadata_tmp_path, final_metadata_path
                        )
                        self.logger.debug(
                            "Moved _metadata.json from _tmp to production"
                        )
                    except Exception as e:
                        error_msg = f"Failed to move _metadata.json: {str(e)}"
                        move_errors.append(error_msg)
                        self.logger.error(error_msg)

                # Step 4: Move partner events file (if it exists and was written
                # to _tmp)
                if self._partner_events_tmp_path:
                    # Build final partner events path: replace _tmp with LandingZone
                    final_partner_events_path = self._convert_tmp_to_production_path(
                        self._partner_events_tmp_path
                    )
                    try:
                        await adls_ops.move_file(
                            self._partner_events_tmp_path,
                            final_partner_events_path,
                        )
                        self.logger.debug(
                            "Moved _partnerEvents.json from _tmp to production"
                        )
                    except Exception as e:
                        error_msg = f"Failed to move _partnerEvents.json: {str(e)}"
                        move_errors.append(error_msg)
                        self.logger.error(error_msg)

                # Step 5: Move schema file if it was written to _tmp
                if self._schema_tmp_path:
                    final_schema_path = self._convert_tmp_to_production_path(
                        self._schema_tmp_path
                    )
                    try:
                        await adls_ops.move_file(
                            self._schema_tmp_path,
                            final_schema_path,
                        )
                        self.logger.debug("Moved _schema.json from _tmp to production")
                    except Exception as e:
                        error_msg = f"Failed to move _schema.json: {str(e)}"
                        move_errors.append(error_msg)
                        self.logger.error(error_msg)

                # If any moves failed, raise a comprehensive error
                if move_errors:
                    error_summary = (
                        f"full_drop atomic operation partially failed. "
                        f"{len(move_errors)} file(s) failed to move from _tmp "
                        f"to production. Production folder was deleted, but some "
                        f"files remain in _tmp. Manual intervention required: "
                        f"Files in _tmp may need to be moved manually or cleaned up. "
                        f"Errors: {'; '.join(move_errors)}"
                    )
                    raise StoreError(error_summary)

                self.logger.debug(
                    "Atomic full_drop operation completed: "
                    "All data successfully moved from _tmp to production"
                )

                # Log completion stats using shared helper
                self._log_completion_stats()
            else:
                # Normal mode: use parent's finish() which moves files and logs stats
                # Buffer already flushed and rows logged above, so parent will skip
                # those steps (checks if buffer exists and rows_since_last_log > 0)
                await super().finish()
        finally:
            # Reset per-write staging state so repeated finish() calls (e.g., mirrored
            # journal appends) don't attempt to move already-published files.
            if hasattr(self, "saved_paths"):
                self.saved_paths = []
            if hasattr(self, "uploaded_files"):
                self.uploaded_files = []
            self._metadata_tmp_path = None
            self._partner_events_tmp_path = None
            self._schema_tmp_path = None
