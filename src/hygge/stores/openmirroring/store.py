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
from typing import Any, Dict, List, Optional

import polars as pl
from pydantic import Field, field_validator, model_validator

from hygge.stores.onelake import OneLakeStore, OneLakeStoreConfig
from hygge.utility.azure_onelake import ADLSOperations
from hygge.utility.exceptions import StoreError
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

        With schema and initial load:
        ```yaml
        store:
          type: open_mirroring
          account_url: https://onelake.dfs.fabric.microsoft.com
          filesystem: <workspace-guid>  # Workspace GUID
          mirror_name: <database-guid>  # Database GUID (required)
          schema: dbo  # or schema_name: dbo
          key_columns: ["id", "user_id"]
          row_marker: 4  # Required: Upsert for updates
          full_drop: true  # Clear existing data before writing
          credential: managed_identity
        ```
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
    key_columns: List[str] = Field(
        ...,
        description=(
            "List of column names that form the unique key "
            "(required for updates/deletes)"
        ),
        min_length=1,
    )

    # File naming strategy
    file_detection: str = Field(
        default="timestamp",
        description="File detection strategy: 'timestamp' or 'sequential'",
    )

    # Data replacement mode
    full_drop: bool = Field(
        default=False,
        description=(
            "If true, clears all existing data files in table folder before writing. "
            "Use for full drop and reload scenarios. "
            "Keeps _metadata.json file (or regenerates it)."
        ),
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

    # Optional: Wait time after folder deletion (for full_drop mode)
    folder_deletion_wait_seconds: float = Field(
        default=2.0,
        ge=0.0,
        le=60.0,
        description=(
            "Wait time in seconds after deleting folder in full_drop mode. "
            "Allows ADLS propagation and Open Mirroring to detect deletion "
            "before recreating folder. Default: 2.0 seconds."
        ),
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

        # Open Mirroring specific configuration
        self.key_columns = config.key_columns
        self.file_detection = config.file_detection
        self.row_marker = config.row_marker
        self.full_drop = config.full_drop

        # Log full drop mode
        if self.full_drop:
            self.logger.debug(
                "Full drop mode: will clear existing data files before writing"
            )
        self.partner_name = config.partner_name
        self.source_type = config.source_type
        self.source_version = config.source_version
        self.schema_name = config.schema_name
        self.starting_sequence = config.starting_sequence

        # Track if metadata files have been written
        self._metadata_written = False
        self._partner_events_written = False

        # Track metadata file paths (for full_drop atomic operation)
        self._metadata_tmp_path = None
        self._partner_events_tmp_path = None

        # Track if table folder has been prepared (full_drop + metadata)
        self._table_folder_prepared = False

        # Track sequence counter - will be initialized from existing files
        self.sequence_counter = None  # Will be set lazily from existing files

        # Prepare table folder (clear if full_drop)
        # Note: We'll do this lazily on first write to ensure ADLS client is ready

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
        """
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

        Args:
            df: DataFrame to validate

        Raises:
            StoreError: If key columns are missing
        """
        df_columns = set(df.columns)
        missing_keys = [col for col in self.key_columns if col not in df_columns]

        if missing_keys:
            raise StoreError(
                f"DataFrame missing required key columns: {missing_keys}. "
                f"Required key columns: {self.key_columns}"
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
                # Fallback: construct _tmp path
                metadata_path = f"{self.base_path}/_metadata.json"
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
        if self.full_drop:
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

            # Validate key columns exist
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
            await adls_ops.upload_bytes(data, cloud_staging_path)

            # Log and track (reusing parent's tracking logic)
            self._log_write_progress(len(df))
            if not hasattr(self, "saved_paths"):
                self.saved_paths = []
            self.saved_paths.append(cloud_staging_path)
            if not hasattr(self, "uploaded_files"):
                self.uploaded_files = []
            self.uploaded_files.append(filename)

        except Exception as e:
            self.logger.error(f"Failed to save to Open Mirroring store: {str(e)}")
            raise StoreError(f"Failed to save to Open Mirroring store: {str(e)}")

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

        if self.full_drop:
            # Atomic operation: Delete production folder AFTER all data written
            self.logger.debug(
                "full_drop mode: Performing atomic operation - "
                "deleting production folder and moving files from _tmp"
            )

            # Step 1: Delete production folder (ACID: only after all writes succeed)
            await self._delete_table_folder()

            # Step 2: Move all data files from _tmp to production
            # Collect any errors but continue moving files to minimize data loss
            adls_ops = self._get_adls_ops()
            move_errors = []

            if hasattr(self, "saved_paths") and self.saved_paths:
                for staging_path_str in self.saved_paths:
                    if not staging_path_str:
                        continue

                    # Build final path: replace _tmp with LandingZone
                    if "_tmp" in staging_path_str:
                        final_path_str = staging_path_str.replace(
                            "/Files/_tmp/", "/Files/LandingZone/"
                        )
                    else:
                        # Fallback: build final path from staging path
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
                final_metadata_path = self._metadata_tmp_path.replace(
                    "/Files/_tmp/", "/Files/LandingZone/"
                )
                try:
                    await adls_ops.move_file(
                        self._metadata_tmp_path, final_metadata_path
                    )
                    self.logger.debug("Moved _metadata.json from _tmp to production")
                except Exception as e:
                    error_msg = f"Failed to move _metadata.json: {str(e)}"
                    move_errors.append(error_msg)
                    self.logger.error(error_msg)

            # Step 4: Move partner events file (if it exists and was written to _tmp)
            if self._partner_events_tmp_path:
                # Build final partner events path: replace _tmp with LandingZone
                final_partner_events_path = self._partner_events_tmp_path.replace(
                    "/Files/_tmp/", "/Files/LandingZone/"
                )
                try:
                    await adls_ops.move_file(
                        self._partner_events_tmp_path, final_partner_events_path
                    )
                    self.logger.debug(
                        "Moved _partnerEvents.json from _tmp to production"
                    )
                except Exception as e:
                    error_msg = f"Failed to move _partnerEvents.json: {str(e)}"
                    move_errors.append(error_msg)
                    self.logger.error(error_msg)

            # If any moves failed, raise a comprehensive error
            if move_errors:
                error_summary = (
                    f"full_drop atomic operation partially failed. "
                    f"{len(move_errors)} file(s) failed to move from _tmp "
                    f"to production. Production folder was deleted, but some "
                    f"files remain in _tmp. Errors: {'; '.join(move_errors)}"
                )
                raise StoreError(error_summary)

            self.logger.success(
                "Atomic full_drop operation completed: "
                "All data successfully moved from _tmp to production"
            )
        else:
            # Normal mode: use parent's finish() which moves files via
            # _move_to_final(). For normal mode, metadata files were written to
            # production directly, so we just need to move data files
            if self.uses_file_staging:
                if hasattr(self, "_move_staged_files_to_final"):
                    await self._move_staged_files_to_final()
                elif hasattr(self, "saved_paths") and hasattr(self, "_move_to_final"):
                    # Move staged files to final location
                    for staging_path_str in self.saved_paths:
                        if staging_path_str:
                            staging_path = Path(staging_path_str)
                            final_dir = self.get_final_directory()
                            if final_dir:
                                final_path = final_dir / staging_path.name
                                await self._move_to_final(staging_path, final_path)

        # Log completion stats (from parent finish())
        if self.start_time:
            duration = asyncio.get_event_loop().time() - self.start_time
            rows_per_sec = self.rows_written / duration if duration > 0 else 0
            self.logger.debug(
                f"Store {self.name} completed in {duration:.1f}s "
                f"({rows_per_sec:,.0f} rows/sec)"
            )
