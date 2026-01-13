"""
Azure Data Lake Storage Gen2 store implementation.

Generic ADLS Gen2 store that works with any Azure Data Lake Storage account.
For Fabric OneLake-specific path conventions, see OneLakeStore which extends this.
"""
import io
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl
from azure.core.credentials import AzureNamedKeyCredential
from azure.identity import (
    ClientSecretCredential,
    DefaultAzureCredential,
    ManagedIdentityCredential,
)
from azure.storage.filedatalake import DataLakeServiceClient
from pydantic import BaseModel, Field, field_validator, model_validator

from hygge.core.polish import PolishConfig, Polisher
from hygge.core.store import Store, StoreConfig
from hygge.utility.azure_onelake import ADLSOperations
from hygge.utility.exceptions import StoreError
from hygge.utility.path_helper import PathHelper


class ADLSStoreConfig(BaseModel, StoreConfig, config_type="adls"):
    """
    Configuration for Azure Data Lake Storage Gen2 store.

    Works with both standard ADLS Gen2 accounts and Fabric OneLake.

    Example:
        ```yaml
        store:
          type: adls
          account_url: https://mystorage.dfs.core.windows.net
          filesystem: mycontainer
          path: data/{entity}/
          credential: managed_identity
        ```
    """

    type: str = Field(default="adls", description="Store type")

    # Connection parameters
    account_url: str = Field(..., description="ADLS Gen2 account URL")
    filesystem: str = Field(..., description="Filesystem/container name")
    path: str = Field(..., description="Base path for data files")
    credential: str = Field(
        default="managed_identity",
        description=(
            "Authentication method: managed_identity, "
            "service_principal, or storage_key"
        ),
    )

    # Authentication for service principal
    tenant_id: Optional[str] = Field(
        None, description="Tenant ID for service principal"
    )
    client_id: Optional[str] = Field(
        None, description="Client ID for service principal"
    )
    client_secret: Optional[str] = Field(
        None, description="Client secret for service principal"
    )

    # Authentication for storage key
    storage_account_key: Optional[str] = Field(None, description="Storage account key")

    # File options
    compression: str = Field(default="snappy", description="Parquet compression type")
    file_pattern: str = Field(
        default="{sequence:020d}.parquet",
        description="File naming pattern with {sequence}, {timestamp}, {name}",
    )
    batch_size: int = Field(
        default=100_000,
        ge=1,
        description=(
            "Number of rows to accumulate before writing to parquet. "
            "100,000 creates ~10-20MB files - optimal for parquet compression "
            "and cloud throughput."
        ),
    )

    incremental: Optional[bool] = Field(
        default=None,
        description=(
            "Optional override for incremental behaviour. "
            "None (default) defers to the flow's run_type, "
            "True forces incremental append, False forces full-drop reloads."
        ),
    )

    # Additional options
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional ADLS store options"
    )
    # Optional last-mile polishing configuration
    polish: Optional[PolishConfig] = Field(
        default=None,
        description="Optional Polisher configuration for last-mile transforms.",
    )

    @field_validator("credential")
    @classmethod
    def validate_credential(cls, v):
        """Validate credential type."""
        valid_credentials = [
            "managed_identity",
            "service_principal",
            "storage_key",
            "default",
        ]
        if v not in valid_credentials:
            raise ValueError(
                f"Credential must be one of {valid_credentials}, got '{v}'"
            )
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

    @model_validator(mode="after")
    def validate_service_principal(self):
        """Validate service principal credentials if used."""
        if self.credential == "service_principal":
            if not all([self.tenant_id, self.client_id, self.client_secret]):
                raise ValueError(
                    "Service principal authentication requires "
                    "tenant_id, client_id, and client_secret"
                )
        return self

    @model_validator(mode="after")
    def validate_storage_key(self):
        """Validate storage key if used."""
        if self.credential == "storage_key" and not self.storage_account_key:
            raise ValueError("Storage key authentication requires storage_account_key")
        return self

    def get_merged_options(self, flow_name: str = None) -> Dict[str, Any]:
        """Get all options including defaults."""
        options = {
            "batch_size": self.batch_size,
            "compression": self.compression,
            "file_pattern": self.file_pattern,
        }
        options.update(self.options)

        # Set flow-specific file pattern if flow_name provided
        if flow_name:
            pattern = options["file_pattern"]
            if "{flow_name}" in pattern:
                options["file_pattern"] = pattern.replace("{flow_name}", flow_name)

        return options


class ADLSStore(Store, store_type="adls"):
    """
    Azure Data Lake Storage Gen2 data store with streaming uploads.

    Writes data directly to Azure Data Lake Storage Gen2 without local disk.
    Uses memory → cloud temp → cloud final pattern for atomic writes.

    Works with:
    - Standard Azure Data Lake Storage Gen2 accounts
    - Microsoft Fabric OneLake (same underlying storage)

    Example:
        ```python
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
            credential="managed_identity"
        )
        store = ADLSStore("test_store", config, entity_name="users")
        ```
    """

    def __init__(
        self,
        name: str,
        config: ADLSStoreConfig,
        flow_name: Optional[str] = None,
        entity_name: Optional[str] = None,
    ):
        # Get merged options from config
        merged_options = config.get_merged_options(flow_name or name)

        super().__init__(name, merged_options)
        self.config = config
        # Optional polish configuration
        self._polisher = (
            Polisher(config.polish) if getattr(config, "polish", None) else None
        )
        self.entity_name = entity_name
        self.flow_name = flow_name or name

        # Build base path with entity substitution
        self.base_path = PathHelper.substitute_entity(config.path, entity_name)

        # File configuration
        self.file_pattern = merged_options.get(
            "file_pattern", "{sequence:020d}.parquet"
        )
        self.compression = merged_options.get("compression", "snappy")
        self.sequence_counter = 0
        self.full_drop_mode = False
        self.incremental_override = config.incremental

        # ADLS client setup (lazy initialization)
        self._service_client = None
        self._file_system_client = None
        self._adls_ops = None
        self._credential = None

        # Track uploaded files
        self.uploaded_files = []

        # Only log if we have a final path (not template paths like "test/{entity}/")
        if entity_name and self.base_path and "{entity}" not in self.base_path:
            self.logger.debug(f"Initialized ADLS Gen2 store: {name} → {self.base_path}")

    def configure_for_run(self, run_type: str) -> None:
        """Allow flows to toggle truncate behaviour via run type."""
        super().configure_for_run(run_type)

        is_incremental = run_type != "full_drop"
        if self.incremental_override is not None:
            is_incremental = self.incremental_override

        self.full_drop_mode = not is_incremental

        # Reset per-run tracking so we don't carry state across executions
        self.sequence_counter = 0
        self.saved_paths = []
        self.uploaded_files = []

        if self.full_drop_mode:
            self.logger.debug(
                "Run configured for full_drop: will truncate destination "
                "before publishing new files."
            )

        # Reset per-run tracking so we don't carry state across executions
        self.sequence_counter = 0
        self.saved_paths = []
        self.uploaded_files = []

        if self.full_drop_mode:
            self.logger.debug(
                "Run configured for full_drop: will truncate destination directory "
                "before publishing new files."
            )

    def _get_credential(self):
        """Initialize credential based on configuration."""
        if self._credential is not None:
            return self._credential

        try:
            if self.config.credential == "managed_identity":
                self._credential = ManagedIdentityCredential()
            elif self.config.credential == "service_principal":
                if not all(
                    [
                        self.config.tenant_id,
                        self.config.client_id,
                        self.config.client_secret,
                    ]
                ):
                    raise StoreError(
                        "Service principal requires "
                        "tenant_id, client_id, and client_secret"
                    )
                self._credential = ClientSecretCredential(
                    tenant_id=self.config.tenant_id,
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret,
                )
            elif self.config.credential == "storage_key":
                if not self.config.storage_account_key:
                    raise StoreError(
                        "Storage key authentication requires storage_account_key"
                    )
                self._credential = None
            else:  # default
                self._credential = DefaultAzureCredential()

            return self._credential
        except ImportError:
            raise StoreError(
                "azure-identity package required for ADLS authentication. "
                "Install with: pip install azure-identity"
            )

    def _get_service_client(self):
        """Initialize Azure Data Lake Service Client."""
        if self._service_client is not None:
            return self._service_client

        try:
            credential = self._get_credential()

            # For storage key, we need to use AzureNamedKeyCredential
            if self.config.credential == "storage_key":
                # Extract account name from URL
                # URL format: https://{account}.dfs.core.windows.net
                account_name = (
                    self.config.account_url.replace("https://", "")
                    .replace(".dfs.core.windows.net", "")
                    .replace(
                        ".dfs.fabric.microsoft.com",
                        "",  # Support Fabric URLs too
                    )
                )

                named_key = AzureNamedKeyCredential(
                    name=account_name,
                    key=self.config.storage_account_key,
                )

                self._service_client = DataLakeServiceClient(
                    account_url=self.config.account_url,
                    credential=named_key,
                )
            else:
                self._service_client = DataLakeServiceClient(
                    account_url=self.config.account_url,
                    credential=credential,
                )

            return self._service_client
        except ImportError:
            raise StoreError(
                "azure-storage-filedatalake package required for ADLS operations. "
                "Install with: pip install azure-storage-filedatalake"
            )

    def _get_file_system_client(self):
        """Get or create filesystem client."""
        if self._file_system_client is not None:
            return self._file_system_client

        service_client = self._get_service_client()
        self._file_system_client = service_client.get_file_system_client(
            self.config.filesystem
        )
        return self._file_system_client

    def _get_adls_ops(self) -> ADLSOperations:
        """Initialize ADLS Gen2 operations client."""
        if self._adls_ops is not None:
            return self._adls_ops

        file_system_client = self._get_file_system_client()
        service_client = self._get_service_client()

        # Get filesystem name for logging
        filesystem_name = self.config.filesystem

        # Detect if this is OneLake by checking config type
        is_onelake = getattr(self.config, "type", None) == "onelake"

        self._adls_ops = ADLSOperations(
            file_system_client=file_system_client,
            file_system_name=filesystem_name,
            service_client=service_client,
            timeout=300,
            is_onelake=is_onelake,
        )
        return self._adls_ops

    def _build_adls_path(self, filename: str) -> str:
        """
        Build full ADLS Gen2 cloud path.

        Ensures path always has proper trailing slash after base_path.

        Examples:
        - data/users/00000000000000000001.parquet
        - landing/orders/00000000000000000002.parquet
        - staging/_tmp/00000000000000000003.parquet
        """
        return PathHelper.build_final_path(self.base_path, filename)

    async def get_next_filename(self) -> str:
        """Generate the next filename using pattern."""
        self.sequence_counter += 1

        # Build filename with pattern
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.file_pattern.format(
            sequence=self.sequence_counter,
            timestamp=timestamp,
            name=self.name,
            flow_name=self.flow_name,
        )

        return filename

    def get_staging_directory(self) -> Path:
        """
        Get the cloud staging directory path.

        Returns a Path object that represents the staging location.
        Returns "_tmp" which will be expanded to Files/_tmp/entity/ by _save().
        """
        return Path("_tmp")

    def get_final_directory(self):
        """
        Get the cloud final directory path.

        Returns a Path object that represents the final destination.
        The base Store will append filenames to this directory.
        For cloud stores, we translate this to cloud paths using _build_adls_path().
        """
        # Return final dir: {base_path}/
        return Path(self.base_path)

    async def _save(self, df: pl.DataFrame, staging_path: Optional[str] = None) -> None:
        """
        Save data to ADLS Gen2 staging (cloud temp).

        Uploads data to cloud staging directory. The base Store finish()
        method will call _move_to_final() to move from staging to final.

        Args:
            df: Polars DataFrame to write
            staging_path: Staging path from base Store (e.g., "_temp/{name}/filename")

        Raises:
            StoreError: If upload fails
        """
        try:
            # Skip empty DataFrames
            if len(df) == 0:
                self.logger.debug("Skipping empty DataFrame")
                return

            # Generate filename if not provided in staging_path
            if not staging_path:
                filename = await self.get_next_filename()
                # Build staging path using helper
                staging_dir = self.get_staging_directory()
                staging_path = (staging_dir / filename).as_posix()
            else:
                # Normalize path separators from base Store (which uses str())
                # Convert to Path and back to ensure forward slashes for cloud
                staging_path = Path(staging_path).as_posix()

            # Extract filename from staging_path for logging
            filename = PathHelper.get_filename(staging_path)

            # Convert staging path to cloud path
            # Staging goes at Files/_tmp/entity, not Files/entity/_tmp
            # For base_path like "Files/Account", we want "Files/_tmp/Account/filename"
            # PathHelper handles all path structures robustly
            cloud_staging_path = PathHelper.build_staging_path(
                self.base_path,
                self.entity_name,
                filename,
            )

            # Get ADLS Gen2 operations client
            adls_ops = self._get_adls_ops()

            # Write to memory buffer
            buffer = io.BytesIO()
            df.write_parquet(buffer, compression=self.compression)
            data = buffer.getvalue()

            # Upload to cloud staging location
            await adls_ops.upload_bytes(data, cloud_staging_path)

            # Log write progress using base class method with cloud path context
            self._log_write_progress(len(df), path=cloud_staging_path)

            # Track cloud path for moving to final location
            if not hasattr(self, "saved_paths"):
                self.saved_paths = []
            self.saved_paths.append(cloud_staging_path)

            # Track uploaded files for statistics
            if not hasattr(self, "uploaded_files"):
                self.uploaded_files = []
            self.uploaded_files.append(filename)

        except Exception as e:
            self.logger.error(f"Failed to upload to ADLS Gen2 staging: {str(e)}")
            raise StoreError(f"Failed to upload to ADLS Gen2 staging: {str(e)}")

    async def cleanup_staging(self) -> None:
        """Clean up staging/_tmp directory before retrying a flow."""
        try:
            adls_ops = self._get_adls_ops()
            # Build staging directory path using same logic as _save()
            # For base_path like "Files/Account", we want "Files/_tmp/Account"
            # Use a dummy filename, then get parent to get the entity directory
            cloud_staging_dir = PathHelper.build_staging_path(
                self.base_path,
                self.entity_name,
                "dummy.parquet",  # Dummy filename to build full path
            )
            # Get parent to get the entity directory (Files/_tmp/Account)
            cloud_staging_dir = str(Path(cloud_staging_dir).parent)

            # Delete staging directory if it exists
            try:
                await adls_ops.delete_directory(cloud_staging_dir, recursive=True)
                self.logger.debug(f"Cleaned up staging directory: {cloud_staging_dir}")
            except Exception as e:
                # Directory might not exist or already be cleaned up
                error_str = str(e).lower()
                if "notfound" not in error_str and "does not exist" not in error_str:
                    self.logger.warning(
                        f"Failed to cleanup staging directory: {str(e)}"
                    )
        except Exception as e:
            self.logger.warning(f"Failed to cleanup staging directory: {str(e)}")

    async def reset_retry_sensitive_state(self) -> None:
        """Reset retry-sensitive state before retry."""
        # Call parent to reset general store state
        await super().reset_retry_sensitive_state()
        # Reset store-specific state
        self.sequence_counter = 0
        self.uploaded_files.clear()
        # Reset saved_paths if it exists (may be set lazily)
        if hasattr(self, "saved_paths"):
            self.saved_paths.clear()
        self.logger.debug("Reset sequence counter and uploaded files for retry")

    async def _cleanup_temp(self, staging_path: Optional[str] = None) -> None:
        """
        ADLS Gen2 cleanup is handled automatically.

        Temp files are moved to final location, so no cleanup needed.
        """
        pass

    async def _move_to_final(self, staging_path: str, final_path: str) -> None:
        """
        Move file from cloud staging to cloud final location.

        Args:
            staging_path: Staging path from base Store (local path reference)
            final_path: Final path from base Store (local path reference)
        """
        try:
            # Convert Path to string if needed (base Store passes Path objects)
            if isinstance(staging_path, Path):
                staging_path_str = staging_path.as_posix()
            else:
                # Normalize string paths to ensure forward slashes for cloud
                staging_path_str = Path(staging_path).as_posix()

            # Extract filename from staging_path
            filename = PathHelper.get_filename(staging_path_str)

            # Build cloud paths by converting local paths to cloud paths
            # staging_path points to the cloud staging file we uploaded
            # Find it in saved_paths (which has the actual cloud paths)
            cloud_staging_path = None
            if hasattr(self, "saved_paths"):
                for saved_path in self.saved_paths:
                    if saved_path.endswith(filename):
                        cloud_staging_path = saved_path
                        break

            if not cloud_staging_path:
                raise StoreError(f"Cloud staging path not found for {filename}")

            # Build final cloud path
            cloud_final_path = self._build_adls_path(filename)

            # Get ADLS Gen2 operations client
            adls_ops = self._get_adls_ops()

            # Move within ADLS Gen2 (atomic)
            await adls_ops.move_file(cloud_staging_path, cloud_final_path)

            self.logger.debug(f"Moved {filename} to final location")

        except Exception as e:
            self.logger.error(f"Failed to move file to final location: {str(e)}")
            raise StoreError(f"Failed to finalize transfer {staging_path}: {str(e)}")

    async def _truncate_destination(self, adls_ops: ADLSOperations) -> None:
        """Delete destination directory contents before a full_drop run."""
        final_dir = self.get_final_directory()
        if final_dir is None:
            raise StoreError(
                "Cannot truncate destination: final directory is undefined"
            )

        dest_path = final_dir.as_posix().rstrip("/")
        if not dest_path:
            self.logger.debug(
                "Destination path empty after normalization; skipping truncation"
            )
            return

        try:
            deleted = await adls_ops.delete_directory(dest_path, recursive=True)
            if deleted:
                self.logger.debug(
                    f"Cleared existing data at {dest_path} before full_drop run"
                )
        except Exception as exc:
            raise StoreError(
                f"Failed to truncate destination directory '{dest_path}': {exc}"
            ) from exc

    async def _move_staged_files_to_final(self) -> None:
        """Move staged files to final location, respecting full_drop mode."""
        if not getattr(self, "saved_paths", None):
            return

        final_dir = self.get_final_directory()
        if final_dir is None:
            raise StoreError(
                "Cannot move files to final location: destination directory unknown"
            )

        adls_ops = self._get_adls_ops()

        if self.full_drop_mode:
            await self._truncate_destination(adls_ops)

        for staging_path_str in self.saved_paths:
            if not staging_path_str:
                continue

            staging_path = Path(staging_path_str)
            final_path = final_dir / staging_path.name
            await self._move_to_final(staging_path, final_path)

        if self.full_drop_mode:
            self.logger.success(
                "full_drop run completed: replaced destination directory contents"
            )

    async def close(self) -> None:
        """Finalize any remaining writes and log statistics."""
        await self.finish()

        # Log final statistics
        if self.uploaded_files:
            self.logger.success(
                f"ADLS Gen2 store {self.name} uploaded "
                f"{len(self.uploaded_files)} files "
                f"to {self.config.filesystem}/{self.base_path}"
            )
