"""
Journal implementation for tracking flow execution metadata.

Single-file parquet-based journal that tracks entity runs with denormalized
hierarchy information for efficient watermark queries.
"""
import asyncio
import io
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Optional
from uuid import uuid4

import polars as pl
from pydantic import BaseModel, Field, field_validator

from hygge.utility.azure_onelake import ADLSOperations
from hygge.utility.exceptions import ConfigError
from hygge.utility.logger import get_logger
from hygge.utility.path_helper import PathHelper
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
        store: Optional[Any] = None,
        store_config: Optional[Any] = None,
        home_config: Optional[Any] = None,
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

        self.journal_path: Optional[Path] = None
        self.storage_backend: str = "local"
        self.adls_ops: Optional[ADLSOperations] = None
        self.remote_journal_path: Optional[str] = None
        self.remote_dir: Optional[str] = None
        self._mirror_sink = None

        self._configure_storage(
            store=store,
            store_config=store_config or getattr(store, "config", None),
            store_path=store_path,
            home_path=home_path,
            home_config=home_config,
        )
        self._write_lock = asyncio.Lock()

    def _configure_storage(
        self,
        store: Optional[Any],
        store_config: Optional[Any],
        store_path: Optional[str],
        home_path: Optional[str],
        home_config: Optional[Any],
    ) -> None:
        """Configure where the journal will be stored (local vs remote)."""
        # Explicit path always forces local storage
        if self.config.path:
            self._setup_local_storage(Path(self.config.path))
            return

        location = self.config.location
        if location is None:
            raise ConfigError("Journal config must specify either 'path' or 'location'")

        if location == "default":
            location = "store"

        if location == "store":
            if self._store_supports_remote_journal(store_config):
                self._setup_remote_storage(store, store_config)
            else:
                if not store_path:
                    raise ConfigError(
                        "Journal location='store' requires store_path, "
                        "but none provided"
                    )
                self._setup_local_storage(Path(store_path) / ".hygge_journal")
        elif location == "home":
            if not home_path:
                raise ConfigError(
                    "Journal location='home' requires home_path, but none provided"
                )
            self._setup_local_storage(Path(home_path) / ".hygge_journal")
        else:
            raise ConfigError(
                "Journal config must specify either 'path' or 'location' (store/home)"
            )

        # Configure mirrored journal sink if requested
        if (
            self.storage_backend == "adls"
            and store_config
            and getattr(store_config, "type", None) == "open_mirroring"
            and getattr(store_config, "mirror_journal", False)
        ):
            self._mirror_sink = self._create_mirror_sink(store_config)

    def _setup_local_storage(self, journal_dir: Path) -> None:
        """Configure local filesystem storage for the journal."""
        journal_dir.mkdir(parents=True, exist_ok=True)
        self.journal_path = journal_dir / "journal.parquet"
        self.storage_backend = "local"
        self.logger.debug(f"Journal will be stored locally at {self.journal_path}")

    def _store_supports_remote_journal(self, store_config: Optional[Any]) -> bool:
        """Determine whether the store should persist the journal in ADLS/OneLake."""
        store_type = getattr(store_config, "type", None)
        return store_type in {"open_mirroring", "onelake", "adls"}

    def _setup_remote_storage(
        self, store: Optional[Any], store_config: Optional[Any]
    ) -> None:
        """Configure remote ADLS/OneLake storage for the journal."""
        if store is None and store_config is None:
            raise ConfigError(
                "Remote journal storage requires access to the store configuration."
            )

        # Prefer the live store instance for ADLS operations so we don't duplicate auth.
        adls_ops = None
        if store is not None and hasattr(store, "_get_adls_ops"):
            try:
                adls_ops = store._get_adls_ops()  # pylint: disable=protected-access
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.warning(
                    "Failed to reuse store ADLS operations; falling back to config. "
                    f"Error: {exc}"
                )

        effective_config = store_config or getattr(store, "config", None)
        if adls_ops is None:
            adls_ops = self._build_adls_ops_from_store_config(effective_config)

        journal_path = self._build_remote_journal_path(effective_config)
        if not journal_path:
            raise ConfigError(
                "Unable to infer remote journal path from store configuration."
            )

        self.storage_backend = "adls"
        self.adls_ops = adls_ops
        self.remote_journal_path = journal_path
        self.remote_dir = str(PurePosixPath(journal_path).parent)
        self.logger.debug(
            f"Journal will be stored remotely at {self.remote_journal_path}"
        )

    def _build_remote_journal_path(self, store_config: Any) -> Optional[str]:
        """Construct the remote journal path based on the store configuration."""
        if store_config is None:
            return None

        store_type = getattr(store_config, "type", None)
        if store_type == "open_mirroring":
            mirror_name = getattr(store_config, "mirror_name", None)
            if not mirror_name:
                return None
            return f"{mirror_name}/Files/.hygge_journal/journal.parquet"

        path_template = getattr(store_config, "path", None)
        if not path_template:
            return None

        # Remove entity placeholders and trailing slashes.
        substituted = PathHelper.substitute_entity(path_template, entity_name=None)
        base_dir = PurePosixPath(substituted.strip("/"))
        # Drop final segment (entity/table) if there is one.
        if base_dir.parts:
            base_dir = base_dir.parent

        final_dir = base_dir.joinpath(".hygge_journal")
        return final_dir.joinpath("journal.parquet").as_posix()

    def _create_mirror_sink(self, store_config: Any):
        """Create an Open Mirroring sink that mirrors the journal into Fabric."""
        try:
            from hygge.stores.openmirroring import (
                OpenMirroringStore,
                OpenMirroringStoreConfig,
            )
        except ImportError as exc:  # pragma: no cover - environment guard
            self.logger.warning(
                "Unable to import OpenMirroringStore for mirrored journal: %s", exc
            )
            return None

        journal_table = getattr(store_config, "journal_table_name", "__hygge_journal")
        config_kwargs = {
            "account_url": getattr(store_config, "account_url", None),
            "filesystem": getattr(store_config, "filesystem", None),
            "mirror_name": getattr(store_config, "mirror_name", None),
            "key_columns": ["entity_run_id"],
            "row_marker": 4,
            "credential": getattr(store_config, "credential", "managed_identity"),
            "tenant_id": getattr(store_config, "tenant_id", None),
            "client_id": getattr(store_config, "client_id", None),
            "client_secret": getattr(store_config, "client_secret", None),
            "storage_account_key": getattr(store_config, "storage_account_key", None),
            "schema_name": getattr(store_config, "schema_name", None),
            "partner_name": getattr(store_config, "partner_name", None),
            "source_type": getattr(store_config, "source_type", None),
            "source_version": getattr(store_config, "source_version", None),
        }

        # Remove None values to avoid Pydantic validation errors
        config_kwargs = {k: v for k, v in config_kwargs.items() if v is not None}

        try:
            journal_store_config = OpenMirroringStoreConfig(**config_kwargs)
        except Exception as exc:  # pragma: no cover - validation guard
            self.logger.warning(
                "Failed to build OpenMirroringStoreConfig for journal mirroring: %s",
                exc,
            )
            return None

        journal_store = OpenMirroringStore(
            name=f"{self.name}_journal_mirror_store",
            config=journal_store_config,
            flow_name=self.coordinator_name,
            entity_name=journal_table,
        )
        return MirroredJournalWriter(journal_store)

    def _build_adls_ops_from_store_config(
        self, store_config: Optional[Any]
    ) -> ADLSOperations:
        """Construct ADLSOperations from a store configuration."""
        if store_config is None:
            raise ConfigError(
                "Remote journal storage requires a valid store configuration."
            )

        account_url = getattr(store_config, "account_url", None)
        filesystem = getattr(store_config, "filesystem", None)
        if not account_url or not filesystem:
            raise ConfigError(
                "Remote journal storage requires account_url and filesystem."
            )

        credential_type = getattr(store_config, "credential", "managed_identity")

        service_client = self._create_adls_service_client(
            account_url=account_url,
            credential_type=credential_type,
            tenant_id=getattr(store_config, "tenant_id", None),
            client_id=getattr(store_config, "client_id", None),
            client_secret=getattr(store_config, "client_secret", None),
            storage_account_key=getattr(store_config, "storage_account_key", None),
        )

        file_system_client = service_client.get_file_system_client(filesystem)
        is_onelake = getattr(store_config, "type", None) in {
            "onelake",
            "open_mirroring",
        }

        return ADLSOperations(
            file_system_client=file_system_client,
            file_system_name=filesystem,
            service_client=service_client,
            timeout=300,
            is_onelake=is_onelake,
        )

    @staticmethod
    def _create_adls_service_client(
        account_url: str,
        credential_type: str,
        tenant_id: Optional[str],
        client_id: Optional[str],
        client_secret: Optional[str],
        storage_account_key: Optional[str],
    ):
        """Create a DataLakeServiceClient based on credential configuration."""
        try:
            if credential_type == "managed_identity":
                from azure.identity import ManagedIdentityCredential

                credential = ManagedIdentityCredential()
            elif credential_type == "service_principal":
                from azure.identity import ClientSecretCredential

                if not all([tenant_id, client_id, client_secret]):
                    raise ConfigError(
                        "Service principal authentication requires tenant_id, "
                        "client_id, and client_secret."
                    )
                credential = ClientSecretCredential(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret,
                )
            elif credential_type == "storage_key":
                from azure.core.credentials import AzureNamedKeyCredential

                if not storage_account_key:
                    raise ConfigError(
                        "Storage key authentication requires storage_account_key."
                    )
                account_name = (
                    account_url.replace("https://", "")
                    .replace(".dfs.core.windows.net", "")
                    .replace(".dfs.fabric.microsoft.com", "")
                )
                credential = AzureNamedKeyCredential(
                    name=account_name, key=storage_account_key
                )
            else:
                from azure.identity import DefaultAzureCredential

                credential = DefaultAzureCredential()

            from azure.storage.filedatalake import DataLakeServiceClient

            if credential_type == "storage_key":
                return DataLakeServiceClient(
                    account_url=account_url, credential=credential
                )

            return DataLakeServiceClient(account_url=account_url, credential=credential)
        except ImportError as exc:  # pragma: no cover - environment guard
            raise ConfigError(
                "azure-identity and azure-storage-filedatalake packages are required "
                "for remote journal storage."
            ) from exc

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
            if self.storage_backend == "local":
                await asyncio.to_thread(self._append_local_journal, new_row_df)
            else:
                await self._append_remote_journal(new_row_df)

            if self._mirror_sink:
                await self._mirror_sink.append(new_row_df)

        self.logger.debug(
            f"Recorded entity run: {coordinator}/{flow}/{entity} "
            f"(status={status}, rows={row_count})"
        )  # noqa: E501

        return entity_run_id

    def _append_local_journal(self, new_row_df: pl.DataFrame) -> None:
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

    async def _append_remote_journal(self, new_row_df: pl.DataFrame) -> None:
        """Append a new row to the remote journal hosted in ADLS/OneLake."""
        if not self.adls_ops or not self.remote_journal_path:
            raise ConfigError("Remote journal storage is not configured properly.")

        dir_to_create = (self.remote_dir or "").lstrip("/")
        if dir_to_create:
            await self.adls_ops.create_directory_recursive(dir_to_create)

        combined_df = new_row_df
        if await self.adls_ops.file_exists(self.remote_journal_path):
            existing_bytes = await self.adls_ops.read_file_bytes(
                self.remote_journal_path
            )
            if existing_bytes:
                existing_df = pl.read_parquet(
                    io.BytesIO(existing_bytes), schema=self.JOURNAL_SCHEMA
                )
                combined_df = pl.concat([existing_df, new_row_df])

        buffer = io.BytesIO()
        combined_df.write_parquet(buffer)
        data = buffer.getvalue()

        temp_path = f"{self.remote_journal_path}.tmp_{uuid4().hex}"
        await self.adls_ops.upload_bytes(data, temp_path)
        await self.adls_ops.move_file(temp_path, self.remote_journal_path)

    async def _read_journal_df(self) -> Optional[pl.DataFrame]:
        """Read the journal into a Polars DataFrame."""
        if self.storage_backend == "local":
            if not self.journal_path or not self.journal_path.exists():
                return None
            return await asyncio.to_thread(
                pl.read_parquet, self.journal_path, schema=self.JOURNAL_SCHEMA
            )

        if not self.adls_ops or not self.remote_journal_path:
            raise ConfigError("Remote journal storage is not configured properly.")

        if not await self.adls_ops.file_exists(self.remote_journal_path):
            return None

        data = await self.adls_ops.read_file_bytes(self.remote_journal_path)
        if not data:
            return None
        return pl.read_parquet(io.BytesIO(data), schema=self.JOURNAL_SCHEMA)

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
        journal_df = await self._read_journal_df()
        if journal_df is None:
            return None

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
        journal_df = await self._read_journal_df()
        if journal_df is None:
            return {
                "n_entities": 0,
                "n_success": 0,
                "n_fail": 0,
                "n_skip": 0,
                "start_time": None,
                "end_time": None,
            }

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
        journal_df = await self._read_journal_df()
        if journal_df is None:
            return {
                "n_flows": 0,
                "n_entities": 0,
                "start_time": None,
                "end_time": None,
            }

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


class MirroredJournalWriter:
    """Append-only writer that mirrors journal entries into an Open Mirroring table."""

    def __init__(self, store) -> None:
        self.store = store
        self._lock = asyncio.Lock()

    async def append(self, df: pl.DataFrame) -> None:
        async with self._lock:
            await self.store.write(df)
            await self.store.finish()
