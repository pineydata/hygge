"""
Deletions module for OpenMirroringStore.

Handles detection of deleted rows to keep the mirror in sync.
Simple, focused module that keeps store.py comfortable and maintainable.
"""
import io
from typing import TYPE_CHECKING, List, Optional

import polars as pl

from hygge.core.home import Home
from hygge.homes.mssql import MssqlHome, MssqlHomeConfig
from hygge.utility.exceptions import StoreError

if TYPE_CHECKING:
    from hygge.stores.openmirroring.store import OpenMirroringStore


def _get_staging_keys_path(store: "OpenMirroringStore") -> str:
    """
    Get path for staging deletion check keys file.

    Args:
        store: OpenMirroringStore instance

    Returns:
        Path to staging keys file in _tmp folder
    """
    staging_dir = store.get_staging_directory()
    keys_filename = "_deletion_check_keys.parquet"
    return (staging_dir / keys_filename).as_posix()


async def find_deletions(
    store: "OpenMirroringStore",  # Forward reference - avoids circular import
    home: Optional[Home],
) -> None:
    """
    Find deletions by comparing source vs target.

    Main orchestration method for query-based deletions.
    Called by store.before_flow_start() with retry logic wrapper.

    Args:
        store: OpenMirroringStore instance
        home: Home instance for finding source keys (optional, may be None)
    """
    # Validate key_columns is set (required for query-based deletions)
    if not store.key_columns or len(store.key_columns) == 0:
        raise StoreError(
            "key_columns is required for query-based deletions. "
            "Please specify key_columns in the store config."
        )

    target_keys_path = None
    try:
        # 1. Query target and stage in _tmp
        target_keys_path = await stage_target_keys_in_tmp(store)

        # 2. Find source for all current keys
        source_keys_df = await find_source_keys(store, home, store.key_columns)

        if source_keys_df is None or len(source_keys_df) == 0:
            store.logger.debug("No source keys found - skipping")
            return

        # 3. Read target keys from staging
        # Read from ADLS using adls_ops
        adls_ops = store._get_adls_ops()
        data = await adls_ops.read_file_bytes(target_keys_path)
        if not data:
            store.logger.debug("No target keys found in staging file - skipping")
            return
        target_keys_df = pl.read_parquet(io.BytesIO(data))

        if len(target_keys_df) == 0:
            store.logger.debug("No target keys found - skipping")
            return

        # 4. Anti-join: find rows in target but not in source
        deleted_keys = await find_deletions_batched(
            store, target_keys_df, source_keys_df
        )

        if len(deleted_keys) == 0:
            store.logger.debug("No deletions found")
            return

        # 5. Create and write delete markers
        delete_markers = deleted_keys.with_columns(pl.lit(2).alias("__rowMarker__"))

        store.logger.info(
            f"Found {len(delete_markers):,} deleted row(s) via query-based comparison"
        )

        # Track metrics for observability
        # Note: _deletion_metrics is initialized in configure_for_run()
        store._deletion_metrics["query_based_deletions"] += len(delete_markers)

        await store.write(delete_markers)
        if hasattr(store, "data_buffer") and store.data_buffer:
            await store._flush_buffer()

    except (StoreError, ValueError) as e:
        # Configuration errors - fail fast
        store.logger.error(f"Finding deletions failed (configuration error): {str(e)}")
        raise StoreError(
            f"Finding deletions failed: {str(e)}. "
            f"This is a configuration error - please fix and retry."
        ) from e
    finally:
        if target_keys_path:
            await cleanup_temp_keys_file(store, target_keys_path)


async def find_deletions_batched(
    store: "OpenMirroringStore",
    target_keys_df: pl.DataFrame,
    source_keys_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Find deletions using anti-join.

    Args:
        store: OpenMirroringStore instance
        target_keys_df: DataFrame with target keys
        source_keys_df: DataFrame with source keys

    Returns:
        DataFrame with deleted keys
    """
    # Simple anti-join - Polars handles large datasets efficiently
    deleted_keys = target_keys_df.join(source_keys_df, on=store.key_columns, how="anti")

    if len(deleted_keys) == 0:
        # Return empty DataFrame with correct schema
        # (preserve key column types from target)
        return pl.DataFrame(
            schema={col: target_keys_df[col].dtype for col in store.key_columns}
        )

    return deleted_keys


async def stage_target_keys_in_tmp(store: "OpenMirroringStore") -> str:
    """
    Query target database for all current keys and stage in _tmp folder.

    Uses MSSQLHome to connect to the mirrored SQL database endpoint specified
    in deletion_source config. Queries only key columns for efficiency.

    Args:
        store: OpenMirroringStore instance

    Returns:
        Path to staged parquet file with target keys
    """
    if not store.config.deletion_source:
        raise StoreError(
            "deletion_source configuration is required for query-based deletions"
        )

    # Extract connection info from deletion_source
    server = store.config.deletion_source.get("server")
    database = store.config.deletion_source.get("database")
    schema = store.config.deletion_source.get("schema", "dbo")
    table = store.config.deletion_source.get("table", store.entity_name)

    if not server or not database:
        raise StoreError(
            "deletion_source must contain 'server' and 'database' "
            "for query-based deletions"
        )

    # Build table path
    if schema:
        table_path = f"{schema}.{table}"
    else:
        table_path = table

    # Create MSSQLHome config for target database
    target_home_config = MssqlHomeConfig(
        type="mssql",
        server=server,
        database=database,
        table=table_path,
    )

    # Create temporary MSSQLHome instance
    target_home = MssqlHome("_deletion_target", target_home_config)

    try:
        # Query all keys from target database
        store.logger.debug(
            f"Querying target database {database}.{table_path} "
            f"for key columns: {store.key_columns}"
        )

        # Use find_keys() method to get only key columns
        target_keys_df = await target_home.find_keys(store.key_columns)

        if target_keys_df is None:
            raise StoreError(
                "Failed to query target database keys. "
                "find_keys() returned None - this indicates a configuration "
                "error. Please ensure the target database table is accessible "
                "and configured correctly."
            )

        if len(target_keys_df) == 0:
            # Target database is empty - fail fast to avoid schema assumption issues
            # Empty target means we can't determine key column types safely
            raise StoreError(
                "Cannot determine key column schema: target database is empty. "
                "Query-based deletion detection requires at least one row in the "
                "target database to infer key column types. "
                "Please ensure target database has data, or use column-based "
                "deletions instead."
            )

        # Stage keys in _tmp folder
        keys_path = _get_staging_keys_path(store)

        # Write to parquet
        buffer = io.BytesIO()
        target_keys_df.write_parquet(buffer, compression=store.compression)
        data = buffer.getvalue()

        # Upload to ADLS _tmp folder
        adls_ops = store._get_adls_ops()
        stored_path = await adls_ops.upload_bytes(data, keys_path)

        store.logger.debug(
            f"Staged {len(target_keys_df):,} target keys to {stored_path or keys_path}"
        )

        return stored_path or keys_path

    except Exception as e:
        store.logger.error(f"Failed to stage target keys: {str(e)}")
        raise StoreError(f"Failed to query and stage target keys: {str(e)}") from e


async def find_source_keys(
    store: "OpenMirroringStore",
    home: Optional[Home],
    key_columns: List[str],
) -> Optional[pl.DataFrame]:
    """
    Find all current key values in source Home.

    Uses Home.find_keys() method to get all current keys from source.
    FlowFactory validates Home support before this is called, but we keep
    defensive checks to fail fast if something unexpected happens.

    Args:
        store: OpenMirroringStore instance
        home: Home instance (should be set by FlowFactory)
        key_columns: List of key column names to find

    Returns:
        DataFrame with key columns, or None if no keys found

    Raises:
        StoreError: If Home is None or doesn't support key finding (unexpected)
    """
    if home is None:
        raise StoreError(
            "Query-based deletion finding requires Home reference. "
            "Home not set - this should have been validated by FlowFactory. "
            "This is a configuration error."
        )

    # Check if Home supports key finding
    # (defensive check - FlowFactory should have validated)
    if not hasattr(home, "supports_key_finding") or not home.supports_key_finding():
        raise StoreError(
            f"Home type '{home.__class__.__name__}' does not support "
            "find_keys() for deletion finding. "
            "This should have been validated by FlowFactory. "
            "This is a configuration error."
        )

    # Find source keys via Home interface
    source_keys = await home.find_keys(key_columns)
    if source_keys is not None and len(source_keys) > 0:
        store.logger.debug(f"Found {len(source_keys):,} current keys from source")
    return source_keys


async def cleanup_temp_keys_file(store: "OpenMirroringStore", keys_path: str) -> None:
    """
    Clean up temporary keys file from _tmp folder.

    Cleanup failures are logged as warnings but do not fail the flow.
    This ensures deletion detection completes even if cleanup fails.
    """
    try:
        adls_ops = store._get_adls_ops()
        await adls_ops.delete_file(keys_path)
        store.logger.debug(f"Cleaned up temporary keys file: {keys_path}")
    except Exception as e:
        # Don't fail flow on cleanup errors - deletion detection already completed
        # Log as warning so it's visible but doesn't break the flow
        store.logger.warning(
            f"Failed to cleanup temp keys file {keys_path}: {str(e)}. "
            "This is non-blocking - deletion detection completed successfully."
        )
