"""
Azure Data Lake Storage Gen2 operations for hygge.

Provides Azure-specific operations for working with Azure Data Lake Storage Gen2.
Works with both standard ADLS Gen2 accounts and Microsoft Fabric OneLake.

Refactored from utils2/onelake_ops.py for integration into hygge core.
"""

import json
from pathlib import Path
from typing import BinaryIO, Dict, Optional, Union

from azure.core.exceptions import AzureError
from azure.storage.filedatalake import FileSystemClient

from hygge.messages import get_logger
from hygge.utility.exceptions import StoreError
from hygge.utility.retry import with_retry


class ADLSOperations:
    """
    Handles file operations within Azure Data Lake Storage Gen2.

    This class provides utilities for common ADLS Gen2 operations like moving files
    between locations. Works with both standard ADLS Gen2 accounts and Fabric OneLake.
    """

    def __init__(
        self,
        file_system_client: FileSystemClient,
        file_system_name: str,
        service_client,
        timeout: int = 300,
        is_onelake: bool = False,
    ):
        self.file_system_client = file_system_client
        self.file_system_name = file_system_name
        self.service_client = service_client
        self.timeout = timeout
        self.is_onelake = is_onelake
        self.logger = get_logger("hygge.adls_gen2_ops")

    @with_retry(
        retries=3,
        delay=2,
        exceptions=(AzureError, TimeoutError),
        timeout=300,
        logger_name="hygge.adls_gen2_ops",
    )
    async def move_file(self, source_path: str, dest_path: str) -> None:
        """
        Move file between locations in ADLS Gen2 storage.

        Args:
            source_path: Source file path in ADLS
            dest_path: Destination path in ADLS

        Raises:
            StoreError: If file move fails
        """
        try:
            self.logger.debug("Moving file in ADLS")

            # Create destination directory recursively
            dest_dir = str(Path(dest_path).parent)
            self.logger.debug(f"Creating destination directory: {dest_dir}")
            await self.create_directory_recursive(dest_dir)

            # Verify source exists
            source_client = self.file_system_client.get_file_client(source_path)
            try:
                source_client.get_file_properties()
                self.logger.debug("Source file exists")
            except Exception as e:
                self.logger.error(f"Source file does not exist: {str(e)}")
                raise StoreError(f"Source file does not exist: {str(e)}")

            # Determine which move strategy to use
            dest_client = None

            # For OneLake, skip rename attempt and go straight to copy-then-delete
            # because OneLake doesn't support rename_file API
            if self.is_onelake:
                self.logger.debug(
                    "OneLake detected - using copy-then-delete for atomic move"
                )
                dest_client = self.file_system_client.get_file_client(dest_path)
            else:
                # Use server-side rename for atomic move operation (ADLS Gen2 only)
                # This is more efficient than copy-then-delete and avoids downloading
                try:
                    # Rename_file performs a server-side move/rename operation
                    # The destination path should be relative to the file system root
                    source_client.rename_file(dest_path, timeout=self.timeout)
                    self.logger.debug(f"Successfully moved file to: {dest_path}")
                    return  # Successfully renamed, we're done
                except Exception as rename_error:
                    # If rename fails, fall back to copy-then-delete as a backup
                    self.logger.warning(
                        f"Rename failed, falling back to copy-then-delete: "
                        f"{str(rename_error)}"
                    )
                    dest_client = self.file_system_client.get_file_client(dest_path)

            # Copy-then-delete fallback (used for OneLake and when ADLS rename fails)
            # At this point, dest_client is guaranteed to be set
            # Copy file data to destination in chunks
            CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
            download_stream = source_client.download_file()
            dest_client.create_file()
            offset = 0
            while True:
                chunk = download_stream.read(CHUNK_SIZE)
                if not chunk:
                    break
                dest_client.append_data(chunk, offset, len(chunk))
                offset += len(chunk)
            dest_client.flush_data(offset)
            # Verify destination file exists before deleting source
            try:
                dest_client.get_file_properties()
                self.logger.debug(
                    f"Successfully verified destination file: {dest_path}"
                )

                # Now delete the source file
                source_client.delete_file(timeout=self.timeout)
                self.logger.debug(f"Successfully moved file to: {dest_path}")
            except Exception as e:
                self.logger.error(
                    f"Failed to verify destination file {dest_path}: {str(e)}"
                )
                raise

        except Exception as e:
            if "timeout" in str(e).lower():
                raise TimeoutError(
                    f"Move operation timed out for {dest_path}: {str(e)}"
                )
            raise StoreError(
                f"Failed to move file from {source_path} to {dest_path}: {str(e)}"
            )

    @with_retry(
        retries=3,
        delay=2,
        exceptions=(AzureError, TimeoutError),
        timeout=300,
        logger_name="hygge.adls_gen2_ops",
    )
    async def upload_file(
        self,
        source: Union[str, bytes, BinaryIO],
        dest_path: str,
        overwrite: bool = True,
    ) -> str:
        """
        Upload a file to ADLS Gen2.

        Args:
            source: Either a file path, bytes, or file-like object to upload
            dest_path: Destination path in ADLS
            overwrite: Whether to overwrite existing file

        Returns:
            str: Path where file was uploaded in ADLS
        """
        try:
            # Create all parent directories recursively
            raw_dest_dir = str(Path(dest_path).parent)
            dest_dir = raw_dest_dir.lstrip("/")
            dest_path_normalized = dest_path.lstrip("/")

            if dest_dir:
                await self.create_directory_recursive(dest_dir)
            else:
                # When uploading to the root, skip directory creation but ensure the
                # filesystem exists (upload will fail later if it doesn't).
                self.logger.debug(
                    "Destination is filesystem root; skipping directory creation"
                )

            # Add a small delay to allow for directory propagation
            import time

            time.sleep(0.5)  # 500ms delay

            # Verify directory exists (skip for OneLake to avoid transient policy
            # checks)
            if dest_dir and not self.is_onelake:
                resolved_dir = dest_dir
                if not await self.directory_exists(resolved_dir):
                    raise StoreError(
                        "Directory creation failed - "
                        f"{raw_dest_dir} does not exist after creation"
                    )

            # Create file first
            # Use normalized path (without leading slash) if normalization produced
            # a non-empty result. For root paths ("/"), normalized is empty, so use
            # the original path to avoid creating files with empty names.
            effective_path = dest_path_normalized if dest_path_normalized else dest_path
            file_client = self.file_system_client.get_file_client(effective_path)
            file_client.create_file()

            # Get data to upload
            if isinstance(source, str):
                with open(source, "rb") as f:
                    data = f.read()
            elif isinstance(source, bytes):
                data = source
            else:
                data = source.read()

            # Upload data in append mode
            file_client.append_data(data, 0, len(data))
            file_client.flush_data(len(data))

            self.logger.debug(f"Successfully uploaded file to: {dest_path}")
            return effective_path

        except Exception as e:
            if "timeout" in str(e).lower():
                raise TimeoutError(
                    f"Upload operation timed out for {dest_path}: {str(e)}"
                )
            raise StoreError(f"Failed to upload file to {dest_path}: {str(e)}")

    @with_retry(
        retries=3,
        delay=2,
        exceptions=(AzureError, TimeoutError),
        timeout=300,
        logger_name="hygge.adls_gen2_ops",
    )
    async def upload_bytes(self, data: bytes, dest_path: str) -> str:
        """
        Upload bytes to ADLS Gen2.

        Convenience method for uploading bytes directly.

        Args:
            data: Bytes data to upload
            dest_path: Destination path in ADLS

        Returns:
            str: Path where file was uploaded in ADLS
        """
        return await self.upload_file(source=data, dest_path=dest_path)

    @with_retry(
        retries=3,
        delay=2,
        exceptions=(AzureError, TimeoutError),
        timeout=300,
        logger_name="hygge.adls_gen2_ops",
    )
    async def read_file_bytes(self, path: str) -> bytes:
        """Read an entire file into memory."""
        try:
            file_client = self.file_system_client.get_file_client(path)
            download = file_client.download_file(timeout=self.timeout)
            return download.readall()
        except Exception as e:
            if "timeout" in str(e).lower():
                raise TimeoutError(f"Read operation timed out for {path}: {str(e)}")
            raise StoreError(f"Failed to read file {path}: {str(e)}")

    async def directory_exists(self, path: str) -> bool:
        """Check if a directory exists in ADLS Gen2 storage"""
        try:
            self.logger.debug(f"Checking directory exists: {path}")
            directory_client = self.file_system_client.get_directory_client(path)
            exists = directory_client.exists()
            self.logger.debug(
                f"Directory {path} {'exists' if exists else 'does not exist'}"
            )
            return exists
        except Exception as e:
            self.logger.debug(f"Error checking directory {path}: {str(e)}")
            return False

    async def create_directory(self, path: str) -> None:
        """Create a directory in ADLS Gen2 if it doesn't exist"""
        try:
            self.logger.debug(f"Creating directory: {path}")
            directory_client = self.file_system_client.get_directory_client(path)

            # Create if doesn't exist
            if not await self.directory_exists(path):
                directory_client.create_directory(timeout=self.timeout)
                self.logger.debug(f"Created directory: {path}")
            else:
                self.logger.debug(f"Directory already exists: {path}")

        except Exception as e:
            raise StoreError(f"Failed to create directory {path}: {str(e)}")

    async def create_directory_recursive(self, path: str) -> None:
        """
        Create a directory and all parent directories in ADLS Gen2 if they don't exist
        """
        try:
            self.logger.debug(f"Creating directory recursively: {path}")

            # Create parent directories first
            path_parts = [part for part in path.split("/") if part]
            current_path = ""

            for idx, part in enumerate(path_parts):
                if current_path:
                    current_path += f"/{part}"
                else:
                    current_path = part

                if (
                    self.is_onelake
                    and idx == 0
                    and part.lower() not in {"files", "tables"}
                ):
                    # Mounted relational databases expose a top-level GUID folder that
                    # already exists and cannot be re-created. Skip explicit creation
                    # for that segment and continue with child folders where writes
                    # are permitted (Files/…, Tables/…).
                    continue

                if current_path:  # Skip empty parts
                    try:
                        directory_client = self.file_system_client.get_directory_client(
                            current_path
                        )
                        directory_client.create_directory(timeout=self.timeout)
                        # Add a small delay to allow OneLake to propagate the directory
                        import time

                        time.sleep(0.1)
                    except Exception as e:
                        # Directory might already exist, which is fine
                        if (
                            "PathAlreadyExists" not in str(e)
                            and "already exists" not in str(e).lower()
                        ):
                            self.logger.warning(
                                f"Directory creation failed: {current_path} - {str(e)}"
                            )
                            # Re-raise if it's not an "already exists" error
                            raise

        except Exception as e:
            raise StoreError(f"Failed to create directory recursively {path}: {str(e)}")

    async def file_exists(self, path: str) -> bool:
        """Check if file exists in ADLS Gen2"""
        try:
            file_client = self.file_system_client.get_file_client(path)
            file_client.get_file_properties(timeout=self.timeout)
            return True
        except Exception:
            return False

    async def read_json(self, path: str) -> Optional[Dict]:
        """Read JSON from lake storage with retries"""
        from azure.core.exceptions import ResourceNotFoundError

        try:
            file_client = self.file_system_client.get_file_client(str(path))
            try:
                file_client.get_file_properties(timeout=self.timeout)
            except ResourceNotFoundError:
                self.logger.debug(f"No file found at {path}")
                return None

            download = file_client.download_file(timeout=self.timeout)
            data = download.readall()
            return json.loads(data.decode("utf-8"))

        except Exception as e:
            if "timeout" in str(e).lower():
                raise TimeoutError(f"Read operation timed out for {path}: {str(e)}")
            raise StoreError(f"Failed to read JSON from {path}: {str(e)}")

    async def write_json(self, path: str, data: Dict) -> None:
        """Write JSON to lake storage with retries"""
        try:
            directory = str(Path(path).parent)
            try:
                self.file_system_client.create_directory(
                    directory, timeout=self.timeout
                )
                self.logger.debug(f"Created directory: {directory}")
            except Exception:
                self.logger.debug(f"Directory already exists: {directory}")

            json_data = json.dumps(data, indent=2).encode("utf-8")

            file_client = self.file_system_client.get_file_client(path)
            file_client.upload_data(json_data, overwrite=False, timeout=self.timeout)
            self.logger.info(f"Saved JSON to {path}")

        except Exception as e:
            if "timeout" in str(e).lower():
                raise TimeoutError(f"Write operation timed out for {path}: {str(e)}")
            raise StoreError(f"Failed to write JSON file: {str(e)}")

    @with_retry(
        retries=3,
        delay=2,
        exceptions=(AzureError, TimeoutError),
        timeout=300,
        logger_name="hygge.adls_gen2_ops",
    )
    async def delete_directory(self, path: str, recursive: bool = True) -> bool:
        """
        Delete a directory in ADLS Gen2 storage.

        Handles SDK version differences and gracefully handles cases where
        Open Mirroring or other processes might be using the directory.

        Args:
            path: Directory path to delete
            recursive: Whether to delete directory recursively (default: True)

        Returns:
            bool: True if directory was successfully deleted, False if deletion
                  failed but files were deleted (graceful fallback)

        Raises:
            StoreError: If deletion fails unexpectedly
        """
        import os

        try:
            self.logger.debug(f"Deleting directory: {path} (recursive={recursive})")

            # Get directory client
            directory_client = self.file_system_client.get_directory_client(path)

            # Check if directory exists
            if not directory_client.exists():
                self.logger.debug(f"Directory {path} does not exist")
                return True  # Already deleted, consider success

            # Step 1: Delete all files in the directory first
            # (Required before deleting directory in ADLS)
            deleted_files = 0
            try:
                # Use FileSystemClient.get_paths() to list files
                paths = self.file_system_client.get_paths(path=path, recursive=False)
                for path_item in paths:
                    # Delete all files (not just .parquet - also _metadata.json, etc.)
                    file_path = path_item.name
                    file_client = self.file_system_client.get_file_client(file_path)
                    file_client.delete_file()
                    deleted_files += 1
                    filename = os.path.basename(file_path)
                    self.logger.debug(f"Deleted file: {filename}")
            except Exception as e:
                error_str = str(e).lower()
                # If directory is empty or listing fails, that's okay
                # Continue to try directory deletion anyway
                if (
                    "directorynotempty" not in error_str
                    and "recursive" not in error_str
                ):
                    self.logger.debug(
                        f"Error listing/deleting files in {path}: {str(e)}"
                    )

            # Step 2: Delete the directory itself
            try:
                # Handle SDK version differences - some versions have issues
                # with recursive parameter. The error "got multiple values for
                # keyword argument 'recursive'" suggests a signature conflict.
                try:
                    # Try with keyword argument (standard approach)
                    directory_client.delete_directory(recursive=recursive)
                except (TypeError, ValueError) as param_err:
                    error_str = str(param_err).lower()
                    # If we get a "multiple values" error, SDK has parameter conflict
                    if "multiple values" in error_str or (
                        "recursive" in error_str and "keyword" in error_str
                    ):
                        self.logger.debug(
                            f"DirectoryClient.delete_directory() parameter "
                            f"conflict detected, trying **kwargs: {param_err}"
                        )
                        # Use **kwargs to pass recursive - avoids parameter conflicts
                        directory_client.delete_directory(**{"recursive": recursive})
                    else:
                        raise

                self.logger.debug(f"Successfully deleted directory: {path}")
                if deleted_files > 0:
                    self.logger.debug(
                        f"Deleted {deleted_files} file(s) before directory"
                    )
                return True

            except Exception as e:
                # Open Mirroring or other processes might be using the folder
                # This is expected behavior - gracefully handle it
                error_str = str(e).lower()
                if (
                    "notfound" in error_str
                    or "does not exist" in error_str
                    or "being used" in error_str
                    or "in use" in error_str
                    or "conflict" in error_str
                    or "directorynotempty" in error_str
                    or "recursive" in error_str
                ):
                    self.logger.debug(
                        f"Could not delete directory {path}: {str(e)}. "
                        f"Open Mirroring or other process may be using it. "
                        f"Files were deleted successfully."
                    )
                    # Partial success - files deleted, directory might be locked
                    return False
                else:
                    # Unexpected error - re-raise
                    raise StoreError(f"Failed to delete directory {path}: {str(e)}")

        except Exception as e:
            if "timeout" in str(e).lower():
                raise TimeoutError(
                    f"Delete directory operation timed out for {path}: {str(e)}"
                )
            raise StoreError(f"Failed to delete directory {path}: {str(e)}")
