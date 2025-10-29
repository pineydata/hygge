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

from hygge.utility.exceptions import StoreError
from hygge.utility.logger import get_logger
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
    ):
        self.file_system_client = file_system_client
        self.file_system_name = file_system_name
        self.service_client = service_client
        self.timeout = timeout
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

            # Use copy-then-delete approach since rename_file has path issues
            dest_client = self.file_system_client.get_file_client(dest_path)

            # Copy file data to destination
            source_data = source_client.download_file().readall()

            # Create destination file
            dest_client.create_file()

            # Upload data in append mode
            data_length = len(source_data)
            dest_client.append_data(source_data, 0, data_length)
            dest_client.flush_data(data_length)

            # Verify destination file exists before deleting source
            try:
                dest_client.get_file_properties()
                self.logger.debug(
                    f"Successfully verified destination file: {dest_path}"
                )

                # Now delete the source file
                source_client.delete_file(timeout=self.timeout)
            except Exception as e:
                self.logger.error(
                    f"Failed to verify destination file {dest_path}: {str(e)}"
                )
                raise

            self.logger.debug(f"Successfully moved file to: {dest_path}")

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
            dest_dir = str(Path(dest_path).parent)
            await self.create_directory_recursive(dest_dir)

            # Add a small delay to allow for directory propagation
            import time

            time.sleep(0.5)  # 500ms delay

            # Verify directory exists
            if not await self.directory_exists(dest_dir):
                raise StoreError(
                    "Directory creation failed - "
                    f"{dest_dir} does not exist after creation"
                )

            # Create file first
            file_client = self.file_system_client.get_file_client(dest_path)
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
            return dest_path

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
            path_parts = path.split("/")
            current_path = ""

            for part in path_parts:
                if current_path:
                    current_path += f"/{part}"
                else:
                    current_path = part

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
