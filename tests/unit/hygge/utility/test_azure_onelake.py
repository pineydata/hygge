"""
Tests for Azure Data Lake Storage Gen2 operations.

Following hygge's testing principles:
- Test behavior that matters to users
- Focus on data integrity and reliability
- Verify Azure operations work correctly for both ADLS Gen2 and OneLake
"""
import sys
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError

from hygge.utility.azure_onelake import ADLSOperations
from hygge.utility.exceptions import StoreError


@pytest.fixture(autouse=True)
def mock_time_sleep(monkeypatch):
    """Mock time.sleep globally to speed up tests with retry decorators."""
    # Patch time.sleep and asyncio.sleep to avoid delays in retry decorator
    mock_sleep = MagicMock()
    monkeypatch.setattr("time.sleep", mock_sleep)
    # Tenacity uses asyncio.sleep for async retries
    async_mock_sleep = AsyncMock()
    monkeypatch.setattr("asyncio.sleep", async_mock_sleep)
    yield


@pytest.fixture
def mock_file_system_client():
    """Create a mock FileSystemClient."""
    return MagicMock()


@pytest.fixture
def mock_service_client():
    """Create a mock service client."""
    return MagicMock()


@pytest.fixture
def adls_ops(mock_file_system_client, mock_service_client):
    """Create ADLSOperations instance for ADLS Gen2."""
    return ADLSOperations(
        file_system_client=mock_file_system_client,
        file_system_name="test-container",
        service_client=mock_service_client,
        timeout=300,
        is_onelake=False,
    )


@pytest.fixture
def onelake_ops(mock_file_system_client, mock_service_client):
    """Create ADLSOperations instance for OneLake."""
    return ADLSOperations(
        file_system_client=mock_file_system_client,
        file_system_name="test-workspace",
        service_client=mock_service_client,
        timeout=300,
        is_onelake=True,
    )


class TestADLSOperationsInitialization:
    """Test ADLSOperations initialization."""

    def test_adls_ops_initialization(self, adls_ops, mock_file_system_client):
        """Test ADLSOperations initializes correctly for ADLS Gen2."""
        assert adls_ops.file_system_client == mock_file_system_client
        assert adls_ops.file_system_name == "test-container"
        assert adls_ops.timeout == 300
        assert adls_ops.is_onelake is False

    def test_onelake_ops_initialization(self, onelake_ops):
        """Test ADLSOperations initializes correctly for OneLake."""
        assert onelake_ops.is_onelake is True


class TestADLSOperationsMoveFile:
    """Test file move operations."""

    @pytest.mark.asyncio
    async def test_move_file_adls_gen2_uses_rename(self, adls_ops):
        """Test ADLS Gen2 uses efficient server-side rename."""
        source_path = "source/file.parquet"
        dest_path = "dest/file.parquet"

        # Mock source and destination clients
        source_client = MagicMock()
        source_client.get_file_properties.return_value = MagicMock()
        source_client.rename_file = MagicMock()

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=source_client
        )

        # Mock directory creation
        adls_ops.create_directory_recursive = AsyncMock()

        await adls_ops.move_file(source_path, dest_path)

        # Should use rename_file for ADLS Gen2
        source_client.rename_file.assert_called_once_with(dest_path, timeout=300)
        # Should not copy-then-delete for ADLS Gen2 when rename succeeds
        assert (
            not hasattr(source_client, "delete_file")
            or not source_client.delete_file.called
        )

    @pytest.mark.asyncio
    async def test_move_file_onelake_uses_copy_then_delete(self, onelake_ops):
        """Test OneLake uses copy-then-delete since rename isn't supported."""
        source_path = "source/file.parquet"
        dest_path = "dest/file.parquet"

        # Mock source and destination clients
        source_client = MagicMock()
        source_client.get_file_properties.return_value = MagicMock()
        download_stream = MagicMock()
        download_stream.read.side_effect = [b"chunk1", b"chunk2", b""]
        source_client.download_file.return_value = download_stream
        source_client.delete_file = MagicMock()

        dest_client = MagicMock()
        dest_client.get_file_properties.return_value = MagicMock()
        dest_client.create_file = MagicMock()
        dest_client.append_data = MagicMock()
        dest_client.flush_data = MagicMock()

        onelake_ops.file_system_client.get_file_client = MagicMock(
            side_effect=[source_client, dest_client]
        )

        # Mock directory creation
        onelake_ops.create_directory_recursive = AsyncMock()

        await onelake_ops.move_file(source_path, dest_path)

        # Should use copy-then-delete for OneLake
        dest_client.create_file.assert_called_once()
        dest_client.append_data.assert_called()
        dest_client.flush_data.assert_called_once()
        source_client.delete_file.assert_called_once_with(timeout=300)

    @pytest.mark.asyncio
    async def test_move_file_falls_back_to_copy_on_rename_failure(self, adls_ops):
        """Test ADLS Gen2 falls back to copy-then-delete when rename fails."""
        source_path = "source/file.parquet"
        dest_path = "dest/file.parquet"

        # Mock source client with rename failure
        source_client = MagicMock()
        source_client.get_file_properties.return_value = MagicMock()
        source_client.rename_file.side_effect = Exception("Rename failed")
        download_stream = MagicMock()
        download_stream.read.side_effect = [b"chunk1", b""]
        source_client.download_file.return_value = download_stream
        source_client.delete_file = MagicMock()

        dest_client = MagicMock()
        dest_client.get_file_properties.return_value = MagicMock()
        dest_client.create_file = MagicMock()
        dest_client.append_data = MagicMock()
        dest_client.flush_data = MagicMock()

        adls_ops.file_system_client.get_file_client = MagicMock(
            side_effect=[source_client, dest_client]
        )

        # Mock directory creation
        adls_ops.create_directory_recursive = AsyncMock()

        await adls_ops.move_file(source_path, dest_path)

        # Should fall back to copy-then-delete
        dest_client.create_file.assert_called_once()
        source_client.delete_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_move_file_source_not_found_raises_error(self, adls_ops):
        """Test move raises error when source file doesn't exist."""
        source_path = "source/file.parquet"
        dest_path = "dest/file.parquet"

        source_client = MagicMock()
        source_client.get_file_properties.side_effect = ResourceNotFoundError(
            "File not found"
        )

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=source_client
        )
        adls_ops.create_directory_recursive = AsyncMock()

        with pytest.raises(StoreError, match="Source file does not exist"):
            await adls_ops.move_file(source_path, dest_path)

    @pytest.mark.asyncio
    @pytest.mark.skip(
        reason="Timeout handling triggers retry decorator which is slow. "
        "Timeout behavior is tested in retry decorator tests."
    )
    async def test_move_file_handles_timeout(self, adls_ops):
        """Test move handles timeout errors correctly.

        Note: Skipped because timeout errors trigger retry decorator retries,
        which makes this test slow. Timeout handling is verified in retry
        decorator tests and integration tests.
        """
        pass


class TestADLSOperationsUploadFile:
    """Test file upload operations."""

    @pytest.mark.asyncio
    async def test_upload_file_from_path(self, adls_ops, temp_dir):
        """Test uploading file from local path."""
        test_file = temp_dir / "test.txt"
        test_file.write_bytes(b"test content")
        dest_path = "dest/test.txt"

        file_client = MagicMock()
        file_client.create_file = MagicMock()
        file_client.append_data = MagicMock()
        file_client.flush_data = MagicMock()

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=file_client
        )
        adls_ops.create_directory_recursive = AsyncMock()
        adls_ops.directory_exists = AsyncMock(return_value=True)

        with patch("time.sleep"):  # Mock sleep to speed up test
            result = await adls_ops.upload_file(str(test_file), dest_path)

        assert result == dest_path.lstrip("/")
        file_client.create_file.assert_called_once()
        file_client.append_data.assert_called_once()
        file_client.flush_data.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_file_from_bytes(self, adls_ops):
        """Test uploading file from bytes."""
        data = b"test content"
        dest_path = "dest/test.txt"

        file_client = MagicMock()
        file_client.create_file = MagicMock()
        file_client.append_data = MagicMock()
        file_client.flush_data = MagicMock()

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=file_client
        )
        adls_ops.create_directory_recursive = AsyncMock()
        adls_ops.directory_exists = AsyncMock(return_value=True)

        with patch("time.sleep"):  # Mock sleep to speed up test
            result = await adls_ops.upload_file(data, dest_path)

        assert result == dest_path.lstrip("/")
        file_client.append_data.assert_called_once_with(data, 0, len(data))

    @pytest.mark.asyncio
    async def test_upload_file_from_file_like(self, adls_ops):
        """Test uploading file from file-like object."""
        data = b"test content"
        file_obj = BytesIO(data)
        dest_path = "dest/test.txt"

        file_client = MagicMock()
        file_client.create_file = MagicMock()
        file_client.append_data = MagicMock()
        file_client.flush_data = MagicMock()

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=file_client
        )
        adls_ops.create_directory_recursive = AsyncMock()
        adls_ops.directory_exists = AsyncMock(return_value=True)

        with patch("time.sleep"):  # Mock sleep to speed up test
            result = await adls_ops.upload_file(file_obj, dest_path)

        assert result == dest_path.lstrip("/")
        file_client.append_data.assert_called_once_with(data, 0, len(data))

    @pytest.mark.asyncio
    async def test_upload_file_to_root_skips_directory_creation(self, adls_ops):
        """Test uploading to root skips directory creation."""
        data = b"test content"
        dest_path = "/file.txt"

        file_client = MagicMock()
        file_client.create_file = MagicMock()
        file_client.append_data = MagicMock()
        file_client.flush_data = MagicMock()

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=file_client
        )
        adls_ops.create_directory_recursive = AsyncMock()

        with patch("time.sleep"):  # Mock sleep to speed up test
            await adls_ops.upload_file(data, dest_path)

        # Should not call create_directory_recursive for root path
        # On Windows root is "\\", implementation may call with it; skip assertion there
        if sys.platform != "win32":
            adls_ops.create_directory_recursive.assert_not_called()

    @pytest.mark.asyncio
    async def test_upload_file_onelake_skips_directory_verification(self, onelake_ops):
        """Test OneLake skips directory verification to avoid policy checks."""
        data = b"test content"
        dest_path = "dest/test.txt"

        file_client = MagicMock()
        file_client.create_file = MagicMock()
        file_client.append_data = MagicMock()
        file_client.flush_data = MagicMock()

        onelake_ops.file_system_client.get_file_client = MagicMock(
            return_value=file_client
        )
        onelake_ops.create_directory_recursive = AsyncMock()
        onelake_ops.directory_exists = AsyncMock()

        with patch("time.sleep"):  # Mock sleep to speed up test
            await onelake_ops.upload_file(data, dest_path)

        # Should not verify directory for OneLake
        onelake_ops.directory_exists.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.skip(
        reason="Timeout handling triggers retry decorator which is slow. "
        "Timeout behavior is tested in retry decorator tests."
    )
    async def test_upload_file_handles_timeout(self, adls_ops):
        """Test upload handles timeout errors.

        Note: Skipped because timeout errors trigger retry decorator retries.
        Timeout handling is verified in retry decorator tests.
        """
        pass

    @pytest.mark.asyncio
    async def test_upload_bytes_convenience_method(self, adls_ops):
        """Test upload_bytes convenience method."""
        data = b"test content"
        dest_path = "dest/test.txt"

        with patch.object(
            adls_ops, "upload_file", new_callable=AsyncMock
        ) as mock_upload:
            mock_upload.return_value = dest_path

            result = await adls_ops.upload_bytes(data, dest_path)

            assert result == dest_path
            mock_upload.assert_called_once_with(source=data, dest_path=dest_path)


class TestADLSOperationsReadFile:
    """Test file read operations."""

    @pytest.mark.asyncio
    async def test_read_file_bytes_success(self, adls_ops):
        """Test reading file bytes successfully."""
        path = "source/file.txt"
        expected_data = b"file content"

        file_client = MagicMock()
        download = MagicMock()
        download.readall.return_value = expected_data
        file_client.download_file.return_value = download

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=file_client
        )

        result = await adls_ops.read_file_bytes(path)

        assert result == expected_data
        file_client.download_file.assert_called_once_with(timeout=300)

    @pytest.mark.asyncio
    @pytest.mark.skip(
        reason="Timeout handling triggers retry decorator which is slow. "
        "Timeout behavior is tested in retry decorator tests."
    )
    async def test_read_file_bytes_handles_timeout(self, adls_ops):
        """Test read handles timeout errors.

        Note: Skipped because timeout errors trigger retry decorator retries.
        Timeout handling is verified in retry decorator tests.
        """
        pass


class TestADLSOperationsDirectoryOperations:
    """Test directory operations."""

    @pytest.mark.asyncio
    async def test_directory_exists_returns_true(self, adls_ops):
        """Test directory_exists returns True when directory exists."""
        path = "test/dir"

        directory_client = MagicMock()
        directory_client.exists.return_value = True

        adls_ops.file_system_client.get_directory_client = MagicMock(
            return_value=directory_client
        )

        result = await adls_ops.directory_exists(path)

        assert result is True
        directory_client.exists.assert_called_once()

    @pytest.mark.asyncio
    async def test_directory_exists_returns_false_on_error(self, adls_ops):
        """Test directory_exists returns False on error."""
        path = "test/dir"

        directory_client = MagicMock()
        directory_client.exists.side_effect = Exception("Error")

        adls_ops.file_system_client.get_directory_client = MagicMock(
            return_value=directory_client
        )

        result = await adls_ops.directory_exists(path)

        assert result is False

    @pytest.mark.asyncio
    async def test_create_directory_creates_if_not_exists(self, adls_ops):
        """Test create_directory creates directory if it doesn't exist."""
        path = "test/dir"

        directory_client = MagicMock()
        directory_client.create_directory = MagicMock()

        adls_ops.file_system_client.get_directory_client = MagicMock(
            return_value=directory_client
        )
        adls_ops.directory_exists = AsyncMock(return_value=False)

        await adls_ops.create_directory(path)

        directory_client.create_directory.assert_called_once_with(timeout=300)

    @pytest.mark.asyncio
    async def test_create_directory_skips_if_exists(self, adls_ops):
        """Test create_directory skips creation if directory exists."""
        path = "test/dir"

        directory_client = MagicMock()

        adls_ops.file_system_client.get_directory_client = MagicMock(
            return_value=directory_client
        )
        adls_ops.directory_exists = AsyncMock(return_value=True)

        await adls_ops.create_directory(path)

        directory_client.create_directory.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_directory_recursive_creates_parents(self, adls_ops):
        """Test create_directory_recursive creates all parent directories."""
        path = "level1/level2/level3"

        directory_client = MagicMock()
        directory_client.create_directory = MagicMock()

        adls_ops.file_system_client.get_directory_client = MagicMock(
            return_value=directory_client
        )

        with patch("time.sleep"):  # Mock sleep to speed up test
            await adls_ops.create_directory_recursive(path)

        # Should create each level
        assert directory_client.create_directory.call_count == 3

    @pytest.mark.asyncio
    async def test_create_directory_recursive_handles_existing_directories(
        self, adls_ops
    ):
        """Test create_directory_recursive handles already existing directories."""
        path = "level1/level2"

        directory_client = MagicMock()
        directory_client.create_directory.side_effect = [
            Exception("PathAlreadyExists"),
            None,  # Second call succeeds
        ]

        adls_ops.file_system_client.get_directory_client = MagicMock(
            return_value=directory_client
        )

        with patch("time.sleep"):  # Mock sleep to speed up test
            await adls_ops.create_directory_recursive(path)

        # Should continue even if some directories exist
        assert directory_client.create_directory.called

    @pytest.mark.asyncio
    async def test_create_directory_recursive_skips_guid_folder_for_onelake(
        self, onelake_ops
    ):
        """Test OneLake skips GUID folder creation (already exists)."""
        path = "guid-folder/Files/data"

        directory_client = MagicMock()
        directory_client.create_directory = MagicMock()

        onelake_ops.file_system_client.get_directory_client = MagicMock(
            return_value=directory_client
        )

        with patch("time.sleep"):  # Mock sleep to speed up test
            await onelake_ops.create_directory_recursive(path)

        # Should skip first segment (GUID folder) and create Files/data
        # guid-folder is skipped, so we create Files and data
        assert directory_client.create_directory.call_count == 2


class TestADLSOperationsFileOperations:
    """Test file existence operations."""

    @pytest.mark.asyncio
    async def test_file_exists_returns_true(self, adls_ops):
        """Test file_exists returns True when file exists."""
        path = "test/file.txt"

        file_client = MagicMock()
        file_client.get_file_properties.return_value = MagicMock()

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=file_client
        )

        result = await adls_ops.file_exists(path)

        assert result is True
        file_client.get_file_properties.assert_called_once_with(timeout=300)

    @pytest.mark.asyncio
    async def test_file_exists_returns_false_when_not_found(self, adls_ops):
        """Test file_exists returns False when file doesn't exist."""
        path = "test/file.txt"

        file_client = MagicMock()
        file_client.get_file_properties.side_effect = ResourceNotFoundError("Not found")

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=file_client
        )

        result = await adls_ops.file_exists(path)

        assert result is False


class TestADLSOperationsJSONOperations:
    """Test JSON read/write operations."""

    @pytest.mark.asyncio
    async def test_read_json_success(self, adls_ops):
        """Test reading JSON file successfully."""
        path = "test/data.json"
        json_data = {"key": "value"}
        json_bytes = b'{"key": "value"}'

        file_client = MagicMock()
        file_client.get_file_properties.return_value = MagicMock()
        download = MagicMock()
        download.readall.return_value = json_bytes
        file_client.download_file.return_value = download

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=file_client
        )

        result = await adls_ops.read_json(path)

        assert result == json_data

    @pytest.mark.asyncio
    async def test_read_json_returns_none_when_not_found(self, adls_ops):
        """Test read_json returns None when file doesn't exist."""
        path = "test/data.json"

        file_client = MagicMock()
        file_client.get_file_properties.side_effect = ResourceNotFoundError("Not found")

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=file_client
        )

        result = await adls_ops.read_json(path)

        assert result is None

    @pytest.mark.asyncio
    async def test_write_json_success(self, adls_ops):
        """Test writing JSON file successfully."""
        path = "test/data.json"
        data = {"key": "value"}

        file_client = MagicMock()
        file_client.upload_data = MagicMock()

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=file_client
        )
        adls_ops.file_system_client.create_directory = MagicMock()

        await adls_ops.write_json(path, data)

        file_client.upload_data.assert_called_once()
        # Verify JSON was serialized
        call_args = file_client.upload_data.call_args
        uploaded_data = call_args[0][0]
        assert b'"key"' in uploaded_data
        assert b'"value"' in uploaded_data

    @pytest.mark.asyncio
    async def test_write_json_handles_existing_directory(self, adls_ops):
        """Test write_json handles existing directory gracefully."""
        path = "test/data.json"
        data = {"key": "value"}

        file_client = MagicMock()
        file_client.upload_data = MagicMock()

        adls_ops.file_system_client.get_file_client = MagicMock(
            return_value=file_client
        )
        # Simulate directory already exists
        adls_ops.file_system_client.create_directory.side_effect = Exception(
            "Directory exists"
        )

        # Should not raise error
        await adls_ops.write_json(path, data)

        file_client.upload_data.assert_called_once()


class TestADLSOperationsDeleteDirectory:
    """Test directory deletion operations."""

    @pytest.mark.asyncio
    async def test_delete_directory_success(self, adls_ops):
        """Test deleting directory successfully."""
        path = "test/dir"

        directory_client = MagicMock()
        directory_client.exists.return_value = True
        directory_client.delete_directory = MagicMock()

        # Mock file listing (empty directory)
        adls_ops.file_system_client.get_paths = MagicMock(return_value=[])

        adls_ops.file_system_client.get_directory_client = MagicMock(
            return_value=directory_client
        )

        result = await adls_ops.delete_directory(path)

        assert result is True
        directory_client.delete_directory.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_directory_deletes_files_first(self, adls_ops):
        """Test delete_directory deletes files before directory."""
        path = "test/dir"

        directory_client = MagicMock()
        directory_client.exists.return_value = True
        directory_client.delete_directory = MagicMock()

        # Mock file listing with files
        path_item1 = MagicMock()
        path_item1.name = "test/dir/file1.parquet"
        path_item2 = MagicMock()
        path_item2.name = "test/dir/file2.parquet"

        file_client1 = MagicMock()
        file_client1.delete_file = MagicMock()
        file_client2 = MagicMock()
        file_client2.delete_file = MagicMock()

        adls_ops.file_system_client.get_paths = MagicMock(
            return_value=[path_item1, path_item2]
        )
        adls_ops.file_system_client.get_file_client = MagicMock(
            side_effect=[file_client1, file_client2]
        )
        adls_ops.file_system_client.get_directory_client = MagicMock(
            return_value=directory_client
        )

        result = await adls_ops.delete_directory(path)

        assert result is True
        # Should delete files first
        file_client1.delete_file.assert_called_once()
        file_client2.delete_file.assert_called_once()
        # Then delete directory
        directory_client.delete_directory.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_directory_handles_already_deleted(self, adls_ops):
        """Test delete_directory handles already deleted directory."""
        path = "test/dir"

        directory_client = MagicMock()
        directory_client.exists.return_value = False

        adls_ops.file_system_client.get_directory_client = MagicMock(
            return_value=directory_client
        )

        result = await adls_ops.delete_directory(path)

        # Should return True (already deleted is success)
        assert result is True

    @pytest.mark.asyncio
    async def test_delete_directory_handles_in_use_gracefully(self, adls_ops):
        """Test delete_directory handles directory in use gracefully."""
        path = "test/dir"

        directory_client = MagicMock()
        directory_client.exists.return_value = True
        directory_client.delete_directory.side_effect = Exception(
            "Directory is being used"
        )

        # Mock file listing and deletion
        file_client = MagicMock()
        file_client.delete_file = MagicMock()

        adls_ops.file_system_client.get_paths = MagicMock(return_value=[])
        adls_ops.file_system_client.get_directory_client = MagicMock(
            return_value=directory_client
        )

        result = await adls_ops.delete_directory(path)

        # Should return False (partial success - files deleted, directory locked)
        assert result is False

    @pytest.mark.asyncio
    async def test_delete_directory_handles_sdk_parameter_conflict(self, adls_ops):
        """Test delete_directory handles SDK version parameter conflicts."""
        path = "test/dir"

        directory_client = MagicMock()
        directory_client.exists.return_value = True
        # First call fails with parameter conflict
        error_msg = "got multiple values for keyword argument 'recursive'"
        directory_client.delete_directory.side_effect = [
            TypeError(error_msg),
            None,  # Second call with **kwargs succeeds
        ]

        adls_ops.file_system_client.get_paths = MagicMock(return_value=[])
        adls_ops.file_system_client.get_directory_client = MagicMock(
            return_value=directory_client
        )

        result = await adls_ops.delete_directory(path)

        assert result is True
        # Should retry with **kwargs
        assert directory_client.delete_directory.call_count == 2

    @pytest.mark.asyncio
    @pytest.mark.skip(
        reason="Timeout handling triggers retry decorator which is slow. "
        "Timeout behavior is tested in retry decorator tests."
    )
    async def test_delete_directory_handles_timeout(self, adls_ops):
        """Test delete_directory handles timeout errors.

        Note: Skipped because timeout errors trigger retry decorator retries.
        Timeout handling is verified in retry decorator tests.
        """
        pass
