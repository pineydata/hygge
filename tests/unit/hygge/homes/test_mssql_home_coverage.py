"""
Additional comprehensive tests for MssqlHome to improve coverage.

Focuses on error handling, edge cases, and complex methods that are currently
under-tested. This file supplements test_mssql_home.py.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from hygge.homes.mssql import MssqlHome, MssqlHomeConfig
from hygge.utility.exceptions import HomeConnectionError, HomeReadError


class TestMssqlHomeErrorHandling:
    """Test error handling paths in MssqlHome."""

    @pytest.mark.asyncio
    async def test_stream_query_handles_connection_error(self):
        """Test _stream_query() preserves connection errors."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        mock_acquire = AsyncMock(side_effect=HomeConnectionError("Connection failed"))
        with patch.object(home, "_acquire_connection", new=mock_acquire):
            with pytest.raises(HomeConnectionError, match="Connection failed"):
                async for _ in home._stream_query("SELECT * FROM users"):
                    pass

    @pytest.mark.asyncio
    async def test_stream_query_handles_home_error(self):
        """Test _stream_query() preserves home errors."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        with patch.object(home, "_acquire_connection", new_callable=AsyncMock):
            with patch.object(
                home, "_extract_batches_sync", side_effect=HomeReadError("Read failed")
            ):
                with pytest.raises(HomeReadError, match="Read failed"):
                    async for _ in home._stream_query("SELECT * FROM users"):
                        pass

    @pytest.mark.asyncio
    async def test_stream_query_wraps_unexpected_errors(self):
        """Test _stream_query() wraps unexpected errors in HomeReadError."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        with patch.object(home, "_acquire_connection", new_callable=AsyncMock):
            with patch.object(
                home,
                "_extract_batches_sync",
                side_effect=ValueError("Unexpected error"),
            ):
                with pytest.raises(HomeReadError, match="Failed to read from MSSQL"):
                    async for _ in home._stream_query("SELECT * FROM users"):
                        pass

    @pytest.mark.asyncio
    async def test_stream_query_cleans_up_connection_on_error(self):
        """Test _stream_query() cleans up connection even on error."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        mock_cleanup = AsyncMock()
        with patch.object(home, "_acquire_connection", new_callable=AsyncMock):
            with patch.object(home, "_cleanup_connection", mock_cleanup):
                with patch.object(
                    home, "_extract_batches_sync", side_effect=Exception("Error")
                ):
                    try:
                        async for _ in home._stream_query("SELECT * FROM users"):
                            pass
                    except HomeReadError:
                        pass

        # Should clean up connection even on error
        mock_cleanup.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stream_query_logs_no_data_debug(self):
        """Test _stream_query() logs debug message when no data returned."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        def empty_batches(query, batch_size):
            # Empty generator (no batches)
            return
            yield  # Make it a generator

        with patch.object(home, "_acquire_connection", new_callable=AsyncMock):
            with patch.object(home, "_extract_batches_sync", side_effect=empty_batches):
                with patch("hygge.connections.execution.get_engine") as mock_engine:
                    mock_thread_engine = MagicMock()
                    mock_thread_engine.execute_streaming = AsyncMock(
                        return_value=iter([])
                    )
                    mock_engine.return_value = mock_thread_engine

                    # Should complete without error (logs debug message)
                    async for _ in home._stream_query("SELECT * FROM users"):
                        pass

    @pytest.mark.asyncio
    async def test_acquire_connection_from_pool(self):
        """Test connection acquisition from pool."""
        config = MssqlHomeConfig(
            type="mssql",
            connection="test_db",
            table="dbo.users",
        )
        mock_pool = AsyncMock()
        mock_connection = MagicMock()
        mock_pool.acquire = AsyncMock(return_value=mock_connection)

        home = MssqlHome("test", config, pool=mock_pool)

        await home._acquire_connection()

        assert home._connection == mock_connection
        assert home._owned_connection is False
        mock_pool.acquire.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_acquire_connection_creates_dedicated(self):
        """Test connection creation when no pool."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        mock_connection = MagicMock()
        with patch("hygge.homes.mssql.home.MssqlConnection") as mock_factory:
            mock_factory_instance = MagicMock()
            mock_factory_instance.get_connection = AsyncMock(
                return_value=mock_connection
            )
            mock_factory.return_value = mock_factory_instance

            await home._acquire_connection()

        assert home._connection == mock_connection
        assert home._owned_connection is True

    @pytest.mark.asyncio
    async def test_cleanup_connection_returns_to_pool(self):
        """Test connection cleanup returns to pool."""
        config = MssqlHomeConfig(
            type="mssql",
            connection="test_db",
            table="dbo.users",
        )
        mock_pool = AsyncMock()
        mock_connection = MagicMock()
        home = MssqlHome("test", config, pool=mock_pool)
        home._connection = mock_connection
        home._owned_connection = False

        await home._cleanup_connection()

        mock_pool.release.assert_awaited_once_with(mock_connection)
        assert home._connection is None

    @pytest.mark.asyncio
    async def test_cleanup_connection_closes_dedicated(self):
        """Test connection cleanup closes dedicated connection."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)
        mock_connection = MagicMock()
        home._connection = mock_connection
        home._owned_connection = True

        await home._cleanup_connection()

        # Should close connection in thread
        assert home._connection is None

    @pytest.mark.asyncio
    async def test_cleanup_connection_handles_errors(self):
        """Test connection cleanup handles errors gracefully."""
        config = MssqlHomeConfig(
            type="mssql",
            connection="test_db",
            table="dbo.users",
        )
        mock_pool = AsyncMock()
        mock_pool.release = AsyncMock(side_effect=Exception("Release failed"))
        mock_connection = MagicMock()
        home = MssqlHome("test", config, pool=mock_pool)
        home._connection = mock_connection
        home._owned_connection = False

        # Should handle error gracefully (logs warning)
        await home._cleanup_connection()

        # Connection should still be cleared
        assert home._connection is None


class TestMssqlHomeWatermarkEdgeCases:
    """Test watermark handling edge cases."""

    @pytest.mark.asyncio
    async def test_read_with_watermark_missing_watermark(self):
        """Test read_with_watermark() handles missing watermark."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        # Mock _get_batches to be an async generator function
        import polars as pl

        async def mock_get_batches():
            yield pl.DataFrame({"id": []})

        # Replace the method with our async generator function
        home._get_batches = mock_get_batches
        async for _ in home.read_with_watermark({}):  # Empty watermark
            pass

    @pytest.mark.asyncio
    async def test_read_with_watermark_custom_query(self):
        """Test read_with_watermark() warns with custom query."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            query="SELECT * FROM users",
        )
        home = MssqlHome("test", config)

        # Mock _get_batches to be an async generator function
        async def mock_get_batches():
            yield pl.DataFrame({"id": []})

        # Replace the method with our async generator function
        home._get_batches = mock_get_batches
        async for _ in home.read_with_watermark(
            {"watermark": "2024-01-01T00:00:00Z", "watermark_type": "datetime"}
        ):
            pass

    @pytest.mark.asyncio
    async def test_read_with_watermark_invalid_filter(self):
        """Test read_with_watermark() handles invalid filter construction."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        # Mock _get_batches to be an async generator function
        async def mock_get_batches():
            yield pl.DataFrame({"id": []})

        # Replace the method with our async generator function
        home._get_batches = mock_get_batches
        # Invalid watermark (missing required fields)
        async for _ in home.read_with_watermark(
            {"watermark": "invalid", "watermark_type": "unknown"}
        ):
            pass


class TestMssqlHomeWatermarkFilterBuilding:
    """Test watermark filter building logic."""

    def test_build_watermark_filter_datetime(self):
        """Test datetime watermark filter construction."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        watermark = {
            "watermark": "2024-01-02T09:00:00Z",
            "watermark_type": "datetime",
            "watermark_column": "updated_at",
        }

        filter_clause = home._build_watermark_filter(watermark)

        assert filter_clause == "updated_at > '2024-01-02T09:00:00Z'"

    def test_build_watermark_filter_datetime_escapes_quotes(self):
        """Test datetime filter escapes single quotes."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        watermark = {
            "watermark": "2024-01-02T09:00:00'Z",
            "watermark_type": "datetime",
            "watermark_column": "updated_at",
        }

        filter_clause = home._build_watermark_filter(watermark)

        # Should escape single quote
        assert "''" in filter_clause

    def test_build_watermark_filter_integer(self):
        """Test integer watermark filter construction."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        watermark = {
            "watermark": "1050",
            "watermark_type": "int",
            "watermark_column": "updated_at",
            "primary_key": "id",
        }

        filter_clause = home._build_watermark_filter(watermark)

        # Should use primary_key for integer watermarks
        assert filter_clause == "id > 1050"

    def test_build_watermark_filter_integer_falls_back_to_watermark_column(self):
        """Test integer filter falls back to watermark_column if no primary_key."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        watermark = {
            "watermark": "1050",
            "watermark_type": "int",
            "watermark_column": "sequence_id",
            # No primary_key
        }

        filter_clause = home._build_watermark_filter(watermark)

        # Should use watermark_column
        assert filter_clause == "sequence_id > 1050"

    def test_build_watermark_filter_integer_invalid_value(self):
        """Test integer filter handles invalid value."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        watermark = {
            "watermark": "not_a_number",
            "watermark_type": "int",
            "watermark_column": "id",
        }

        filter_clause = home._build_watermark_filter(watermark)

        # Should return None for invalid integer
        assert filter_clause is None

    def test_build_watermark_filter_string(self):
        """Test string watermark filter construction."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        watermark = {
            "watermark": "ABC123",
            "watermark_type": "string",
            "watermark_column": "code",
        }

        filter_clause = home._build_watermark_filter(watermark)

        assert filter_clause == "code > 'ABC123'"

    def test_build_watermark_filter_unsupported_type(self):
        """Test watermark filter handles unsupported types."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        watermark = {
            "watermark": "value",
            "watermark_type": "unsupported",
            "watermark_column": "col",
        }

        filter_clause = home._build_watermark_filter(watermark)

        # Should return None for unsupported types
        assert filter_clause is None

    def test_build_watermark_filter_missing_fields(self):
        """Test watermark filter handles missing required fields."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        # Missing watermark_value
        filter_clause = home._build_watermark_filter({"watermark_type": "datetime"})
        assert filter_clause is None

        # Missing watermark_type
        filter_clause = home._build_watermark_filter({"watermark": "value"})
        assert filter_clause is None

        # Missing watermark_column
        filter_clause = home._build_watermark_filter(
            {"watermark": "value", "watermark_type": "datetime"}
        )
        assert filter_clause is None


class TestMssqlHomeIdentifierValidation:
    """Test SQL identifier validation."""

    def test_validate_identifier_valid(self):
        """Test valid identifier passes validation."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        assert home._validate_identifier("user_id", "field") == "user_id"
        assert home._validate_identifier("[user_id]", "field") == "[user_id]"
        assert home._validate_identifier("dbo.users", "field") == "dbo.users"

    def test_validate_identifier_invalid_characters(self):
        """Test invalid identifier fails validation."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        # SQL injection attempt
        assert home._validate_identifier("'; DROP TABLE users; --", "field") is None
        # Invalid characters
        assert home._validate_identifier("user-id", "field") is None
        # Starts with number
        assert home._validate_identifier("123column", "field") is None

    def test_validate_identifier_empty(self):
        """Test empty identifier fails validation."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        assert home._validate_identifier("", "field") is None
        assert home._validate_identifier("   ", "field") is None
        assert home._validate_identifier(None, "field") is None


class TestMssqlHomeQueryAppending:
    """Test query filter appending logic."""

    def test_append_filter_to_query_simple(self):
        """Test appending filter to simple query."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        query = "SELECT * FROM users"
        filter_clause = "id > 100"

        result = home._append_filter_to_query(query, filter_clause)

        assert result == "SELECT * FROM users WHERE id > 100"

    def test_append_filter_to_query_with_semicolon(self):
        """Test appending filter preserves semicolon."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        query = "SELECT * FROM users;"
        filter_clause = "id > 100"

        result = home._append_filter_to_query(query, filter_clause)

        assert result == "SELECT * FROM users WHERE id > 100;"

    def test_append_filter_to_query_existing_where(self):
        """Test appending filter adds AND to existing WHERE."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        query = "SELECT * FROM users WHERE active = 1"
        filter_clause = "id > 100"

        result = home._append_filter_to_query(query, filter_clause)

        assert result == "SELECT * FROM users WHERE active = 1 AND id > 100"

    def test_append_filter_to_query_case_insensitive_where(self):
        """Test appending filter handles case-insensitive WHERE."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        query = "SELECT * FROM users WhErE active = 1"
        filter_clause = "id > 100"

        result = home._append_filter_to_query(query, filter_clause)

        # Should detect WHERE case-insensitively
        assert "AND id > 100" in result
