"""
Unit tests for MssqlConnection.

Note: These tests mock Azure credentials and pyodbc to avoid
requiring actual database connections or Azure authentication.
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from azure.core.credentials import AccessToken

from hygge.connections.mssql import MssqlConnection


@pytest.fixture
def mock_credential():
    """Create a mock Azure credential."""
    mock_cred = MagicMock()
    # Create a token that expires in 1 hour
    mock_token = AccessToken(
        token="mock_access_token_string", expires_on=time.time() + 3600
    )
    mock_cred.get_token.return_value = mock_token
    return mock_cred


@pytest.fixture
def mssql_connection():
    """Create an MssqlConnection instance."""
    with patch("hygge.connections.mssql.DefaultAzureCredential"):
        conn = MssqlConnection(
            server="test.database.windows.net",
            database="testdb",
            options={
                "driver": "ODBC Driver 18 for SQL Server",
                "encrypt": "Yes",
                "trust_cert": "Yes",
                "timeout": 30,
            },
        )
        return conn


def test_mssql_connection_init(mssql_connection):
    """Test MssqlConnection initialization."""
    assert mssql_connection.server == "test.database.windows.net"
    assert mssql_connection.database == "testdb"
    assert mssql_connection.driver == "ODBC Driver 18 for SQL Server"
    assert mssql_connection.encrypt == "Yes"
    assert mssql_connection.trust_cert == "Yes"
    assert mssql_connection.timeout == 30
    assert mssql_connection._token is None


def test_mssql_connection_defaults():
    """Test MssqlConnection uses defaults correctly."""
    with patch("hygge.connections.mssql.DefaultAzureCredential"):
        conn = MssqlConnection(server="test.database.windows.net", database="testdb")

        assert conn.driver == "ODBC Driver 18 for SQL Server"
        assert conn.encrypt == "Yes"
        assert conn.trust_cert == "Yes"
        assert conn.timeout == 30


@pytest.mark.asyncio
async def test_token_caching(mssql_connection, mock_credential):
    """Test token caching - should reuse token if not expired."""
    mssql_connection._credential = mock_credential

    # First call - should fetch token
    token1 = await mssql_connection._get_token()
    assert token1.token == "mock_access_token_string"
    assert mock_credential.get_token.call_count == 1

    # Second call - should use cached token
    token2 = await mssql_connection._get_token()
    assert token2.token == "mock_access_token_string"
    assert mock_credential.get_token.call_count == 1  # Not called again

    # Tokens should be the same instance
    assert token1 is token2


@pytest.mark.asyncio
async def test_token_refresh_when_expiring(mssql_connection, mock_credential):
    """Test token refreshes when within expiry buffer."""
    mssql_connection._credential = mock_credential

    # Create token that expires in 4 minutes (within 5-minute buffer)
    expiring_token = AccessToken(
        token="expiring_token",
        expires_on=time.time() + 240,  # 4 minutes
    )

    # Create fresh token for refresh
    fresh_token = AccessToken(token="fresh_token", expires_on=time.time() + 3600)

    mock_credential.get_token.side_effect = [expiring_token, fresh_token]

    # First call - gets expiring token
    token1 = await mssql_connection._get_token()
    assert token1.token == "expiring_token"
    assert mock_credential.get_token.call_count == 1

    # Second call - should refresh because token is expiring soon
    token2 = await mssql_connection._get_token()
    assert token2.token == "fresh_token"
    assert mock_credential.get_token.call_count == 2


def test_convert_token_to_bytes(mssql_connection):
    """Test token conversion to MS Windows byte string format."""
    token = AccessToken(token="test_token", expires_on=time.time() + 3600)

    token_bytes = mssql_connection._convert_token_to_bytes(token)

    # Should be bytes
    assert isinstance(token_bytes, bytes)

    # Should have length prefix (4 bytes) + encoded content
    # Length should be 4-byte prefix + 2 bytes per character in token
    expected_length = 4 + len(token.token) * 2
    assert len(token_bytes) == expected_length


def test_build_connection_string(mssql_connection):
    """Test connection string building."""
    token = AccessToken(token="test_token", expires_on=time.time() + 3600)

    conn_str, attrs_before = mssql_connection._build_connection_string(token)

    # Check connection string format
    assert "DRIVER=ODBC Driver 18 for SQL Server" in conn_str
    assert "SERVER=test.database.windows.net" in conn_str
    assert "DATABASE=testdb" in conn_str
    assert "Encrypt=Yes" in conn_str
    assert "TrustServerCertificate=Yes" in conn_str
    assert "Timeout=30" in conn_str

    # Check attrs_before has token
    assert 1256 in attrs_before  # SQL_COPT_SS_ACCESS_TOKEN
    assert isinstance(attrs_before[1256], bytes)


def test_mask_server(mssql_connection):
    """Test server name masking for logging."""
    assert mssql_connection._mask_server() == "test"

    # Test with no dots
    with patch("hygge.connections.mssql.DefaultAzureCredential"):
        conn = MssqlConnection(server="localhost", database="testdb")
        assert conn._mask_server() == "localhost"


@pytest.mark.asyncio
async def test_get_connection_wraps_in_thread(mssql_connection, mock_credential):
    """Test that get_connection wraps blocking calls in asyncio.to_thread."""
    mssql_connection._credential = mock_credential

    mock_pyodbc_conn = MagicMock()

    with patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:
        # Setup mock returns
        mock_to_thread.side_effect = [
            # First call: get_token
            AccessToken(token="test", expires_on=time.time() + 3600),
            # Second call: pyodbc.connect
            mock_pyodbc_conn,
        ]

        try:
            await mssql_connection.get_connection()
        except Exception:
            pass  # We're just checking that to_thread was called

        # Should have called asyncio.to_thread at least once
        assert mock_to_thread.call_count >= 1


@pytest.mark.asyncio
async def test_close_connection_wraps_in_thread(mssql_connection):
    """Test that close_connection wraps blocking close in asyncio.to_thread."""
    mock_conn = MagicMock()

    with patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:
        await mssql_connection.close_connection(mock_conn)

        # Should have called asyncio.to_thread for conn.close()
        assert mock_to_thread.call_count == 1


@pytest.mark.asyncio
async def test_is_connection_alive(mssql_connection):
    """Test connection health check."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.return_value = (1,)

    with patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:
        mock_to_thread.return_value = None  # Simulates successful query

        is_alive = await mssql_connection.is_connection_alive(mock_conn)

        # Should return True for successful health check
        assert is_alive is True


@pytest.mark.asyncio
async def test_is_connection_alive_handles_errors(mssql_connection):
    """Test connection health check returns False on error."""
    mock_conn = MagicMock()

    with patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:
        mock_to_thread.side_effect = Exception("Connection dead")

        is_alive = await mssql_connection.is_connection_alive(mock_conn)

        # Should return False for failed health check
        assert is_alive is False
