"""
MS SQL Server connection factory with Azure AD authentication.

Ports proven pattern from elk2 with improvements:
- Async-friendly with asyncio.to_thread() for all blocking operations
- Instance-based token caching (thread-safe)
- Implements BaseConnection interface
- Cleaner API (no env var dependencies, direct parameters)
"""
import asyncio
import struct
import time
from typing import Any, Dict, Optional

import pyodbc
from azure.core.credentials import AccessToken
from azure.identity import DefaultAzureCredential

from hygge.messages import get_logger
from hygge.utility.exceptions import HomeConnectionError, HomeError

from .base import BaseConnection


class MssqlConnection(BaseConnection):
    """
    MS SQL Server connection factory with Azure AD authentication.

    Features:
    - Azure AD authentication via DefaultAzureCredential
    - Token caching with 5-minute expiry buffer
    - All blocking operations wrapped in asyncio.to_thread()
    - Thread-safe instance-based token cache

    Authentication:
    - Supports Managed Identity (Azure)
    - Supports Azure CLI (local development)
    - Supports Environment Credentials
    - And more via DefaultAzureCredential

    Example:
        ```python
        factory = MssqlConnection(
            server="myserver.database.windows.net",
            database="mydatabase",
            options={"driver": "ODBC Driver 18 for SQL Server"}
        )
        conn = await factory.get_connection()
        ```
    """

    # SQL Server constant for access token
    SQL_COPT_SS_ACCESS_TOKEN = 1256

    # Token refresh buffer (seconds before expiry)
    TOKEN_EXPIRY_BUFFER = 300  # 5 minutes

    def __init__(
        self, server: str, database: str, options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize MSSQL connection factory.

        Args:
            server: SQL Server address (e.g., "myserver.database.windows.net")
            database: Database name
            options: Additional options:
                - driver: ODBC driver name (default: "ODBC Driver 18 for SQL Server")
                - encrypt: Enable encryption (default: "Yes")
                - trust_cert: Trust server certificate (default: "Yes")
                - timeout: Connection timeout in seconds (default: 30)
        """
        super().__init__(server, database, options)

        # Azure credential (reused across connections)
        self._credential = DefaultAzureCredential()

        # Instance-based token cache (thread-safe per instance)
        self._token: Optional[AccessToken] = None

        # Connection options with defaults
        self.driver = self.options.get("driver", "ODBC Driver 18 for SQL Server")
        self.encrypt = self.options.get("encrypt", "Yes")
        self.trust_cert = self.options.get("trust_cert", "Yes")
        self.timeout = self.options.get("timeout", 30)

        # Logging
        self.logger = get_logger("hygge.connections.mssql")

    async def get_connection(self) -> pyodbc.Connection:
        """
        Create and return a new MSSQL connection with Azure AD auth.

        Wraps all blocking operations in asyncio.to_thread() to prevent
        blocking the event loop.

        Returns:
            pyodbc.Connection: Database connection

        Raises:
            HomeError: If connection fails
        """
        try:
            # Get Azure AD token (blocking operation)
            token = await self._get_token()

            # Build connection string and attributes
            conn_str, attrs_before = self._build_connection_string(token)

            # Connect (blocking operation - wrap in thread)
            self.logger.debug(f"Connecting to {self._mask_server()}.{self.database}")
            conn = await asyncio.to_thread(
                pyodbc.connect, conn_str, attrs_before=attrs_before
            )

            self.logger.debug(f"Connected to {self._mask_server()}.{self.database}")
            return conn

        except pyodbc.Error as e:
            error_msg = str(e)
            sqlstate = getattr(e, "sqlstate", None)

            # Check for ODBC driver not found error
            if "IM002" in error_msg:
                raise HomeError(
                    f"ODBC Driver not found. Expected: {self.driver}. "
                    f"Install with: brew install msodbcsql18"
                ) from e

            # Use SQLSTATE for connection errors
            # (08xxx are connection-related per SQL standard)
            if sqlstate and sqlstate.startswith("08"):
                # CRITICAL: Use 'from e' to preserve exception context
                raise HomeConnectionError(f"Connection error: {error_msg}") from e

            # Fallback: Check for known connection error SQLSTATE in error message
            # (Only if SQLSTATE attribute is not available)
            if not sqlstate and "08S01" in error_msg:
                # CRITICAL: Use 'from e' to preserve exception context
                raise HomeConnectionError(f"Connection error: {error_msg}") from e

            # Other database errors
            raise HomeError(f"Database connection failed: {error_msg}") from e
        except Exception as e:
            # CRITICAL: Use 'from e' to preserve exception context
            raise HomeError(
                f"Failed to create MSSQL connection: {str(e)}"
            ) from e

    async def close_connection(self, conn: pyodbc.Connection) -> None:
        """
        Close a database connection.

        Wraps blocking close operation in asyncio.to_thread().

        Args:
            conn: Connection to close
        """
        try:
            if conn:
                await asyncio.to_thread(conn.close)
                self.logger.debug("Connection closed")
        except Exception as e:
            self.logger.warning(f"Error closing connection: {str(e)}")

    async def is_connection_alive(self, conn: pyodbc.Connection) -> bool:
        """
        Check if a connection is still alive.

        Performs a simple query to verify connection health.

        Args:
            conn: Connection to check

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            # Simple health check query (blocking - wrap in thread)
            await asyncio.to_thread(lambda: conn.execute("SELECT 1").fetchone())
            return True
        except Exception:
            return False

    async def _get_token(self) -> AccessToken:
        """
        Get Azure AD token with caching.

        Caches token and only refreshes when within TOKEN_EXPIRY_BUFFER
        seconds of expiration.

        Returns:
            AccessToken: Azure AD access token
        """
        # Check if we have a valid cached token
        if self._token:
            time_remaining = self._token.expires_on - time.time()
            if time_remaining > self.TOKEN_EXPIRY_BUFFER:
                self.logger.debug(
                    f"Using cached token ({time_remaining:.0f}s remaining)"
                )
                return self._token

        # Get new token (blocking operation - wrap in thread)
        self.logger.debug("Fetching new Azure AD token")
        self._token = await asyncio.to_thread(
            self._credential.get_token, "https://database.windows.net/.default"
        )

        time_remaining = self._token.expires_on - time.time()
        self.logger.debug(f"New token acquired ({time_remaining:.0f}s until expiry)")
        return self._token

    def _build_connection_string(self, token: AccessToken) -> tuple[str, Dict]:
        """
        Build ODBC connection string and attributes.

        Args:
            token: Azure AD access token

        Returns:
            Tuple of (connection_string, attrs_before_dict)
        """
        # Convert token to MS Windows byte string
        token_bytes = self._convert_token_to_bytes(token)

        # Build connection string
        conn_str = (
            f"DRIVER={self.driver};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"Encrypt={self.encrypt};"
            f"TrustServerCertificate={self.trust_cert};"
            f"Timeout={self.timeout}"
        )

        # Attributes to pass before connection
        attrs_before = {self.SQL_COPT_SS_ACCESS_TOKEN: token_bytes}

        return conn_str, attrs_before

    def _convert_token_to_bytes(self, token: AccessToken) -> bytes:
        """
        Convert Azure AD token to MS Windows byte string format.

        SQL Server expects a length-prefixed UTF-8 encoded byte string with
        each character interleaved with a zero byte (similar to UTF-16LE).
        See Microsoft docs for ODBC Azure AD authentication.

        Args:
            token: Azure AD access token

        Returns:
            Token in MS Windows byte string format
        """
        # Encode token string as UTF-16LE (per Microsoft documentation)
        encoded_bytes = token.token.encode("utf-16-le")
        # Pack with length prefix
        return struct.pack("<i", len(encoded_bytes)) + encoded_bytes

    def _mask_server(self) -> str:
        """Mask server name for logging (show only first part)."""
        if "." in self.server:
            return self.server.split(".")[0]
        return self.server
