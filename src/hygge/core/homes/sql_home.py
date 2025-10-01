"""
SQL database home implementation.
"""
from typing import Any, AsyncIterator, Dict, Optional

import polars as pl

from hygge.core.home import Home


class SQLHome(Home):
    """
    A SQL database home for data.

    Features:
    - Efficient batch reading with polars
    - Progress tracking

    Example:
        ```python
        home = SQLHome(
            "users",
            connection=conn,
            options={
                'table': 'users',
                'schema': 'dbo',
                'batch_size': 10_000
            }
        )
        ```
    """

    def __init__(
        self,
        name: str,
        connection,
        options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(name, options)
        self.connection = connection

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """
        Get data in batches from the SQL database.

        Uses polars for efficient batch processing and memory management.
        """
        query = await self._prepare_query()
        self.logger.debug(f"Reading from {self.name} using query: {query}")

        for df in pl.read_database(
            query,
            self.connection,
            iter_batches=True,
            batch_size=self.batch_size
        ):
            yield df

    async def _prepare_query(self) -> str:
        """
        Build the SQL query from table and schema configuration.
        """

        # Build query from schema and table
        table = self.options.get('table', self.name)
        schema = self.options.get('schema', 'dbo')

        return f"SELECT * FROM {schema}.{table}"

    async def close(self) -> None:
        """Clean up database connection."""
        if hasattr(self.connection, 'close'):
            await self.connection.close()