"""
Parquet file home implementation.
"""
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

import polars as pl

from hygge.core.home import Home
from hygge.utility.exceptions import HomeError


class ParquetHome(Home):
    """
    A Parquet file home for data.

    Features:
    - Efficient batch reading with polars
    - Progress tracking

    Example:
        ```python
        home = ParquetHome(
            "users",
            path="data/users.parquet",
            options={
                'batch_size': 10_000
            }
        )
        ```
    """

    def __init__(
        self,
        name: str,
        path: str,
        options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(name, options)
        self.path = Path(path)

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """Get data iterator for parquet file."""
        try:
            self.logger.debug(f"Reading from {self.name} at {self.path}")

            # Use polars' streaming capabilities
            lf = pl.scan_parquet(self.path)
            df = lf.collect(engine='streaming')

            # Yield the entire DataFrame as a single batch
            yield df

        except Exception as e:
            raise HomeError(f"Failed to read parquet from {self.path}: {str(e)}")
