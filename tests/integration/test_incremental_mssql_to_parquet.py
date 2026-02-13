"""
Incremental end-to-end example: synthetic MSSQL home → Parquet store.

Simulates two flow executions:
1. Full drop baseline load that writes two rows.
2. Incremental run that sees the journal watermark and only writes new rows.

Uses the filesystem-backed journal so no cloud dependencies are required.
"""

from __future__ import annotations

from pathlib import Path
from typing import AsyncIterator, Iterable

import polars as pl
import pytest

from hygge.core.flow import Flow
from hygge.core.home import Home
from hygge.core.journal import Journal, JournalConfig
from hygge.stores.parquet import ParquetStore, ParquetStoreConfig


class SyntheticMssqlHome(Home):
    """Minimal home that mimics MSSQL behaviour with watermark support."""

    def __init__(self, name: str, rows: Iterable[dict], *, batch_size: int = 1000):
        super().__init__(name, options={"batch_size": batch_size})
        self._dataframe = pl.DataFrame(rows)
        self.read_with_watermark_called = False

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        """Yield a single batch for full-load reads."""
        if not self._dataframe.is_empty():
            yield self._dataframe

    async def read_with_watermark(self, watermark: dict) -> AsyncIterator[pl.DataFrame]:
        """Filter rows using the provided journal watermark."""
        self.read_with_watermark_called = True

        df = self._dataframe
        threshold_value = watermark.get("watermark")
        watermark_column = watermark.get("watermark_column")
        watermark_type = watermark.get("watermark_type")

        if threshold_value is None or watermark_column not in df.columns:
            if not df.is_empty():
                yield df
            return

        if watermark_type == "int":
            threshold = int(threshold_value)
            filtered = df.filter(pl.col(watermark_column) > threshold)
        elif watermark_type == "datetime":
            threshold = pl.datetime(threshold_value)
            filtered = df.filter(pl.col(watermark_column) > threshold)
        else:
            filtered = df

        if not filtered.is_empty():
            yield filtered


@pytest.mark.asyncio
async def test_incremental_flow_mssql_to_parquet(tmp_path: Path):
    """Full-drop + incremental runs using a filesystem journal."""

    journal_dir = tmp_path / "journal"
    output_dir = tmp_path / "output"

    journal_config = JournalConfig(path=str(journal_dir))
    store_config = ParquetStoreConfig(path=str(output_dir))

    # First run: full drop baseline
    initial_rows = [
        {"id": 1, "updated_at": 1, "name": "Alice"},
        {"id": 2, "updated_at": 2, "name": "Bob"},
    ]
    home_full_drop = SyntheticMssqlHome("synthetic_home", initial_rows)
    store_full_drop = ParquetStore("synthetic_store", store_config)
    journal_full_drop = Journal(
        name="synthetic_journal",
        config=journal_config,
        coordinator_name="test_coordinator",
    )

    flow_full_drop = Flow(
        name="synthetic_flow_full_drop",
        home=home_full_drop,
        store=store_full_drop,
        options={"queue_size": 2},
        journal=journal_full_drop,
        coordinator_run_id="coord_run_1",
        flow_run_id="flow_run_1",
        coordinator_name="test_coordinator",
        base_flow_name="synthetic_flow",
        entity_name="synthetic_entity",
        run_type="full_drop",
        watermark_config={"primary_key": "id", "watermark_column": "updated_at"},
    )

    await flow_full_drop.start()

    first_run_files = sorted(output_dir.glob("*.parquet"))
    assert first_run_files, "First run did not produce any parquet files"
    first_run_df = pl.concat([pl.read_parquet(f) for f in first_run_files])
    assert sorted(first_run_df["id"].to_list()) == [1, 2]

    # Second run: incremental with additional row (and existing rows present)
    incremental_rows = [
        {"id": 1, "updated_at": 1, "name": "Alice"},
        {"id": 2, "updated_at": 2, "name": "Bob"},
        {"id": 3, "updated_at": 3, "name": "Charlie"},
    ]
    home_incremental = SyntheticMssqlHome("synthetic_home", incremental_rows)
    store_incremental = ParquetStore("synthetic_store", store_config)
    journal_incremental = Journal(
        name="synthetic_journal",
        config=journal_config,
        coordinator_name="test_coordinator",
    )

    flow_incremental = Flow(
        name="synthetic_flow_incremental",
        home=home_incremental,
        store=store_incremental,
        options={"queue_size": 2},
        journal=journal_incremental,
        coordinator_run_id="coord_run_2",
        flow_run_id="flow_run_2",
        coordinator_name="test_coordinator",
        base_flow_name="synthetic_flow",
        entity_name="synthetic_entity",
        run_type="incremental",
        watermark_config={"primary_key": "id", "watermark_column": "updated_at"},
    )

    await flow_incremental.start()

    assert home_incremental.read_with_watermark_called

    all_files = sorted(output_dir.glob("*.parquet"))
    assert (
        len(all_files) >= 2
    ), "Expected additional parquet output after incremental run"

    combined_df = pl.concat([pl.read_parquet(f) for f in all_files])
    assert sorted(combined_df["id"].to_list()) == [1, 2, 3]

    counts = combined_df.group_by("id").len().sort("id")["len"].to_list()
    assert counts == [1, 1, 1], "Incremental run should append only new rows"
