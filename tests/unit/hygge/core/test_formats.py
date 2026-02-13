"""
Tests for the format layer (read, write, format_to_suffix).

Verifies parquet (streaming), CSV (chunked), and NDJSON (chunked) read/write
and that format_to_suffix returns the correct extension.
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from hygge.core.formats import format_to_suffix
from hygge.core.formats import read as format_read
from hygge.core.formats import write as format_write


class TestFormatToSuffix:
    def test_parquet_suffix(self):
        assert format_to_suffix("parquet") == ".parquet"

    def test_csv_suffix(self):
        assert format_to_suffix("csv") == ".csv"

    def test_ndjson_suffix(self):
        assert format_to_suffix("ndjson") == ".ndjson"

    def test_case_insensitive(self):
        assert format_to_suffix("PARQUET") == ".parquet"
        assert format_to_suffix("CSV") == ".csv"

    def test_unknown_format_raises(self):
        with pytest.raises(ValueError, match="Unknown format"):
            format_to_suffix("unknown")


class TestFormatReadWriteParquet:
    def test_read_parquet_yields_batches(self):
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = Path(f.name)
        try:
            df = pl.DataFrame({"a": range(100), "b": [f"x{i}" for i in range(100)]})
            df.write_parquet(path)
            batches = list(format_read(path, "parquet", batch_size=30))
            assert len(batches) >= 1
            combined = pl.concat(batches)
            assert combined.shape == (100, 2)
            assert combined["a"].to_list() == list(range(100))
        finally:
            path.unlink(missing_ok=True)

    def test_write_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.parquet"
            df = pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            format_write(df, path, "parquet")
            assert path.exists()
            back = pl.read_parquet(path)
            assert back.equals(df)


class TestFormatReadWriteCsv:
    def test_read_csv_yields_batches(self):
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            f.write("a,b\n")
            for i in range(50):
                f.write(f"{i},x{i}\n")
            path = Path(f.name)
        try:
            batches = list(format_read(path, "csv", batch_size=20))
            assert len(batches) >= 1
            combined = pl.concat(batches)
            assert combined.shape[0] == 50
            assert "a" in combined.columns and "b" in combined.columns
        finally:
            path.unlink(missing_ok=True)

    def test_write_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            df = pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            format_write(df, path, "csv")
            assert path.exists()
            back = pl.read_csv(path)
            assert back.equals(df)


class TestFormatReadWriteNdjson:
    def test_read_ndjson_yields_batches(self):
        with tempfile.NamedTemporaryFile(
            suffix=".ndjson", delete=False, mode="wb"
        ) as f:
            for i in range(25):
                f.write(f'{{"a":{i},"b":"x{i}"}}\n'.encode())
            path = Path(f.name)
        try:
            batches = list(format_read(path, "ndjson", batch_size=10))
            assert len(batches) >= 1
            combined = pl.concat(batches)
            assert combined.shape[0] == 25
        finally:
            path.unlink(missing_ok=True)

    def test_write_ndjson(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.ndjson"
            df = pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            format_write(df, path, "ndjson")
            assert path.exists()
            back = pl.read_ndjson(path)
            assert back.shape == df.shape
