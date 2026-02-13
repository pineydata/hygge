"""
File format layer: read/write Polars DataFrames by format (parquet, csv, ndjson).

Separates "what format" from "where". Used by LocalHome/LocalStore and can be
reused by GDrive/OneDrive. No registry—simple dispatch by format name.
"""

from io import BytesIO
from pathlib import Path
from typing import Any, Iterator

import polars as pl

# Format → file extension for globbing and generated filenames
FORMAT_SUFFIX: dict[str, str] = {
    "parquet": ".parquet",
    "csv": ".csv",
    "ndjson": ".ndjson",
}


def format_to_suffix(format_name: str) -> str:
    """Return the file extension for a format (e.g. parquet -> .parquet)."""
    suffix = FORMAT_SUFFIX.get(format_name.lower())
    if suffix is None:
        raise ValueError(f"Unknown format: {format_name}. Known: {list(FORMAT_SUFFIX)}")
    return suffix


def read(
    path: Path | str,
    format_name: str,
    batch_size: int = 50_000,
    **options: Any,
) -> Iterator[pl.DataFrame]:
    """
    Read file(s) at path in batches by format. Always yields batches.

    Parquet: scan_parquet + slice + collect(engine="streaming").
    CSV: scan_csv + slice + collect(engine="streaming").
    NDJSON: read in chunks (batch_size lines).
    """
    path = Path(path)
    fmt = format_name.lower()

    if fmt == "parquet":
        yield from _read_parquet(path, batch_size, **options)
    elif fmt == "csv":
        yield from _read_csv(path, batch_size, **options)
    elif fmt == "ndjson":
        yield from _read_ndjson(path, batch_size, **options)
    else:
        raise ValueError(f"Unknown format: {format_name}. Known: {list(FORMAT_SUFFIX)}")


def _read_parquet(
    path: Path,
    batch_size: int,
    **options: Any,
) -> Iterator[pl.DataFrame]:
    """Parquet: streaming via scan_parquet + slice + collect(engine='streaming')."""
    lf = pl.scan_parquet(path, **options)
    total_rows = lf.select(pl.len()).collect().item()
    if total_rows == 0:
        return
    num_batches = (total_rows + batch_size - 1) // batch_size
    for batch_idx in range(num_batches):
        offset = batch_idx * batch_size
        batch_df = lf.slice(offset, batch_size).collect(engine="streaming")
        if len(batch_df) > 0:
            yield batch_df


def _read_csv(
    path: Path,
    batch_size: int,
    **options: Any,
) -> Iterator[pl.DataFrame]:
    """CSV: streaming via scan_csv + slice + collect(engine='streaming')."""
    lf = pl.scan_csv(path, **options)
    total_rows = lf.select(pl.len()).collect().item()
    if total_rows == 0:
        return
    num_batches = (total_rows + batch_size - 1) // batch_size
    for batch_idx in range(num_batches):
        offset = batch_idx * batch_size
        batch_df = lf.slice(offset, batch_size).collect(engine="streaming")
        if len(batch_df) > 0:
            yield batch_df


def _read_ndjson(
    path: Path,
    batch_size: int,
    **options: Any,
) -> Iterator[pl.DataFrame]:
    """NDJSON: chunked read (batch_size lines per batch)."""
    if not path.is_file():
        raise FileNotFoundError(f"NDJSON path must be a file: {path}")
    with open(path, "rb") as f:
        batch_lines: list[bytes] = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            batch_lines.append(line)
            if len(batch_lines) >= batch_size:
                yield pl.read_ndjson(BytesIO(b"\n".join(batch_lines)), **options)
                batch_lines = []
        if batch_lines:
            yield pl.read_ndjson(BytesIO(b"\n".join(batch_lines)), **options)


def write(
    df: pl.DataFrame,
    path: Path | str,
    format_name: str,
    **options: Any,
) -> None:
    """
    Write a single DataFrame to path in the given format.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = format_name.lower()

    if fmt == "parquet":
        df.write_parquet(path, **options)
    elif fmt == "csv":
        df.write_csv(path, **options)
    elif fmt == "ndjson":
        df.write_ndjson(path, **options)
    else:
        raise ValueError(f"Unknown format: {format_name}. Known: {list(FORMAT_SUFFIX)}")
