# File Format Abstraction and Local Home/Store

**Status:** Ready for implementation
**Scope:** Separate "where" (location) from "what format" (file type). Introduce a minimal format layer and LocalHome/LocalStore with a `format` key. Parquet streaming preserved; chunking from day one.

**Depends on:** Nothing. Enables [google-drive-integration.md](google-drive-integration.md) and future path-based stores (OneDrive/SharePoint) to reuse the same format logic.

---

## Context

Today, ParquetHome and ParquetStore bake "local path" and "parquet format" into one type. We want to support multiple file formats (parquet, CSV, NDJSON) and multiple locations (local disk, GDrive, later OneDrive/SharePoint) without N×M classes. The format layer answers: "how do we read/write Polars ↔ this file type?" Locations answer: "how do we resolve paths and credentials?"

**Principle:** Polars is home. Format is the edge—read/write at the boundary. Flow stays DataFrame → DataFrame.

---

## Design: Easy-World

Keep the design minimal.

### Format layer

- **Two functions** in a single place (e.g. `hygge.core.formats`):
  - **`read(path, format, batch_size=50_000, **options) → Iterator[pl.DataFrame]`**
    Always yields batches. No "read whole file" API; the flow only needs batches.
  - **`write(df, path, format, **options) → None`**
    Writes one DataFrame to the given path.
- **Dispatch:** Simple `if format == "parquet": ... elif format == "csv": ...` (or a small dict). One branch per format. No registry class, no protocol.
- **Options:** Pass-through `**options` (e.g. compression for parquet, delimiter/encoding for CSV). No upfront validation; Polars validates when called.

### Parquet: preserve streaming

For parquet, **do not** "load then chunk." Keep the current behavior:

- `lf = pl.scan_parquet(path)`
- Loop: `lf.slice(offset, batch_size).collect(engine="streaming")`, yield each batch.

The format layer's parquet branch implements exactly this. Streaming execution is preserved.

### CSV and others

- CSV: Chunked read (e.g. `pl.read_csv` with `n_rows`/batch_size in a loop, or Polars' batched/streaming API) so large files don't OOM. Yield each chunk.
- NDJSON (or JSON lines): Same idea—read in chunks, yield DataFrames.

### Config

- **`type`** stays the home/store type (e.g. `local`). No breaking change to the type key.
- **`format`** (and optionally **`format_options`**) added where appropriate—i.e. for file-based home/store configs.
  - Example: `type: local`, `path: data/`, `format: parquet`, `format_options: { compression: snappy }`.
- Non–file-based types (mssql, open_mirroring, etc.) do not use `format`; their config models ignore it.

### LocalHome / LocalStore

- Replace (or refactor) ParquetHome/ParquetStore as **LocalHome** / **LocalStore** registered as e.g. `type: local`.
- They own: path resolution, batching (by calling the format layer's `read()` in a loop), staging, `file_pattern`, directory listing for read.
- They delegate actual I/O to the format layer: `read(path, format, batch_size, **format_options)` and `write(df, path, format, **format_options)`.
- **File pattern / extension:** Format-aware. A small mapping (e.g. `{"parquet": ".parquet", "csv": ".csv"}`) or helper so the right extension is used for globbing and generated filenames.

### Backward compatibility

- No reliance on "ParquetXXX" in the wild; we can rename and add `format` without preserving old type names. If existing configs use `type: parquet`, we can either keep that as an alias for `type: local, format: parquet` or migrate configs to `type: local` + `format: parquet`. Decide at implementation time.

---

## Implementation outline

1. **Add `hygge.core.formats`** (or equivalent):
   - `read(path, format, batch_size=50_000, **options) -> Iterator[pl.DataFrame]`
   - `write(df, path, format, **options) -> None`
   - Parquet: scan_parquet + slice + collect(engine="streaming"); CSV (and NDJSON if needed) chunked.
   - Extension/pattern helper for format → suffix (e.g. `.parquet`, `.csv`).

2. **Introduce LocalHome / LocalStore** (or rename existing Parquet*):
   - Config: `path`, `format` (default `parquet`), optional `format_options`, plus existing options (batch_size, file_pattern, compression where applicable).
   - Home: resolve path(s), list files by format extension, call `read()` and yield batches (sync iterator from async via executor or thin wrapper).
   - Store: staging + `write()` + move to final; file_pattern and extension from format.

3. **Register** as `type: local` (and optionally keep `type: parquet` as alias).

4. **Tests:** Format layer (read/write each format, chunking); LocalHome/LocalStore with parquet and CSV; parquet streaming behavior unchanged.

---

## Success criteria

- [ ] Format layer: `read()` yields batches for parquet (streaming) and CSV (chunked); `write()` works for parquet and CSV.
- [ ] LocalHome/LocalStore use `type: local` and `format` (+ optional `format_options`).
- [ ] Parquet path still uses `scan_parquet` + `collect(engine="streaming")`; no regression.
- [ ] Config and CLI work with `type: local` and `format: parquet` (and optionally `format: csv`).
- [ ] GDrive (and later OneDrive/SharePoint) can reuse the same format layer for read/write.

---

## Relationship to other work

- **[google-drive-integration.md](google-drive-integration.md):** GDrive Home/Store will use the same format layer (read/write DataFrame ↔ file type). Implement format abstraction and Local first; then GDrive is "path resolution + credentials + same format layer."
- **OneDrive/SharePoint:** Fast follow after GDrive; same pattern—path + auth + format layer.
- **Welcome Data from Everywhere (GCP, AWS, Salesforce, DuckDB):** Unchanged; this work is independent and prioritized before it so that "You Don't Need a Big Cloud Vendor" (local + GDrive + OneDrive/SharePoint) comes first.

---

## What we're not doing (easy-world)

- No registry or protocol for formats; just two functions and a simple dispatch.
- No pluggable third-party formats; adding a format = one more branch and Polars call.
- No upfront validation of format_options; Polars validates at call time.
- Chunking is the only contract for `read()`; we don't add a "read whole file" API.
