# Welcome Data from Everywhere

**Status:** Horizon 2 – New Homes
**Scope:** Multi-cloud and SaaS integrations

## Context

Your data already has a home. hygge should meet it there – not ask it to move first.

Whether your data lives in **GCS buckets** or **BigQuery tables**, in **S3** or in **Salesforce**, hygge will come to it. No workarounds, no export-import gymnastics. Just comfortable connections to where your data already feels at home.

And for the modern analytics crowd: **DuckDB** is the cozy database – local-first, fast, simple. Like a warm fireplace for your data. **MotherDuck** takes that same warmth to the cloud. Both are coming as first-class residents of hygge.

## The Vision

Data from anywhere can settle comfortably into hygge. Multi-cloud shouldn't feel scattered – it should feel seamless. Your data's home is wherever it lives, and hygge will meet it there.

## Implementation Plan

### Foundation: Staged Write Pattern

**The pattern:** Every cloud store needs to write to staging, then atomically commit. Right now this is duplicated across ParquetStore, ADLSStore, OneLakeStore, and OpenMirroringStore (~200 lines each).

**The solution:** Extract a `StagedWriteMixin` that provides common logic (tracking paths, atomic commit, rollback), while stores implement store-specific parts.

**Why first:** This is the universal reliable idempotent pattern: home → tmp staging → final store. It's foundational for ALL cloud stores (current: ADLS, OneLake, OpenMirroring; future: GCS, S3, and beyond). Enables GCS Store and S3 Store without copy-pasting, adds proper rollback (currently missing), and every future cloud store inherits the pattern.

See [staged-writer-abstraction.md](staged-writer-abstraction.md) for technical details.

### Google Cloud Platform

| Component | Your data's new home |
|-----------|---------------------|
| GCS Home & Store | Parquet files, cozy in Cloud Storage |
| BigQuery Home & Store | Bring data home from BigQuery tables—or send data comfortably back to BigQuery as a store, too |

**Integration approach:**
- Use Google's official `google-cloud-*` libraries
- Find the most performant path to Polars (Arrow preferred, then CSV)
- Keep library interaction in Home/Store implementation
- Let hygge handle the orchestration

### AWS

| Component | Your data's new home |
|-----------|---------------------|
| S3 Home & Store | Parquet files, settled comfortably in S3 |

**Integration approach:**
- Use `boto3` and `s3fs` (the standard)
- Follow staged write pattern for atomic commits
- Leverage S3's multipart upload for large files

### Modern Analytics

| Component | Your data's new home |
|-----------|---------------------|
| DuckDB Home & Store | Local DuckDB – a warm fireplace for your data |
| MotherDuck Home & Store | That same warmth, in the cloud |

**Integration approach:**
- Use `duckdb` library (the database *is* the library)
- Zero-copy Arrow exchange – data moves without getting cold
- Leverage DuckDB's native Polars integration where possible

### SaaS Sources

| Component | Your data's new home |
|-----------|---------------------|
| Salesforce Home | Accounts, Contacts, Opportunities – finally home |

**Integration approach:**
- Use `simple-salesforce` (battle-tested, handles OAuth complexity)
- Bulk API 2.0 returns CSV chunks that flow directly to Polars
- Handle pagination and rate limiting gracefully

## Success Criteria

After this work:
- [ ] GCP data feels at home
- [ ] AWS data feels at home
- [ ] DuckDB workflows feel natural
- [ ] Salesforce data flows home easily
- [ ] Multi-cloud feels seamless, not scattered
- [ ] Data from anywhere can settle comfortably into hygge

## Technical Principles

### Stand on Cozy Shoulders

We wrap proven tools in hygge's comfortable patterns – we don't rebuild them.

When great tools exist, we use them:
- `simple-salesforce` for Salesforce
- Google's official libraries for GCP
- `boto3` / `s3fs` for AWS
- `duckdb` for DuckDB
- `pyodbc` for SQL Server

We find the most performant path to Polars, keep the library interaction in the Home/Store implementation, and let hygge handle the orchestration.

### Polars is Home

All data flows through Polars DataFrames. This is hygge's warm center.

**Homes** get data *into* Polars – whatever the source format (CSV, JSON, database, API), the Home handles the conversion and produces Polars DataFrames.

**Stores** get data *out of* Polars – whatever the destination needs (parquet, database, cloud storage), the Store handles the conversion and consumes Polars DataFrames.

**Flows** never think about formats. They just move Polars DataFrames from Home to Store. The conversion complexity stays at the edges, where it belongs.

## Integration Patterns

When welcoming new sources, we find the coziest path to Polars:

| Source output | The warmest path to Polars |
|---------------|---------------------------|
| Arrow tables | `pl.from_arrow()` – zero copy, instant warmth |
| CSV streams | `pl.read_csv()` – native and natural |
| Database cursors | `pl.read_database()` – batched with care |

### Salesforce Integration Example

Built on `simple-salesforce`. Bulk API 2.0 returns CSV chunks that flow directly to Polars:

```python
results = sf.bulk2.Account.query(query="SELECT Id, Name FROM Account")
for csv_chunk in results:
    df = pl.read_csv(io.StringIO(csv_chunk))
```

### DuckDB Integration Example

Zero-copy Arrow exchange – data moves without getting cold:

```python
# DuckDB → Polars
df = pl.from_arrow(conn.execute("SELECT * FROM table").fetch_arrow_table())

# Polars → DuckDB
conn.register("view", df.to_arrow())
```

## Related Issues

- [staged-writer-abstraction.md](staged-writer-abstraction.md) – Foundation for cloud stores
- Part of [ROADMAP.md](ROADMAP.md) Horizon 2: New Homes
