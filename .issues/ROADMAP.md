# hygge Roadmap

> *Making data movement feel like home*

---

## The Story So Far

hygge started with a simple idea: moving data shouldn't feel like a struggle. It should feel natural, comfortable, reliable. Like wrapping yourself in a warm blanket on a cold morning.

We built that foundation. It works. It's tested. It's solid.

**Now it's time to make hygge feel like home.**

### Our Core Principles

Two commitments guide everything we build:

#### Polars is Home

All data flows through Polars DataFrames. This is hygge's warm center.

**Homes** get data *into* Polars – whatever the source format (CSV, JSON, database, API), the Home handles the conversion and produces Polars DataFrames.

**Stores** get data *out of* Polars – whatever the destination needs (parquet, database, cloud storage), the Store handles the conversion and consumes Polars DataFrames.

**Flows** never think about formats. They just move Polars DataFrames from Home to Store. The conversion complexity stays at the edges, where it belongs.

This means when we add a new source or destination, we only need to build the Home or Store – the rest of hygge already knows how to work with Polars.

#### Stand on Cozy Shoulders

We wrap proven tools in hygge's comfortable patterns – we don't rebuild them.

When great tools exist, we use them: `simple-salesforce` for Salesforce, Google's official libraries for GCP, `boto3` for AWS, `duckdb` for DuckDB, `pyodbc` for SQL Server. We find the most performant path to Polars, keep the library interaction in the Home/Store implementation, and let hygge handle the orchestration.

This means you get battle-tested reliability where it matters most, wrapped in hygge's comfortable patterns.

*For the full architectural details and integration guidance, see [CLAUDE.md](../CLAUDE.md).*

And while we're focused on making hygge feel like home today, we're also keeping an eye on the horizon – thinking about how modern data formats like Iceberg, Delta Lake, and DuckLake might one day find their place in the hygge family, and how an adapter system could welcome community contributions when the time is right.

---

## What's Next

### First: Does hygge *feel* hyggesque?

See [hygge-feels-hyggesque.md](hygge-feels-hyggesque.md) for the full issue.

We've been so focused on making it work that we haven't stopped to ask: **does it feel cozy to use?**

When you run `hygge go`, do you feel wrapped in comfort or left out in the cold? When something goes wrong, do you feel guided home or abandoned in the dark? When it finishes, does your data feel settled and at home?

**Our first priority is making every interaction feel warm.**

We'll audit every moment:

- The messages you see while your data journeys home
- The errors you encounter when things go sideways
- The first time you welcome hygge into a new project
- The moment your data finally settles into its new home

Then we'll make it cozy. Reassuring. Like a warm cup of coffee waiting for you.

Aim for messages like:

```text
Settling 300,000 rows into their new home...
- Connecting to source
- Reading data batch 1/10 (30,000 rows)
- Writing batch to destination: data/users.parquet
- Schema mapped: 10 columns, 0 warnings
- Verifying integrity and finishing up
```

Replace generic, mechanical feedback ("Processing 300,000 rows...") with concrete, narrative updates that clearly tell the user what's happening—data counts, files, batches, finishing steps—in plain, approachable language.

Improvements coming:

**Enhanced `hygge debug`** – One command that checks everything.

Today `hygge debug` shows discovered flows and validates config structure. We'll extend it to also:

- Test connections to databases and cloud storage
- Verify credentials and authentication are working
- Check that source paths exist and destinations are writable
- Surface any issues with clear guidance on how to fix them

One command, complete peace of mind before you run.

**`--dry-run` flag for `hygge go`** – "Show me what would happen first."

Preview mode that shows exactly what `hygge go` would do, without moving any data:

- Which flows and entities would run
- Source → destination mappings for each
- Row counts and schema information from sources
- What files/tables would be created or overwritten
- Any warnings or potential issues

Perfect for validating a new flow before committing, or checking what an incremental run would pick up.

---

### Then: Welcome Data from Everywhere

See [welcome-data-from-everywhere.md](welcome-data-from-everywhere.md) for the full issue.

Your data already has a home. hygge should meet it there – not ask it to move first.

Whether your data lives in **GCS buckets** or **BigQuery tables**, in **S3** or in **Salesforce**, hygge will come to it. No workarounds, no export-import gymnastics. Just comfortable connections to where your data already feels at home.

And for the modern analytics crowd: **DuckDB** is the cozy database – local-first, fast, simple. Like a warm fireplace for your data. **MotherDuck** takes that same warmth to the cloud. Both are coming as first-class residents of hygge.

---

### Finally: Stabilize the API with Table Stakes

See [table-stakes-capabilities.md](table-stakes-capabilities.md) for the full analysis.

Before we expand further, we need to stabilize what we have. After making hygge feel hyggesque and welcoming data from major cloud platforms, we'll stabilize the API and build foundational capabilities.

**The need:** Pipelines break from schema drift. Bad data pollutes destinations. Quality issues go undetected.

**Tier 1 (High Priority):**

- **Schema drift detection & handling** - Capture schema, detect changes, handle drift (warn/fail/adapt)
- **Schema evolution support** - Handle schema changes over time with clear policies

**Tier 2 (Medium Priority):**

- **Data contracts & quality checks** - Define what data should look like, validate before writing, check completeness/uniqueness/freshness/volume

**Why now:** Schema management (drift + evolution) is the #1 cause of pipeline breaks. Building this now:

- Validates Flow/Store interfaces
- Reveals what needs to be public API vs. internal
- Provides immediate production value
- Foundation for data validation capabilities

**Implementation sequence:** Schema management (Tier 1) → Core homes/stores → Data validation (Tier 2).

**Success feels like:** The API is stable, patterns are clear, and future expansion is straightforward.

---

### The Bigger Picture

| Where we are | Where we're going |
|--------------|-------------------|
| Azure-focused | Multi-cloud – data feels at home everywhere |
| Database sources | SaaS sources – Salesforce finds a home first |
| Technical CLI | Warm, cozy CLI that feels like a friend |
| Works reliably | Works reliably *and feels like a warm blanket* |
| Unstable API patterns | Stable API with schema management and data quality |
| Basic capabilities | Table stakes: schema drift, evolution, validation |

---

## The Journey

### Horizon 1: Warmth

**Make the CLI feel like wrapping up in hygge.**

When you run hygge, you should feel like a friend is keeping you company – not like you're wrestling with a tool. Progress messages should tell a story, not spit out numbers. Errors should guide you home, not leave you stranded. The first time you try hygge should feel like being invited in from the cold.

We'll make `hygge debug` your trusted companion – one command that checks your configs, tests your connections, and tells you if anything feels off before you begin. And `hygge go --dry-run` will let you peek ahead, see exactly what would happen, no surprises.

When your data finally settles into its new home, you should feel that quiet satisfaction – like sinking into a cozy chair after a long day.

**Success feels like:** Users reach for hygge because it feels comfortable, not just because it works.

---

### Horizon 2: New Homes

**Let data settle comfortably, wherever it lives.**

#### Staged Write Pattern

**The pattern:** Every cloud store needs to write to staging, then atomically commit. Right now this is duplicated across ParquetStore, ADLSStore, OneLakeStore, and OpenMirroringStore (~200 lines each).

**The solution:** Extract a `StagedWriteMixin` that provides common logic (tracking paths, atomic commit, rollback), while stores implement store-specific parts.

**Why first:** This is the universal reliable idempotent pattern: home → tmp staging → final store. It's foundational for ALL cloud stores (current: ADLS, OneLake, OpenMirroring; future: GCS, S3, and beyond). Enables GCS Store and S3 Store without copy-pasting, adds proper rollback (currently missing), and every future cloud store inherits the pattern.

See [staged-writer-abstraction.md](staged-writer-abstraction.md) for technical details.

#### Google Cloud

| Component | Your data's new home |
|-----------|---------------------|
| GCS Home & Store | Parquet files, cozy in Cloud Storage |
| BigQuery Home & Store | Bring data home from BigQuery tables—or send data comfortably back to BigQuery as a store, too |

#### AWS

| Component | Your data's new home |
|-----------|---------------------|
| S3 Home & Store | Parquet files, settled comfortably in S3 |

#### Modern Analytics

| Component | Your data's new home |
|-----------|---------------------|
| DuckDB Home & Store | Local DuckDB – a warm fireplace for your data |
| MotherDuck Home & Store | That same warmth, in the cloud |

#### SaaS

| Component | Your data's new home |
|-----------|---------------------|
| Salesforce Home | Accounts, Contacts, Opportunities – finally home |

**Success feels like:** Data from anywhere can settle comfortably into hygge.

---

### Horizon 3: API Stabilization & Foundation

**Before we expand further, stabilize what we have.**

After making hygge feel hyggesque (Horizon 1) and welcoming data from major cloud platforms (Horizon 2), we need to stabilize the API and build foundational capabilities.

#### Data Engineering Capabilities

**The need:** Pipelines break from schema drift. Bad data pollutes destinations. Quality issues go undetected.

**Tier 1 (High Priority):**

- **Schema drift detection & handling** - Capture schema, detect changes, handle drift (warn/fail/adapt)
- **Schema evolution support** - Handle schema changes over time with clear policies

**Tier 2 (Medium Priority):**

- **Data contracts & quality checks** - Define what data should look like, validate before writing, check completeness/uniqueness/freshness/volume

**Must-have sources/destinations (can wait):**

- PostgreSQL, MySQL, CSV, JSON

These follow established patterns (like MSSQL) and would exercise the Home/Store interfaces, but they can wait. This is a pragmatic compromise for solo development - we'll build them when needed rather than proactively.

**Why now:** Schema management (drift + evolution) is the #1 cause of pipeline breaks. Building this now:

- Validates Flow/Store interfaces
- Reveals what needs to be public API vs. internal
- Provides immediate production value
- Foundation for data validation capabilities

**Implementation sequence:** Schema management (Tier 1) → Core homes/stores → Data validation (Tier 2).

See [table-stakes-capabilities.md](table-stakes-capabilities.md) for the full analysis and implementation strategy.

**Success feels like:** The API is stable, patterns are clear, and future expansion is straightforward.

---

### Over the Horizon (Way Future - No Plans to Implement)

Landmark targets we're thinking about, but no concrete plans. These won't block current work:

- **Snowflake** – If the cold calls to you
- **HubSpot** – B2B/marketing data
- **Stripe** – Payments and e-commerce
- **Shopify** – E-commerce platforms
- **Data Formats** – Would your data feel cozier in **Iceberg**, **Delta Lake**, or **DuckLake**? Maybe it's time to consider bringing these modern formats into the hygge fold, so data can snuggle into the structure and mutability it deserves.

#### Adapter System

When hygge has community contributors and we need to enable extensibility without PR bottlenecks, we'll implement an adapter system – pip-installable packages that register automatically.

**The vision:** `pip install hygge hygge-mssql hygge-azure` – install what you need, adapters auto-discover.

**Why wait:** We're still iterating on core patterns. Building adapters now would lock in an unstable API. Better to wait until:

- Core API is stable (Horizon 3 complete)
- We have real community contributions
- We understand what patterns need to be stable

See [adapter-system-design.md](adapter-system-design.md) for the full technical design (when we're ready).

---

## How We'll Know It Feels Right

### After Horizon 1

- [ ] The CLI feels warm and welcoming
- [ ] Errors feel like gentle guidance, not cold rejection
- [ ] New users feel invited in from the cold
- [ ] "Cozy" and "comfortable" are words people use

### After Horizon 2

- [ ] GCP data feels at home
- [ ] DuckDB workflows feel natural
- [ ] Salesforce data flows home easily
- [ ] Multi-cloud feels seamless, not scattered

### After Horizon 3

- [ ] The API is stable and clear
- [ ] Schema drift is handled gracefully
- [ ] PostgreSQL, MySQL, CSV, JSON feel like first-class citizens (when built)
- [ ] Data validation gives confidence
- [ ] Cloud stores follow consistent patterns (established in Horizon 2, stabilized in Horizon 3)

### Long-term

- [ ] hygge is *the* cozy choice for data movement
- [ ] True multi-cloud: Azure, AWS, GCP all feel like home
- [ ] Community members build new homes and welcome new data
- [ ] The philosophy spreads: comfort over complexity, always
- [ ] Adapter system enables community contributions (when ready)

---

## Decisions We've Made

| Decision | Why |
|----------|-----|
| Warmth comes first | The feeling *is* the product |
| GCP and AWS together | Data should feel at home everywhere |
| DuckDB before enterprise databases | It shares our cozy philosophy |
| Salesforce as first SaaS source | Important data deserves a comfortable home |

---

## Where Data Feels at Home Today

**Current homes:**

| Platform      | Data can come from    | Data can settle into            |
|---------------|----------------------|---------------------------------|
| Local files   | Parquet ✓            | Parquet ✓, SQLite ✓             |
| Azure         | SQL Server ✓         | ADLS ✓, OneLake ✓, SQL Server ✓ |
| AWS           | —                    | —                               |
| GCP           | —                    | —                               |
| SaaS          | —                    | —                               |

**New homes coming:**

| Platform | Data can come from | Data can settle into |
|----------|-------------------|---------------------|
| Local files | Parquet ✓, DuckDB | Parquet ✓, SQLite ✓, DuckDB |
| Azure | SQL Server ✓ | ADLS ✓, OneLake ✓, SQL Server ✓ |
| AWS | S3 | S3 |
| GCP | GCS, BigQuery | GCS, BigQuery |
| SaaS | Salesforce | — |
| MotherDuck | MotherDuck | MotherDuck |

---

## Appendix: Technical Details

For the cozy implementation specifics, see:

- [table-stakes-capabilities.md](table-stakes-capabilities.md) – Data engineering capabilities (schema drift, contracts, quality)
- [staged-writer-abstraction.md](staged-writer-abstraction.md) – Staged write pattern for cloud stores
- [adapter-system-design.md](adapter-system-design.md) – Adapter system (over the horizon)
- [__TECHNICAL_REVIEW_SUMMARY.md](__TECHNICAL_REVIEW_SUMMARY.md) – Architecture review

### Integration Patterns

When welcoming new sources, we find the coziest path to Polars:

| Source output | The warmest path to Polars |
|---------------|---------------------------|
| Arrow tables | `pl.from_arrow()` – zero copy, instant warmth |
| CSV streams | `pl.read_csv()` – native and natural |
| Database cursors | `pl.read_database()` – batched with care |

### Salesforce Integration

Built on `simple-salesforce`. Bulk API 2.0 returns CSV chunks that flow directly to Polars:

```python
results = sf.bulk2.Account.query(query="SELECT Id, Name FROM Account")
for csv_chunk in results:
    df = pl.read_csv(io.StringIO(csv_chunk))
```

### DuckDB Integration

Zero-copy Arrow exchange – data moves without getting cold:

```python
# DuckDB → Polars
df = pl.from_arrow(conn.execute("SELECT * FROM table").fetch_arrow_table())

# Polars → DuckDB
conn.register("view", df.to_arrow())
```

---

*This roadmap reflects where we're headed. It will evolve as we learn what makes your data feel most at home.*
