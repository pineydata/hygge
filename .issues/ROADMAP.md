# hygge Roadmap

> *Making data movement feel like home*

**Last updated:** February 13, 2026

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

### First: Does hygge *feel* hyggesque? ✅

**Status:** Complete (as of January 2026)

We made every interaction feel warm. hygge now feels like a friend keeping you company, not a tool you're wrestling with.

**What we built:**

1. **Enhanced `hygge debug`** – One command that checks everything:
   - Tests connections to databases and cloud storage
   - Verifies credentials and authentication
   - Checks that source paths exist and destinations are writable
   - Surfaces issues with clear guidance on how to fix them

2. **`--dry-run` flag for `hygge go`** – Preview before you commit:
   - Shows which flows and entities would run
   - Source → destination mappings for each
   - Incremental vs. full load strategy
   - Configuration warnings that need attention
   - No connections, no data movement - instant feedback

3. **Narrative progress messages** – Storytelling during data movement:
   - Warm language with emojis and concrete file paths
   - Clear journey logging (source → destination)
   - Batch completion messages with running totals
   - Milestone messages that feel celebratory

4. **Enhanced completion summaries** – Satisfaction when data settles home:
   - Celebratory success messages with detailed flow results
   - Compassionate error guidance with actionable next steps
   - Shows where data settled for each successful flow

**Next:** Friendly error messages (deferred to Horizon 2, see [friendly-error-messages.md](friendly-error-messages.md))

---

### Then: You Don't Need a Big Cloud Vendor

Not everyone needs a big cloud vendor. Many teams live in Google Drive, OneDrive, or SharePoint—lightweight, familiar, and already where their data is. hygge should meet them there first.

**Priority order:**

1. **File format abstraction + Local Home/Store** – Separate "where" from "what format." A minimal format layer (read/write in batches, parquet streaming preserved) and LocalHome/LocalStore with a `format` key. Enables multiple file types (parquet, CSV) and sets up GDrive and others to reuse the same logic.
   See [file-format-abstraction-and-local-home-store.md](file-format-abstraction-and-local-home-store.md).

2. **Google Drive (Home & Store)** – Ingest from Drive, land to Drive (and optionally Google Sheets). Versioned/append/overwrite; no warehouse required.
   See [google-drive-integration.md](google-drive-integration.md).

3. **OneDrive / SharePoint (fast follow)** – Same pattern as GDrive: path + auth + the same format layer. Data feels at home where people already work.

**Success feels like:** You can run cozy pipelines without committing to a major cloud platform—local files, Drive, or SharePoint first; big cloud when you need it.

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
| Azure-focused | Local + Drive + SharePoint first; big cloud when you need it |
| Parquet-only file types | Format abstraction: parquet, CSV, any file type where it fits |
| Database sources | SaaS and everyday storage – GDrive, then Salesforce, DuckDB |
| Technical CLI | Warm, cozy CLI that feels like a friend |
| Works reliably | Works reliably *and feels like a warm blanket* |
| Unstable API patterns | Stable API with schema management and data quality |
| Basic capabilities | Table stakes: schema drift, evolution, validation |

---

## The Journey

### Horizon 1: Warmth ✅

**Status:** Complete (January 2026)

**Made the CLI feel like wrapping up in hygge.**

When you run hygge, you feel like a friend is keeping you company – not like you're wrestling with a tool. Progress messages tell a story, not just spit out numbers. When your data finally settles into its new home, you feel that quiet satisfaction – like sinking into a cozy chair after a long day.

We made `hygge debug` your trusted companion – one command that checks your configs, tests your connections, and tells you if anything feels off before you begin. And `hygge go --dry-run` lets you peek ahead, see exactly what would happen, no surprises.

**Success achieved:** Users reach for hygge because it feels comfortable, not just because it works.

**Still to do:** Friendly error messages (deferred to Horizon 2)

---

### Horizon 2: New Homes

**Let data settle comfortably, wherever it lives.**

#### You Don't Need a Big Cloud Vendor (First)

Before we add GCP, AWS, and Salesforce, we're making path-based, everyday storage first-class: local disk with any format, then Google Drive, then OneDrive/SharePoint. No warehouse required.

| Component | Priority | Issue |
|-----------|----------|--------|
| File format abstraction + LocalHome/LocalStore | 1st | [file-format-abstraction-and-local-home-store.md](file-format-abstraction-and-local-home-store.md) |
| Google Drive Home & Store (optional Sheets store) | 2nd | [google-drive-integration.md](google-drive-integration.md) |
| OneDrive / SharePoint Home & Store | 3rd (fast follow) | Same pattern as GDrive |

**Why first:** Real need for lightweight, cloud-light pipelines; format abstraction unlocks GDrive and SharePoint without N×M implementations.

#### Cloud-Aware ParquetHome (Priority: GA360 Support)

**Why prioritized:** Marcomm team is becoming a GA360 client. GA360 exports to BigQuery (processed analytics data) and GCS (raw hit-level exports). GCP support is the forcing function.

**The insight:** Polars handles cloud storage natively via `storage_options`. We don't need separate GCSHome, ADLSHome, S3Home classes - we extend ParquetHome to be cloud-aware. **One class, all clouds, DRY.**

| Component | Your data's new home | Priority |
|-----------|---------------------|----------|
| Cloud-Aware ParquetHome | Read parquet from local, GCS, ADLS, S3 via `provider` key | 1st |
| BigQuery Home | Read GA360 sessionized data (database, separate class) | 2nd |
| Cloud-Aware Stores | Apply same DRY pattern to stores | 3rd |

#### Staged Write Pattern

**The pattern:** Every cloud store needs to write to staging, then atomically commit. Right now this is duplicated across ParquetStore, ADLSStore, OneLakeStore, and OpenMirroringStore (~200 lines each).

**The solution:** Extract a `StagedWriteMixin` that provides common logic (tracking paths, atomic commit, rollback), while stores implement store-specific parts.

**Why after GCP Homes:** Homes don't need this pattern. Build GCS Home and BigQuery Home first, then extract the mixin before building GCS Store and BigQuery Store.

See [staged-writer-abstraction.md](staged-writer-abstraction.md) for technical details.

#### AWS

| Component | Your data's new home |
|-----------|---------------------|
| S3 via ParquetHome | `provider: s3` - included in cloud-aware ParquetHome |
| S3 Store | Apply same cloud-aware pattern to stores (later) |

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

### After Horizon 1 ✅

- [x] The CLI feels warm and welcoming
- [x] Progress messages tell a story
- [x] Debug and dry-run give confidence before running
- [x] Completion feels satisfying
- [ ] Errors feel like gentle guidance (deferred to Horizon 2)

### After "You Don't Need a Big Cloud Vendor"

- [ ] Local home/store support multiple formats (parquet, CSV) via `format` key
- [ ] Google Drive home and store work; optional Sheets store
- [ ] OneDrive/SharePoint home and store (fast follow)
- [ ] Pipelines run without a big cloud vendor when that's what you need

### After Horizon 2 (Welcome Data from Everywhere)

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
| You don't need a big cloud vendor first | GDrive, OneDrive, SharePoint meet many teams where they already work; format abstraction + local then cloud-light stores before GCP/AWS/Salesforce |
| Format abstraction (easy-world) | Two functions, chunking from day one, parquet streaming preserved; enables Local + GDrive + OneDrive without N×M classes |
| GCP before AWS | Marcomm team is becoming GA360 client - real need drives priority |
| One ParquetHome for all clouds | Polars handles cloud natively - DRY over duplication |
| Explicit `provider` key | Pythonic: explicit is better than implicit |
| Homes before Stores | Homes don't need StagedWriteMixin - get reading data faster |
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
| Local files | Parquet ✓, CSV (via format), Local + format | Parquet ✓, CSV ✓, SQLite ✓, Local + format |
| Google Drive | GDrive Home (parquet, CSV) | GDrive Store, Google Sheets |
| OneDrive/SharePoint | OneDrive/SharePoint Home | OneDrive/SharePoint Store (fast follow) |
| Azure | SQL Server ✓, ADLS (via ParquetHome) | ADLS ✓, OneLake ✓, SQL Server ✓ |
| AWS | S3 (via ParquetHome) | S3 |
| GCP | GCS (via ParquetHome), BigQuery | GCS, BigQuery |
| SaaS | Salesforce | — |
| MotherDuck | MotherDuck | MotherDuck |

> **Note:** Format abstraction (file-format-abstraction-and-local-home-store) gives Local + `format` key; GDrive and OneDrive/SharePoint reuse the same format layer. GCS, ADLS, S3 parquet reading via cloud-aware ParquetHome with `provider` key.

---

## Appendix: Technical Details

For the cozy implementation specifics, see:

- [file-format-abstraction-and-local-home-store.md](file-format-abstraction-and-local-home-store.md) – Format layer + LocalHome/LocalStore (You Don't Need a Big Cloud Vendor)
- [google-drive-integration.md](google-drive-integration.md) – Google Drive and Sheets home/store
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
