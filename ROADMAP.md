# hygge Roadmap

## Vision

hygge is an **Extract and Load (EL) toolkit** for solo developers and small teams. It focuses on moving data comfortably and reliably between systems, leaving transformations to dedicated tools like dbt.

**Core Philosophy:**
- **Extract and Load only** - No transformations, no DAGs, no orchestration
- **Comfort over complexity** - Simple cases should be effortless
- **Type-appropriate terminology** - SQL has `table`, files have `path`, APIs have `endpoint`
- **Convention over configuration** - Smart defaults with explicit overrides
- **Entity-centric design** - Logical data entities are first-class citizens

---

## Current State (v0.1.x)

### What Works
- ✅ Parquet file support (read/write)
- ✅ MS SQL Server support (read/write with Azure AD auth)
- ✅ **Automatic table creation with schema inference** (Oct 2025)
- ✅ Connection pooling for concurrent flows
- ✅ Entity pattern (basic - single config file)
- ✅ Flow-scoped logging
- ✅ Project-centric CLI (`hygge init`, `hygge start`, `hygge debug`)

### Recent Wins
- ✅ Schema inference: Polars → SQL Server type mapping
- ✅ Smart string sizing with buffer factors
- ✅ `if_exists` policies: fail (default), append, replace
- ✅ CCI by default (analytics-optimized)
- ✅ 225+ tests passing

---

## Architecture Evolution

### Current: Single-File Configuration
```yaml
# hygge.yml - Everything in one file
flows:
  crm_extract:
    home:
      type: mssql
      table: dbo.{entity}
    store:
      type: parquet
      path: data/{entity}
    entities: [Account, Contact, Opportunity]
```

**Limitations:**
- String templating for entity substitution (`{entity}`)
- Entity-specific config mixed with flow config
- Hard to manage 50+ entities in one file
- Schema overrides don't have natural home

### Future: Multi-File Entity Pattern
```
flows/
  └── crm_extract/
      ├── flow.yml          # Flow-level: pipes and defaults
      └── entities/
          ├── accounts.yml   # Entity-level: specifics
          ├── contacts.yml
          └── opportunities.yml
```

**Benefits:**
- Clear separation: flow (pipes) vs entity (specifics)
- One file per entity (scales to 100+ entities)
- Natural place for schema overrides
- Entity-specific config stays with entity
- Version control friendly (easy diffs per entity)

---

## Design Principles

### 1. Flow = The Pipe
Defines **how** data moves (connections, types, base locations):
```yaml
# flow.yml
name: crm_extract

home:
  type: mssql
  connection: salesforce_db
  # No table - entities provide specifics

store:
  type: parquet
  path: data_lake/crm/
  # Base path - entities append to it

defaults:
  batch_size: 50000
  if_exists: append
```

### 2. Entity = The Specifics
Defines **what** moves and **where** exactly:
```yaml
# entities/accounts.yml
name: accounts

home:
  table: dbo.Account_v2    # Type-specific field for MSSQL

store:
  path: dimensions/accounts    # Relative to flow base path

schema_overrides:
  AccountId: BIGINT
  Email: NVARCHAR(320)
```

### 3. Type-Specific Fields (Embrace It)
Each store/home type uses natural terminology:
- **SQL**: `table`, `query`, `schema`
- **Files**: `path`, `partition_by`, `compression`
- **APIs**: `endpoint`, `params`, `headers`

**Don't force generic field names.** SQL people think in tables, file people think in paths.

### 4. Support Both Simple and Complex
```yaml
# Simple (no entities) - single file
flows:
  users:
    home:
      type: mssql
      table: dbo.Users
    store:
      type: parquet
      path: data/users

# Complex (with entities) - multi-file
flows/crm_extract/
  ├── flow.yml
  └── entities/
      └── accounts.yml
```

### 5. Clear Inheritance
**Precedence:** Entity > Flow > Coordinator

```yaml
# Coordinator level (global defaults)
defaults:
  batch_size: 10000

# Flow level (flow.yml)
defaults:
  batch_size: 50000
  if_exists: append

# Entity level (entities/accounts.yml)
home:
  batch_size: 200000    # Wins for this entity
```

---

## Roadmap by Phase

## Phase 1: Entity-First Architecture (v0.2.0)

**Goal:** Make entity files first-class, support multi-file pattern

### 1.1 Entity File Support
- [ ] Parse entity files from `flows/{flow_name}/entities/`
- [ ] Merge entity config with flow config
- [ ] Support relative paths in entity configs (relative to flow base)
- [ ] Entity-level schema overrides
- [ ] Backward compatible with current single-file approach

### 1.2 CLI Enhancements
- [ ] `hygge start --flow {name} --entity {name}` - Run single entity
- [ ] `hygge debug --entity {name}` - Debug entity config
- [ ] `hygge list entities --flow {name}` - List entities in flow
- [ ] Better error messages for entity config issues

### 1.3 Documentation & Examples
- [ ] Multi-file entity pattern examples
- [ ] Migration guide (single-file → multi-file)
- [ ] Best practices for entity organization
- [ ] Code generation template (`hygge generate entities`)

### 1.4 Testing
- [ ] Entity file parsing tests
- [ ] Config inheritance tests
- [ ] Entity-specific override tests
- [ ] Integration tests with multi-file pattern

**Success Criteria:**
- Users can organize 50+ entities cleanly
- Entity files are intuitive and self-documenting
- Simple cases (no entities) still work perfectly
- Migration path is clear and non-breaking

---

## Phase 2: State Management (v0.2.x)

**Goal:** Support incremental loads with watermarking

### 2.1 State File Support
- [ ] `.state.yml` per flow (gitignored)
- [ ] Track watermark values per entity
- [ ] `watermark_column` config in entity files
- [ ] Reset command: `hygge reset --flow {name} --entity {name}`

### 2.2 Incremental Load Patterns
```yaml
# entities/events.yml
load_type: incremental
watermark_column: EventDate
watermark_initial: 2024-01-01
```

### 2.3 State Table Option (Future)
- [ ] Optional state table in target database
- [ ] `hygge_metadata.watermarks` table
- [ ] Concurrent-safe updates

**Success Criteria:**
- Users can run incremental loads without manual tracking
- State is persistent across runs
- Easy to reset/debug watermarks

---

## Phase 3: DuckDB Support (v0.2.x)

**Goal:** Local analytical database for development and testing

### 3.1 DuckDB Store
- [ ] `DuckDbStore` implementation
- [ ] Auto-create tables with schema inference (reuse MSSQL logic)
- [ ] Fast local writes for testing

### 3.2 DuckDB Home
- [ ] `DuckDbHome` implementation
- [ ] Query support (SQL interface to parquet)
- [ ] Integration with parquet files

### 3.3 Use Cases
- Local development without cloud resources
- Testing data pipelines locally
- SQL interface to file-based data
- Prototype before deploying to production databases

**Success Criteria:**
- Developers can test full pipelines locally
- No cloud credentials needed for development
- Fast iteration cycles

---

## Phase 4: API Sources (v0.3.0)

**Goal:** Extract data from REST APIs

### 4.1 REST API Home
```yaml
home:
  type: rest_api
  base_url: https://api.example.com/v2/
  endpoint: customers
  auth:
    type: bearer
    token: ${API_TOKEN}
  pagination:
    type: offset
    page_size: 100
```

### 4.2 Features
- [ ] Common auth patterns (Bearer, API key, OAuth)
- [ ] Pagination strategies (offset, cursor, link header)
- [ ] Rate limiting and retries
- [ ] JSON → DataFrame conversion
- [ ] Nested JSON flattening options

### 4.3 Popular APIs
- [ ] Salesforce (SOQL queries, Bulk API)
- [ ] GitHub
- [ ] Stripe
- [ ] Generic REST API support

**Success Criteria:**
- Extract from common SaaS APIs
- Handle pagination automatically
- Respect rate limits gracefully

---

## Phase 5: PostgreSQL Support (v0.3.x)

**Goal:** Full Postgres connectivity

### 5.1 PostgreSQL Home
- [ ] `PostgresHome` with connection pooling
- [ ] Schema.table support
- [ ] Custom queries
- [ ] Entity pattern support

### 5.2 PostgreSQL Store
- [ ] `PostgresStore` with parallel writes
- [ ] Auto-create tables with schema inference
- [ ] `if_exists` policies
- [ ] Optimal batch sizes for Postgres

**Success Criteria:**
- Feature parity with MSSQL support
- Handles Postgres-specific types (JSON, ARRAY, etc.)
- Performance optimized for Postgres

---

## Phase 6: Cloud Storage (v0.4.0)

**Goal:** Write to cloud object storage

### 6.1 S3 Store
```yaml
store:
  type: s3
  bucket: my-data-lake
  path: raw/salesforce/
  partition_by: [year, month, day]
```

### 6.2 Azure Blob Store
```yaml
store:
  type: azure_blob
  container: data-lake
  path: raw/salesforce/
```

### 6.3 GCS Store
```yaml
store:
  type: gcs
  bucket: my-data-lake
  path: raw/salesforce/
```

**Success Criteria:**
- Write parquet to cloud storage
- Efficient multi-part uploads
- Partition support
- Credential management (env vars, IAM roles)

---

## What's NOT in Scope

### ❌ Transformations (Use dbt)
hygge moves data, it doesn't transform it. For transformations:
- Use dbt for SQL transformations
- Use Polars/Pandas in separate scripts
- Use dedicated transformation tools

### ❌ Orchestration (Use Airflow/Dagster/Prefect)
hygge doesn't schedule or orchestrate:
- No DAGs or dependencies between entities
- No scheduling (cron, triggers)
- No retries with backoff (orchestrator handles this)
- No monitoring/alerting

### ❌ Data Quality (Use Great Expectations/Soda)
hygge doesn't validate data:
- No schema validation (beyond type inference)
- No data quality checks
- No anomaly detection

### ❌ CDC / Real-time (Use Debezium/Kafka)
hygge is batch-oriented:
- No change data capture
- No real-time streaming
- No event processing

---

## Design Philosophy

### Comfort Over Complexity
- Simple cases should be effortless (one YAML file, run it)
- Complex cases should be manageable (entity files, inheritance)
- Don't force users to think about things they don't need

### Reliability Over Speed
- Default to `if_exists: fail` (prevent accidents)
- Connection pooling to avoid exhaustion
- Clear error messages
- Retry logic for transient failures

### Convention Over Configuration
- Smart defaults that "just work"
- Auto-create tables with schema inference
- Sensible batch sizes per database type
- Type-appropriate terminology

### Flow Over Force
- Data should move smoothly
- Batching and buffering should be natural
- Progress should be visible but unobtrusive

---

## Success Metrics

### For Solo Developers
- "I extracted 50 Salesforce tables in 10 minutes"
- "I didn't have to write any SQL DDL"
- "The config file is readable and obvious"

### For Small Teams
- "We have 200 entity files, each owned by a different person"
- "We can test individual entities locally with DuckDB"
- "Our data engineers love the simplicity"

### For Data Engineers
- "hygge handles the boring EL, we focus on transformations"
- "It's fast enough for our needs (millions of rows/min)"
- "Connection pooling just works"

---

## Contributing

This roadmap is a living document. As hygge evolves, we'll:
- ✅ Mark completed items
- 🔄 Update priorities based on user feedback
- 📝 Add new ideas as they emerge

**hygge isn't just about moving data - it's about making data movement feel natural, comfortable, and reliable.**

---

*Last updated: October 12, 2025*
