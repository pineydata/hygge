# Table Stakes: Data Engineering Capabilities

> What capabilities become essential for midmarket regional orgs after core cloud platforms and Salesforce?

---

## Current State

hygge has the movement foundation:

- ✅ Polars DataFrames for efficient data movement
- ✅ Incremental processing (watermarks)
- ✅ Error handling and retries
- ✅ Connection pooling
- ✅ Basic schema validation (watermark columns, MSSQL append mode)

**What's Missing:** The "engineering" part of data engineering - schema management, data quality, validation.

---

## Tier 1: Essential Capabilities (Table Stakes)

### 1. Schema Drift Detection & Handling

**Why:** Sources change. Columns get added, removed, renamed. Types change. This is what breaks pipelines in production.

**Current State:**

- MSSQL store validates for append mode (checks NOT NULL columns)
- Watermark validates its columns exist
- But no general schema drift detection

**What's Needed:**

- **Schema snapshot** - Capture schema at first run, store it
- **Schema comparison** - Compare current schema to expected
- **Drift detection** - Identify: new columns, missing columns, type changes, nullable changes
- **Drift handling** - Options: warn, fail, auto-adapt (add nullable columns)

**Configuration:**

```yaml
# In flow config
schema:
  mode: strict  # fail on drift | warn | adapt
  expected_schema_path: schemas/users.json  # optional: explicit schema
```

**Priority:** High - this is what breaks pipelines. Must have.

---

### 2. Schema Evolution Support

**Why:** Schemas change over time. Need to handle: adding columns, removing columns, type changes, nullable changes.

**Current State:**

- MSSQL store can auto-add nullable columns (warns)
- But no general schema evolution strategy

**What's Needed:**

- **Evolution policies** - How to handle each type of change
- **Backward compatibility** - Can old code read new data?
- **Migration support** - Transform data during schema changes

**Configuration:**

```yaml
# In flow config
schema_evolution:
  add_columns: auto  # auto-add nullable columns
  remove_columns: warn  # warn but continue
  type_changes: fail  # fail on incompatible types
  nullable_changes: warn  # warn if nullable -> not null
```

**Priority:** High - needed for long-running pipelines, works with drift detection.

---

## Tier 2: Data Quality & Validation

### Data Contracts & Quality Checks (Combined)

**Why:** Define what data should look like, then validate it. Contracts + quality checks work together to ensure data meets requirements.

**What's Needed:**

**Contracts:**

- Schema validation (columns, types, nullable)
- Constraints (min/max values, regex patterns, enums)
- Business rules (e.g., "email must be valid", "age must be 0-150")

**Quality Checks:**

- Completeness (% of non-null values per column)
- Uniqueness (check for duplicate keys)
- Freshness (is data current? timestamp checks)
- Volume (expected row counts - warn if too low/high)

**Configuration:**

```yaml
# In flow config
validation:
  contract: contracts/users.yml  # optional: explicit contract
  mode: strict  # fail | warn | quarantine

  quality:
    checks:
      - type: completeness
        column: email
        threshold: 0.95
      - type: uniqueness
        columns: [id]
      - type: freshness
        column: updated_at
        max_age_hours: 24
      - type: volume
        min_rows: 1000
        max_rows: 1000000
```

**Priority:** Medium - important for some orgs, optional for others.

**Note:** Contracts = "what data should look like". Quality = "does data meet standards". Together they provide comprehensive validation.

---

## Core Homes & Stores (Must Have)

Beyond capabilities, these sources/destinations become expected:

1. **PostgreSQL** - Most common database after MSSQL
2. **MySQL** - Very common in midmarket
3. **CSV** - Still the most common file format
4. **JSON** - Very common for APIs and logs

**Note:** Additional sources (Snowflake, HubSpot, Stripe, etc.) move to "over the horizon" with the adapter system - build as demand emerges.

---

## Implementation Strategy

### Phase 1: Schema Management (Tier 1 - High Priority)

**Focus:** Schema drift detection and evolution support.

**Implementation:**

1. **Schema tracking** - Capture and store schema on first run
2. **Drift detection** - Compare current schema to expected, identify changes
3. **Evolution policies** - Handle add/remove columns, type changes, nullable changes
4. **Drift handling** - Warn, fail, or auto-adapt based on config

**Where:** In Flow or Store (capture on first write)

**Pattern:**

```python
class SchemaManager:
    def capture_schema(self, df: pl.DataFrame) -> dict:
        """Capture schema snapshot."""

    def detect_drift(self, current: dict, expected: dict) -> DriftReport:
        """Detect schema changes."""

    def apply_evolution_policy(self, drift: DriftReport, policy: dict):
        """Apply evolution policy to handle drift."""
```

**Why first:** Exercises Flow/Store interfaces, stabilizes API, provides immediate value.

---

### Phase 2: Core Homes & Stores

**File Formats (Core hygge):**

- CSV Home & Store
- JSON Home & Store

**Databases (Follow MSSQL pattern):**

- PostgreSQL Home & Store
- MySQL Home & Store

**Priority:** Build as user requests emerge. These follow established patterns.

**Why second:** Essential sources/destinations that users need after cloud platforms and Salesforce.

---

### Phase 3: Data Validation (Tier 2 - Medium Priority)

**Focus:** Data contracts and quality checks (combined).

**Implementation:**

1. **Contract definition** - YAML format for contracts
2. **Schema validation** - Validate against contract schema
3. **Quality checks** - Completeness, uniqueness, freshness, volume
4. **Violation handling** - Fail, warn, or quarantine bad rows

**Where:** In Flow (validate after reading, before writing)

**Pattern:**

```python
class DataValidator:
    def validate_contract(self, df: pl.DataFrame, contract: dict) -> ValidationResult:
        """Validate data against contract."""

    def check_quality(self, df: pl.DataFrame, checks: list) -> QualityReport:
        """Run quality checks."""

    def handle_violations(self, result: ValidationResult, mode: str):
        """Handle violations based on mode."""
```

**Why third:** Builds on schema management, provides data quality assurance.

---

### Over the Horizon: Additional Sources

**With adapter system (when ready):**

- Snowflake (data warehouse)
- HubSpot (CRM/Marketing)
- Stripe (payments)
- Shopify (e-commerce)
- Additional databases/sources

**Priority:** Build with adapter system based on community demand.

---

## For hygge's Philosophy

**Comfort over complexity:**

- Schema drift should "just work" with smart defaults
- Contracts should be simple to define (YAML)
- Quality checks should be optional, not required

**Reliability over speed:**

- Fail fast on schema drift (strict mode)
- Validate contracts before writing
- Log everything for debugging

**Natural flow:**

- Schema tracking happens automatically
- Contracts are defined in YAML alongside flows
- Quality checks are declarative, not programmatic

**Convention over configuration:**

- Smart defaults: warn on drift, don't fail
- Minimal config for common cases
- Clear error messages when things go wrong

---

## API Stability Connection

These capabilities relate to API stability:

**What needs to be stable:**

- Flow interface (where validation happens)
- Store interface (where schema tracking happens)
- Config structure (how contracts/quality are defined)

**Sequence:**

1. Stabilize Flow/Store interfaces
2. Add schema tracking
3. Add contract validation
4. Add quality checks

Building these capabilities will help stabilize the API by exercising the interfaces and revealing what needs to be public vs. internal.

---

## Recommendation

### Phase 1: Schema Management (Tier 1)

1. Most common cause of pipeline breaks
2. Exercises Flow/Store interfaces, stabilizes API
3. Immediate value
4. Foundation for other features

### Phase 2: Core Homes/Stores

- PostgreSQL, MySQL, CSV, JSON
- Essential sources/destinations after cloud platforms
- Follow established patterns (MSSQL model)

### Phase 3: Data Validation (Tier 2)

1. Contracts + quality checks combined
2. Builds on schema management
3. Provides comprehensive data validation
4. Can start simple, expand over time

---

## The Real Question

**What do your users actually ask for?**

After cloud platforms + Salesforce, the next pain points will be:

- "My pipeline broke because a column was added" → Schema management (drift + evolution)
- "Bad data got into my destination" → Data validation (contracts + quality)
- "Can hygge read from PostgreSQL?" → Core homes/stores

**My bet:** Schema management becomes table stakes immediately. Data validation is important but secondary. Core homes/stores build as demand emerges.

Build schema management first. It exercises the API, stabilizes interfaces, provides immediate value, and is a foundation for everything else.

**Note:** hygge is focused on movement, not lineage or observability. Those are separate concerns better handled by external tools.
