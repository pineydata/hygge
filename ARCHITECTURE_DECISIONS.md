# Architecture Decision Records

## ADR-001: Auto-Create Tables with Clean Architecture

**Date:** October 14, 2025
**Status:** Accepted
**Context:** Initial implementation had architectural coupling, needed clean separation

### Decision

Keep auto-create tables feature but implement with clean architecture (no Home/Store coupling). Use DataFrame schema directly for table creation.

### Context

We initially built automatic table creation with custom schema inference that required Home/Store coupling:
- Sample data from home
- Infer SQL types from Polars types
- Size strings with buffer factors
- Auto-create tables with CCI

**Problem identified:**
```python
# Store needing Home reference - architectural coupling
store.set_home(home)
```

### Options Considered

**Option 1:** Codegen tool (explicit DDL generation)
- Generate → Review → Apply → Load
- Version control schemas
- Full user control

**Option 2:** Polars native `write_database()` (interim approach)
- Built-in inference
- No custom code
- No coupling

**Option 3:** Keep auto-create with clean architecture ← **CHOSEN**
- Convenient and reliable
- No Home/Store coupling
- Uses DataFrame schema directly

### Decision Rationale

1. **Architectural clean separation:**
   Home and Store should be independent. `set_home()` broke this.

2. **User needs:**
   Users want auto-create convenience without architectural complexity.

3. **DataFrame schema is sufficient:**
   Polars DataFrames contain complete type information, no need to sample from Home.

4. **Conservative defaults:**
   Use safe type mappings (NVARCHAR(4000), BIGINT, etc.) that work for most cases.

### Consequences

**Positive:**
- ✅ Clean architecture (no Home/Store coupling)
- ✅ Auto-create tables work automatically
- ✅ Conservative type mapping (safe defaults)
- ✅ Safe index name generation
- ✅ Users don't need to write CREATE TABLE statements

**Negative:**
- ❌ Conservative type sizing (NVARCHAR(4000) for all strings)
- ❌ No custom string sizing heuristics
- ❌ Users may need to ALTER tables for optimization

**Mitigation:**
- Conservative defaults work for 90% of cases
- Users can ALTER tables after creation if needed
- Future: Optional codegen tool for custom schemas

### Implementation

**Removed (architectural coupling):**
- `set_home()` method and Home wiring in `Flow`
- Complex schema inference from Home samples
- Home/Store architectural dependency

**Implemented (clean architecture):**
- Table existence checking via INFORMATION_SCHEMA
- Direct DataFrame schema → SQL type mapping
- Safe index name generation (replaces invalid characters)
- `if_exists` config: fail (default), append, replace
- Conservative type defaults: String→NVARCHAR(4000), Int64→BIGINT, etc.
- 24 comprehensive unit tests

**Kept:**
- Core MSSQL write functionality
- Connection pooling
- Parallel batch writes
- Performance optimizations

### References

- User feedback: "This feels like a shoehorn..." (original coupling approach)
- User feedback: "We are a small team that is not looking to buy another tool. Make this the tool. I don't want to spend my time writing 70, 80, 90 create statements."
- ROADMAP.md: Phase 1 Entity-First Architecture

### Follow-up

- [ ] Move to Phase 1: Entity-First Architecture (next)
- [ ] Consider optional codegen tool for custom schema generation

---

## ADR-002: Choose EL Over ELT Scope

**Date:** October 12, 2025
**Status:** Accepted

### Decision

hygge is an **Extract and Load (EL) toolkit**, not ELT (Extract, Load, Transform).

### Context

During roadmap planning, clarified hygge's boundaries:
- No transformations (use dbt)
- No DAGs (use Airflow/Dagster)
- No data quality checks (use Great Expectations)
- Focus on comfortable data movement

### Rationale

1. **Clear scope prevents feature creep**
2. **Composable with existing tools** (dbt, Airflow)
3. **Solo dev/small team focus** (not enterprise orchestration)
4. **Do one thing well** (move data)

### Consequences

**What hygge DOES:**
- ✅ Extract data from sources (SQL, files, APIs)
- ✅ Load data to destinations (SQL, files, cloud)
- ✅ Connection pooling and performance
- ✅ Entity pattern for bulk operations
- ✅ Incremental loads with watermarks (planned)

**What hygge DOESN'T do:**
- ❌ Transformations (SQL, Python)
- ❌ Orchestration (scheduling, dependencies)
- ❌ Data quality validation
- ❌ Real-time streaming

### Integration Pattern

```
Source → hygge (EL) → dbt (T) → Destination
         ↓
      Orchestrated by Airflow/Dagster
```

---

## ADR-003: Entity Files Over Single YAML

**Date:** October 12, 2025
**Status:** Proposed (Not Implemented)

### Decision

Support multi-file entity pattern for scalable configuration.

### Context

Current single-file approach doesn't scale to 50+ entities:
```yaml
# hygge.yml - One giant file
flows:
  crm_extract:
    home:
      table: dbo.{entity}
    store:
      path: data/{entity}
    entities: [Account, Contact, ..., 50 more ...]
```

### Proposed

```
flows/
  └── crm_extract/
      ├── flow.yml          # Flow-level config
      └── entities/
          ├── accounts.yml   # Per-entity config
          ├── contacts.yml
          └── ...
```

### Benefits

- One file per entity (scales to 100s)
- Entity-specific config natural home
- Version control friendly (easy diffs)
- Clear separation: flow (pipes) vs entity (specifics)

### Status

Designed but not yet implemented. See ROADMAP.md Phase 1.

---

## Future ADRs

- ADR-004: State Management Design (TBD)
- ADR-005: DuckDB Integration Approach (TBD)
- ADR-006: API Source Authentication Patterns (TBD)

---

*Architecture decisions should be revisited as we learn. These are guidelines, not laws.*
