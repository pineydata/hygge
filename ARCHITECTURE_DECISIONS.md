# Architecture Decision Records

## ADR-001: Remove Auto-Create Tables Feature

**Date:** October 12, 2025
**Status:** Accepted
**Context:** Path A (validation) complete, reconsidering architecture

### Decision

Remove auto-magic table creation feature. Use Polars native inference instead. Build explicit codegen tool on future branch.

### Context

We built automatic table creation with custom schema inference:
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

**Option 2:** Polars native `write_database()` ← **CHOSEN (interim)**
- Built-in inference
- No custom code
- No coupling

**Option 3:** Keep auto-create as-is
- Convenient but opaque
- Architectural coupling
- Data engineers want control

### Decision Rationale

1. **Architectural clean separation:**
   Home and Store should be independent. `set_home()` broke this.

2. **Data engineer mindset:**
   Users want explicit control over schemas, not auto-magic.

3. **Simplicity:**
   Polars already does inference. Don't reinvent it.

4. **Future path clear:**
   Codegen tool (Option 1) will come on separate feature branch.

### Consequences

**Positive:**
- ✅ Clean architecture (no coupling)
- ✅ Simpler codebase (~500 lines removed)
- ✅ Clear path forward (codegen)
- ✅ Better aligns with data engineer expectations

**Negative:**
- ❌ Users must create tables manually (for now)
- ❌ No custom string sizing heuristics
- ❌ Time spent on feature that was removed

**Mitigation:**
- Polars `write_database()` handles basic inference
- Codegen tool coming on next feature branch
- Learning experience on architecture decisions

### Implementation

**Removed:**
- `src/hygge/stores/mssql/schema_inference.py` (410 lines)
- Schema inference integration in `MssqlStore`
- `set_home()` method
- Home wiring in `Flow`
- Config fields: `schema_inference`, `schema_overrides`, `if_exists`
- Tests for inference (48+ tests)

**Kept:**
- Core MSSQL write functionality
- Connection pooling
- Parallel batch writes
- Performance optimizations

**Added:**
- `ARCHITECTURE_DECISIONS.md` - This ADR

### References

- User feedback: "This feels like a shoehorn..."
- ROADMAP.md: Phase 1 Entity-First Architecture

### Follow-up

- [ ] Move to Path 2: State Management (next)
- [ ] Consider codegen tool on future branch (optional)

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
