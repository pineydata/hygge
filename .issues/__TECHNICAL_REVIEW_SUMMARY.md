# hygge Technical Review: December 2025

**Review Date:** December 31, 2025
**Reviewer Perspective:** Principal Data Engineer / Product Manager / Designer
**Scale Context:** Midmarket regional org (millions to low billions of rows)

---

## Executive Summary

hygge is a well-designed data movement framework with a clear philosophy and clean architecture. The codebase demonstrates thoughtful design decisions aligned with its "comfort over complexity" philosophy. The framework is **production-ready for midmarket scale** with 793 tests providing good coverage.

**Overall Assessment: APPROVE** ✅

---

## Architecture Overview

### Component Structure

```
hygge/
├── core/                  # Core orchestration
│   ├── coordinator.py     # Orchestrates parallel flow execution
│   ├── workspace.py       # Project discovery and config loading
│   ├── flow/              # Flow package (config, entity, factory, flow)
│   ├── home.py            # Abstract data source interface
│   ├── store.py           # Abstract data destination interface
│   ├── journal.py         # Execution metadata tracking
│   ├── watermark.py       # Incremental load tracking
│   └── polish.py          # Last-mile data transforms
├── homes/                 # Data source implementations
│   ├── parquet/           # Parquet file home
│   └── mssql/             # SQL Server home
├── stores/                # Data destination implementations
│   ├── parquet/           # Parquet file store
│   ├── adls/              # Azure Data Lake store
│   ├── onelake/           # OneLake store
│   ├── openmirroring/     # Microsoft Fabric Open Mirroring
│   ├── mssql/             # SQL Server store
│   └── sqlite/            # SQLite store
├── connections/           # Database connection management
├── messages/              # Logging and progress tracking
└── utility/               # Shared utilities
```

### Data Flow Architecture

```
hygge.yml → Workspace → Coordinator → FlowFactory → Flow(s)
                                            ↓
                               Home → [batches] → Store
                                            ↓
                                       Journal
```

---

## Strengths

### 1. CLI-First Design

**The CLI (`hygge go`) is the primary interface.** Users interact through YAML configs, not code. Internal classes (Flow, FlowFactory, Coordinator) are implementation details, not a public API. This keeps the user experience simple and focused.

### 2. Clear Philosophy Embedded in Code

The "comfort over complexity" philosophy is consistently applied:

- **Convention over configuration**: Parquet is the default type, smart defaults everywhere
- **Clear naming**: Home (source) / Store (destination) / Flow (movement) are intuitive
- **Fail-fast with helpful messages**: Validation errors tell you what's wrong and how to fix it

### 3. Registry Pattern for Extensibility

The Home/Store registry pattern is elegant:

```python
class ParquetStore(Store, store_type="parquet"):
    ...

# Usage: Home.create("name", config) automatically selects the right implementation
```

This makes adding new homes/stores straightforward and discoverable.

### 4. Clean Separation of Concerns

The architecture has clear boundaries:

- **Workspace** handles config discovery and entity expansion (single responsibility)
- **Coordinator** handles flow orchestration and parallelism (pure orchestrator)
- **FlowFactory** handles flow construction and wiring (factory pattern)
- **Flow** handles data movement with producer-consumer pattern
- **Journal** handles execution metadata separately from flow execution

### 5. Entity Pattern for Multi-Table Flows

The Entity concept elegantly handles the common case of 10-200+ tables:

```yaml
flows:
  salesforce:
    entities:
      - Account
      - Contact
      - Opportunity
```

Each entity becomes a separate flow with merged configuration - pragmatic for midmarket needs.

### 6. Producer-Consumer Pattern with Backpressure

The Flow class uses async queues with bounded size for natural backpressure:

```python
queue = asyncio.Queue(maxsize=self.queue_size)  # Default: 10
```

This prevents memory issues with fast producers / slow consumers.

### 7. Exception Hierarchy with Proper Chaining

The exception design supports both precise error handling and debugging:

```python
raise FlowExecutionError(f"Flow failed: {self.name}") from e  # Preserves stack trace
```

Connection errors, read errors, and write errors are distinguished for retry logic.

### 8. Store Interface with Optional Methods

The store interface is well-designed with clear required vs optional methods:

```python
# Required (abstract)
async def _save(self, data, path) -> None

# Optional (default no-op implementations)
def configure_for_run(self, run_type) -> None
async def cleanup_staging(self) -> None
async def reset_retry_sensitive_state(self) -> None
def set_pool(self, pool) -> None
```

---

## Areas for Improvement

### 1. Flow Package Complexity

The `flow/` package has grown to 4 modules with interconnected responsibilities:

```
flow/
├── config.py    # 278 lines - FlowConfig with validation
├── entity.py    # 84 lines - Entity model
├── factory.py   # 564 lines - FlowFactory with creation logic
└── flow.py      # 582 lines - Flow with execution logic
```

**Concern:** `factory.py` at 564 lines is doing a lot - creating instances, injecting dependencies, validating alignment, merging configs. Consider whether some of this belongs elsewhere.

**Recommendation:** Monitor for growth. If factory exceeds 700 lines, consider extracting validation and alignment checking into dedicated helpers.

### 2. Type Safety Gaps

Several places use `Any` where more specific types would help:

```python
def __init__(self, name: str, home: Home, store: Any, ...):  # store should be Store
```

```python
store: Optional[Any] = None,  # In Journal
store_config: Optional[Any] = None,  # Throughout
```

**Impact:** IDE autocomplete is limited, type checking misses potential issues.

**Recommendation:** Add `Store` protocol or use existing base class consistently.

### 3. Configuration Validation Timing

FlowConfig uses "lenient" validation (structure only) because entity configs complete later:

```python
# Don't validate completeness - that happens when FlowInstance is created
```

This deferred validation is pragmatic but could lead to confusing errors if misconfigured.

**Recommendation:** Consider adding a `validate_complete()` method that can be called explicitly when full validation is desired.

### 4. Journal Path Resolution Complexity

The journal supports multiple storage backends with complex path resolution:

```python
def _configure_storage(self, store, store_config, store_path, home_path, home_config):
    if self.config.path:
        self._setup_local_storage(Path(self.config.path))
    elif location == "store":
        if self._store_supports_remote_journal(store_config):
            self._setup_remote_storage(store, store_config)
        else:
            self._setup_local_storage(...)
    elif location == "home":
        ...
```

**Concern:** This handles many scenarios but is getting dense (40+ lines).

**Recommendation:** Consider extracting a `JournalStorageResolver` class if this grows further.

### 5. SQLite Store Test Failures

Six SQLite store tests fail with async event loop errors:

```
RuntimeError: Event loop is closed
```

**Impact:** Test infrastructure issue, not production bug, but failing tests erode confidence.

**Recommendation:** Fix the pytest-asyncio fixture scoping. See [sqlite-store-tests.md](sqlite-store-tests.md).

---

## Design Decisions Worth Preserving

### 1. Polars + PyArrow Commitment

The commitment to Polars is wise:

- Single data representation throughout the pipeline
- Efficient columnar operations
- Good database connectivity via `read_database()`
- Clean API that fits hygge's philosophy

**Preserve:** Don't add pandas as an alternative - keep the stack focused.

### 2. Workspace Pattern

The workspace pattern (hygge.yml + flows/ directory) is Rails-inspired and works well:

```
my-project/
├── hygge.yml           # Project config
└── flows/
    └── my_flow/
        ├── flow.yml    # Flow config
        └── entities/   # Entity configs
```

**Preserve:** This convention-over-configuration approach scales well for midmarket.

### 3. CLI-First Philosophy

The CLI (`hygge go`, `hygge debug`) is the primary interface, with programmatic usage secondary:

```bash
hygge go --flow users --incremental
```

**Preserve:** Keep CLI as the comfortable default, don't over-engineer programmatic APIs.

### 4. Batched Processing with Progress

The consistent batching (default 100K rows) with progress logging feels natural:

```
09:15:32 [users_flow] WROTE 300,000 rows
```

**Preserve:** This cadence (configurable via `row_multiplier`) works well for observability.

---

## Potential Technical Debt

### Low Severity

1. **String-based type checking in some places**
   ```python
   if store_config.type == "open_mirroring":
   ```
   Consider using enums or constants for type strings.

2. **`hasattr()` usage in a few places**
   Most removed, but some remain in edge cases. Complete the transition.

3. **Date placeholders in HYGGE_DONE.md**
   Some entries have `2025-XX-XX` - normalize for consistency.

### Medium Severity

1. **Flow constructor requires entity_name**
   This enforces FlowFactory usage, which is good, but may surprise users wanting simple programmatic flows. Document clearly.

2. **Open Mirroring store at 1300+ lines**
   The spec complexity is real, but the file has refactoring opportunities: duplicate JSON write patterns, monolithic `finish()` method, scattered atomic operation logic. See [openmirroring-refactor.md](openmirroring-refactor.md).

---

## Roadmap Priorities

### Immediate (Fix Before Shipping)

1. **Fix SQLite store tests** - Async event loop handling
2. **Document FlowFactory** - Make canonical creation path clear

### Short-Term (Next Month)

1. **Type hints improvement** - Add Store protocol, reduce `Any` usage
2. **Flow package review** - Monitor factory.py growth

### Long-Term (Quarterly)

1. **Watermark support for other homes** - Extend beyond MSSQL
2. **Schema evolution handling** - How to handle source schema changes
3. **MSSQL stress testing** - When dedicated test infrastructure available

---

## Test Coverage Assessment

- **793 tests collected**
- **774 passed, 6 failed, 13 skipped**
- **Failure rate: 0.76%** (all in SQLite store, infrastructure issue)

**Coverage areas:**
- ✅ Unit tests for all core components
- ✅ Integration tests for parquet-to-parquet at scale
- ✅ Journal, watermark, and polisher thoroughly tested
- ⚠️ SQLite store tests need fixture fixes
- ⏸️ MSSQL stress tests deferred (requires database)

---

## Conclusion

hygge is a well-architected framework that successfully embodies its "comfort over complexity" philosophy. The codebase is clean, the patterns are consistent, and the design decisions are pragmatic for midmarket scale.

**Key Strengths:**
- Clear philosophy consistently applied
- Clean separation of concerns
- Extensible registry pattern
- Production-ready for midmarket volumes
- Good test coverage

**Key Concerns:**
- Minor test infrastructure issue (SQLite)
- Type safety could be improved
- Some modules growing large (factory.py, openmirroring store)

**Verdict:** The framework is ready for production use. The identified issues are minor and don't block deployment. Continue iterating with hygge's philosophy of "progress over perfection."

---

## Active Issues

| Priority | Issue | Status |
|----------|-------|--------|
| 🔴 High | [SQLite Store Tests](sqlite-store-tests.md) | 6 tests failing |
| 🟡 Medium | [FlowFactory Documentation](flowfactory-documentation.md) | Needs clarity |
| 🟢 Low | [Store Type Hints](store-type-hints.md) | Enhancement |
| ⏸️ Deferred | [MSSQL Stress Testing](mssql-stress-testing.md) | Needs infrastructure |
| ⏸️ Deferred | [MSSQL Python Migration](mssql-python-migration.md) | Stay with pyodbc |

---

**Last Updated:** December 31, 2025
**Next Review:** Q1 2026
