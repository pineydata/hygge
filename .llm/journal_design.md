# Hygge Journal Architecture

## Overview

The **Hygge Journal** provides structured, durable tracking of flow execution metadata and watermarks for incremental data movement. It exists as a **first-class component** within the Hygge framework—conceptually parallel to data `Home` and `Store`, but focused on metadata persistence rather than payload persistence.

The journal enables Hygge to support:

- Reliable resumption and incremental loads
- Auditing of flow execution
- Debugging and monitoring flow history

The design aligns with Hygge's philosophy: **simple, comfortable, and extensible**.

---

## Goals

1. **Record**: Persist structured metadata about flow runs, including start/completion status, counts, durations, and high-watermarks.
2. **Query**: Retrieve last successful runs or current high-watermarks for incremental extracts (watermarks tracked even on `full_drop` runs).
3. **Recover**: Allow restarts or resumptions from a known journal state.
4. **Extend**: Support local or centralized storage backends without changing flow logic.

---

## Guiding Principles

| Principle            | Description                                                                                                                           |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **Independence**     | Journal is a separate abstraction from Store. It is its own class, instantiated per flow (home+store combo, not per entity).          |
| **Composition**       | Journal implementations reuse Store backends internally via composition for persistence, but Journal exposes a metadata-focused API.   |
| **Configurability**   | Users can define where the journal lives — locally with the store, at the home, or in a central database/blob.                         |
| **Simplicity First** | The default behavior "just works" without user configuration.                                                                          |
| **Comfort Over Complexity** | Start simple for solo/small teams. Multi-coordinator support deferred to future.                                                     |
| **Convention Over Configuration** | Config classes live alongside implementation classes, matching the stores pattern.                                                     |

---

## Position in the System

### Key Concepts

- **Coordinator**: Manages discovery and execution of multiple flows. Instantiates each flow's journal based on configuration via `create_journal()` factory.
- **Flow**: Defines movement between a `Home` and `Store`, and interacts with its journal for progress tracking.
- **Journal**: Persists metadata about flow runs, not data payloads. Lives independently but can reuse Store backends internally via composition.

### Conceptual Relationships

```python
Coordinator
   ├── Flow A ── Home ──► Store
   │               │
   │               └──► Journal (metadata)
   │                       (shared across all entities in Flow A)
   │
   └── Flow B ── Home ──► Store
                   │
                   └──► Journal (metadata)
```

Each flow (home+store combo) owns one journal instance, shared across all entities in that flow. Journal may point to the same or different physical backend depending on configuration.

---

## Architecture: Composition Over Inheritance

### Design Decision

Journal is **not a Store subclass**. Instead:

- **Journal** is a separate abstract base class with a metadata-focused API
- **Journal implementations** compose Store instances internally for backend persistence
- This keeps concerns separated: Journal defines the metadata API, and Store implementations handle backend operations

### Example: SqliteJournal

```python
class SqliteJournal(Journal):
    def __init__(self, name: str, config: SqliteJournalConfig, ...):
        super().__init__(name, config, ...)
        # Compose SqliteStore instances for backend persistence
        self._events_store = SqliteStore(...)  # For flow_events table
        self._watermarks_store = SqliteStore(...)  # For flow_watermarks table

    async def record_event(self, event: FlowEvent) -> None:
        # Convert FlowEvent to DataFrame
        df = pl.DataFrame([event.to_dict()])
        # Use SqliteStore's _save() for persistence
        await self._events_store._save(df)

    async def get_last_run(self, ...) -> Optional[FlowEvent]:
        # Use sqlite3 directly for parameterized queries
        # (SqliteStore is write-focused, doesn't offer query API)
        ...
```

**Benefits:**

- Clear separation: Journal API vs Store backend operations
- Reuse: Journal implementations leverage existing Store infrastructure
- Flexibility: Journal can query backends directly when needed (e.g., parameterized SQL queries)
- No coupling: Journal doesn't inherit Store's buffering/write semantics

---

## File Organization

Journals follow the same pattern as stores:

```plaintext
src/hygge/
├── core/
│   ├── journal.py              # Abstract Journal base class
│   ├── journal_config.py       # Abstract JournalConfig base class
│   ├── journal_factory.py      # Factory function for creating journals
│   └── flow_event.py           # FlowEvent Pydantic model
└── journals/
    └── sqlite/
        ├── __init__.py
        └── journal.py          # SqliteJournal + SqliteJournalConfig
```

**Convention**: Config classes live alongside their implementation classes (matching stores pattern).

---

## Responsibilities

### Journal Core Responsibilities

| Capability                        | Description                                                                        |
| --------------------------------- | ---------------------------------------------------------------------------------- |
| **Record Events**                 | Append structured events for flow lifecycle (started, completed, failed, skipped). |
| **Maintain High-Watermarks**      | Track extraction cutoffs for incremental loads.                                    |
| **Support Query & Introspection** | Provide access to last successful run, row counts, duration, etc.                  |
| **Ensure Durability**             | Persist state safely to a backend that can survive coordinator or host restarts.   |

### Coordinator Responsibilities

- Instantiate journal from flow configuration via `create_journal()` factory
- Pass journal instance to Flow constructor
- Emit structured run-level events (start, success, failure) via Flow

---

## Configuration Design

A journal is defined in the same YAML-driven configuration style as other Hygge components.

```yaml
flows:
  users_to_parquet:
    home:
      type: parquet
      path: data/source
    store:
      type: parquet
      path: data/destination
    journal:
      location: store        # default: colocated with store
      # If store is parquet → SQLite file at data/destination/.hygge_journal.db
      # If store is mssql → MSSQL table in same database
```

### Supported Options (V1)

- `location`: `store` | `home` | `custom` (default: `store`)
- `type`: `sqlite` | `parquet` | `mssql` (optional, inferred from location)
- `path`: Optional override path (required if `location: custom`)

### Smart Defaults

- If `location: store` and `store.type == "parquet"` → SQLite file at `{store.path}/.hygge_journal.db`
- If `location: store` and `store.type == "mssql"` → MSSQL table `dbo._hygge_journal` (future)
- If `location: home` → Uses home's backend type (similar logic)
- If `location: custom` → Requires explicit `type` and `path`/`connection`

### Journal is Optional

If `journal:` is not specified in flow config, journal is disabled. No events are recorded.

---

## Registry Pattern

Both `Journal` and `JournalConfig` use registry patterns for dynamic instantiation:

### Journal Registry

```python
class Journal(ABC):
    _registry: Dict[str, Type["Journal"]] = {}

    def __init_subclass__(cls, journal_type: str = None):
        if journal_type:
            cls._registry[journal_type] = cls

    @classmethod
    def create(cls, name: str, config: Any, flow_name: Optional[str] = None) -> "Journal":
        journal_type = getattr(config, "type", None)
        journal_class = cls._registry[journal_type]
        return journal_class(name, config, flow_name)
```

### JournalConfig Registry

```python
class JournalConfig(ABC):
    _registry: Dict[str, Type["JournalConfig"]] = {}

    def __init_subclass__(cls, config_type: str = None):
        if config_type:
            cls._registry[config_type] = cls

    @classmethod
    def create(cls, data: Dict[str, Any]) -> "JournalConfig":
        config_type = data.get("type", "sqlite")
        return cls._registry[config_type](**data)
```

### Implementation Pattern

```python
# journals/sqlite/journal.py
class SqliteJournalConfig(BaseModel, JournalConfig, config_type="sqlite"):
    type: str = "sqlite"
    path: str
    table: str = "flow_events"

class SqliteJournal(Journal, journal_type="sqlite"):
    def __init__(self, name: str, config: SqliteJournalConfig, ...):
        ...
```

---

## Backend Types

### SQLite Journal (Default for File-Based Stores)

- Uses Polars native SQLite support (`df.write_database()`, `pl.read_database()`)
- Composes `SqliteStore` instances internally for event and watermark persistence
- Uses `sqlite3` directly for parameterized queries (SQL injection protection)
- Single file database (`.hygge_journal.db`)
- Simple, file-based (similar to Parquet Store pattern)
- No connection pooling needed
- Wraps blocking Polars calls in `asyncio.to_thread()` if needed

**Implementation Details:**

- `SqliteJournal` composes two `SqliteStore` instances:
  - `_events_store`: Writes to `flow_events` table
  - `_watermarks_store`: Writes to `flow_watermarks` table
- Query methods use `sqlite3` directly for parameterized queries
- Event recording converts `FlowEvent` to `DataFrame` and uses `SqliteStore._save()`

### Parquet Journal (Future)

- Events stored as Parquet files
- Queried with Polars
- Analytics-friendly (time-series queries)
- Scales well with incremental files
- Natural fit for Hygge's Polars architecture

### MSSQL Journal (Future)

- Composes `MssqlStore` instance internally
- Stores events in MSSQL table
- Matches store's backend when `location: store` and store is MSSQL

---

## Class Design

### Journal (Abstract)

Separate abstraction from Store, focused on metadata persistence.

**Primary Responsibilities:**

- Append structured records (e.g., flow run, watermark update)
- Query by flow or entity name
- Support async-safe operations
- Provide schema evolution compatibility for metadata versioning

**Interface Methods:**

- `record_event(event: FlowEvent) -> None`
- `get_last_run(flow_name: str, entity: Optional[str] = None) -> Optional[FlowEvent]`
- `get_watermark(flow_name: str, entity: str) -> Optional[Dict[str, Any]]`
- `update_watermark(flow_name: str, entity: str, watermark: Dict[str, Any]) -> None`
- `flush() -> None` (for batched writes, default no-op)

### SqliteJournal

- Inherits from `Journal` (abstract base)
- Composes `SqliteStore` instances for backend persistence
- Implements Journal interface with SQLite-specific query logic
- Simple file-based implementation

### FlowEvent (Pydantic Model)

```python
class FlowEvent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    flow_name: str
    entity: Optional[str] = None
    event_type: Literal["start", "complete", "fail", "skip"]
    row_count: Optional[int] = None
    duration: Optional[float] = None
    watermark: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
```

---

## Data Model

Each journal entry represents a flow event:

| Field        | Description                                       |
| ------------ | ------------------------------------------------- |
| `id`         | Unique event identifier (UUID)                    |
| `timestamp`  | UTC event time                                    |
| `flow_name`  | Flow identifier                                   |
| `entity`     | Entity/table/end-point name (optional)            |
| `event_type` | start, complete, fail, skip                       |
| `row_count`  | Optional metric                                   |
| `duration`   | Optional metric (seconds)                          |
| `watermark`  | Optional JSON object storing high-watermark(s)    |
| `metadata`   | Free-form key/value JSON for extensibility        |

**Storage:**

- `flow_events` table: All flow events (start, complete, fail, skip)
- `flow_watermarks` table: Current high-watermarks per flow/entity

This schema should evolve compatibly — additional fields can be added as needed without breaking existing journals.

---

## Integration with `full_drop`

The `full_drop` flag affects data strategy, not journal behavior:

- `full_drop: true` → Drop/recreate data tables, but **still record journal events**
- **Watermarks are tracked on full_drop runs** - because the next run could be incremental
- Journal records all runs regardless of data strategy
- Watermarks persist across full reloads (critical for incremental loads after full reloads)

Example:

```python
# In Flow.start():
if self.config.full_drop:
    # Clear data store (existing behavior)
    await self.store.prepare_full_drop()

    # But STILL record the run and maintain watermarks
    # The watermark from this full_drop run enables the next incremental run
    await self.journal.record_event(FlowEvent(
        event_type="start",
        flow_name=self.name,
        metadata={"full_drop": True}
    ))
```

---

## Factory Function

The `create_journal()` factory function handles smart defaults and journal instantiation:

```python
def create_journal(
    flow_name: str,
    journal_config: Optional[Dict[str, Any]],
    store_config: Optional[StoreConfig] = None,
    home_config: Optional[Any] = None,
) -> Optional[Journal]:
```

**Responsibilities:**

1. Return `None` if `journal_config` is `None` (journal disabled)
2. Resolve backend type and path based on `location`:
   - `location: store` → Infer from `store_config`
   - `location: home` → Infer from `home_config`
   - `location: custom` → Require explicit `type` and `path`
3. Create appropriate `JournalConfig` instance (e.g., `SqliteJournalConfig`)
4. Use `Journal.create()` registry pattern to instantiate journal

---

## Implementation Phases

### Phase 1: Minimal Viable Journal ✅

1. ✅ `SqliteStore` implementation (uses Polars native SQLite)
2. ✅ `Journal` abstraction (separate from Store)
3. ✅ `SqliteJournal` implementation (composes SqliteStore)
4. ✅ `FlowEvent` Pydantic model
5. ✅ Integration with Flow to record start/complete/fail
6. ✅ Query last run for debugging

### Phase 2: Incremental Support

1. Watermark storage/retrieval ✅
2. Integration with Home for incremental queries
3. Track watermarks on full_drop runs ✅ (enables next incremental run)

### Phase 3: Additional Backends

1. `ParquetJournal` implementation
2. `MssqlJournal` implementation (if needed)

### Phase 4: Polish

1. CLI query tools (`hygge journal show`)
2. Retention policies (optional cleanup)
3. Multi-coordinator support (if needed)

---

## Concurrency Considerations

**V1 (Single Coordinator):**

- SQLite: Single-writer (acceptable for solo/small team deployments)
- Parquet: File-based, atomic writes via temp files
- MSSQL: Uses connection pool (handled by Store infrastructure)

**Future (Multi-Coordinator):**

- Atomic appends: Each event written atomically
- Idempotency: Use unique event IDs to prevent duplicates
- Backend-specific constraints:
  - SQLite: Single-writer limitation
  - Postgres/Delta: Concurrent writers supported
  - JSONL/Parquet: Use atomic rename or batch upload

---

## Examples

### Minimal Configuration (Defaults)

```yaml
flows:
  users_to_parquet:
    home:
      type: parquet
      path: data/source
    store:
      type: parquet
      path: data/destination
    # Journal defaults to SQLite at data/destination/.hygge_journal.db
    journal:
      location: store
```

### Explicit Backend

```yaml
flows:
  users_to_mssql:
    home:
      type: mssql
      connection: source_db
      table: dbo.users
    store:
      type: mssql
      connection: target_db
      table: dbo.users
    journal:
      location: store
      # Journal uses MSSQL table: target_db.dbo._hygge_journal (future)
```

### Parquet Journal

```yaml
flows:
  users_to_parquet:
    home:
      type: parquet
      path: data/source
    store:
      type: parquet
      path: data/destination
    journal:
      type: parquet
      path: data/metadata/journal
      # Events stored as Parquet files, queried with Polars (future)
```

### Custom Location

```yaml
flows:
  users_to_parquet:
    home:
      type: parquet
      path: data/source
    store:
      type: parquet
      path: data/destination
    journal:
      location: custom
      type: sqlite
      path: /shared/metadata/journal.db
      # Centralized journal location
```

---

## Future Directions

| Feature                        | Description                                                                              |
| ------------------------------ | ---------------------------------------------------------------------------------------- |
| **Centralized Journal Service** | Multiple coordinators append to a shared database or Delta table for unified monitoring. |
| **Journal Query API**           | CLI or REST API for inspecting historical runs and watermarks.                           |
| **Flow Recovery Hooks**        | Coordinator auto-resumes failed flows based on journal state.                           |
| **Retention Policies**         | Configurable cleanup of old run metadata.                                                |
| **Event Streaming**            | Optional publish-subscribe layer for live monitoring.                                    |

---

## Summary

The Hygge Journal is a lightweight but foundational component that brings observability, reliability, and recoverability to data movement.

By defining it as a **separate abstraction** that composes Store instances internally (rather than inheriting from Store), you achieve:

- Clear separation of concerns: Journal API vs Store backend operations
- Consistent mental model: Config classes alongside implementation classes
- Extensible architecture: Easy to add new journal backends
- Reuse of backend infrastructure: SQLite, Parquet, MSSQL Stores
- Flexibility: Journal can query backends directly when needed

The result: a design that is simple enough for small, local deployments, but architecturally ready to grow into multi-coordinator, centralized scenarios with minimal refactoring.

**Key Design Decisions:**

1. **Journal (not Ledger)** - More hygge, cozy, story-like
2. **Per flow (home+store), not per entity** - One journal instance shared across all entities in a flow
3. **Composition over inheritance** - Journal composes Store instances internally, doesn't inherit from Store
4. **Separate abstraction** - Journal is its own ABC with metadata-focused API
5. **Polars native SQLite** - No aiosqlite dependency, simple
6. **Config alongside implementation** - Matches stores pattern for consistency
7. **Smart defaults** - `location: store` matches store's backend type
8. **Optional** - Disabled by default, enable when needed
9. **full_drop integration** - Records events AND tracks watermarks on full_drop runs (enables next incremental run)
10. **Factory function** - `create_journal()` handles smart defaults and instantiation

---

**Status:** Phase 1 Complete, Phase 2 In Progress
**Version:** v0.4.x
**Branch:** `feature/journal`
