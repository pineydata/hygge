# Flow Cleanup Proposal

## Goal
Remove entity logic from Flow's core, keeping only metadata needed for journal tracking.

## Entity Expansion Model

**Entities belong to Home (like pages in a book, or tables in a database)**
- Home defines what entities it has (the source data)
- Entities are part of Home's structure/configuration
- Store receives data from Home's entities

**Flow as a Functor: `Flow(home.entity, store)`**
- Flow orchestrates movement from Home's entities to Store
- Entity is part of Home (home.entity)
- Flow takes (home.entity, store) and produces a FlowInstance
- Output: FlowInstance = (home.entity, store) pair

**Category Theory View:**
- **Flow**: Functor from Category(Home.Entities) → Category(FlowInstances)
- **Input**: Home.Entity (entity from Home with metadata)
- **Output**: FlowInstance = (home.entity, store)
- **Structure preservation**: Orchestration pattern (producer-consumer, batching, handoff) is preserved
- **Entity-specific configuration**: Home and Store are configured per entity based on entity metadata

**FlowConfig = Template with Home that has entities**
- FlowConfig has Home (which contains entities) and Store
- Home's entities come from entity yml files or flow.yml
- Each entity in Home → `Flow(home.entity, store)` → FlowInstance
- Home and Store adjust their configuration based on entity metadata

**Expansion:**
- FlowConfig with Home that has entities → multiple Flow instances (one per entity)
- Each Flow instance = `Flow(home.entity, store)` where entity belongs to Home
- Flow itself (the functor) is the same, but Home/Store are configured per entity

## Current Fallbacks to Remove

Since entity metadata always exists (from entity yml files or flow.yml), all fallback logic should be removed:

### Fallback 1: Watermark Context Preparation (line 409)
**Current:**
```python
entity_identifier = self._resolve_entity_name() or self.base_flow_name
```
**Issue:** Falls back to `base_flow_name` if entity name resolution returns None. Since entity metadata always exists, this fallback is unnecessary.

### Fallback 2: Journal Recording (line 554)
**Current:**
```python
entity=entity or self.base_flow_name,
```
**Issue:** Falls back to `base_flow_name` if entity is None. Since entity metadata always exists, this fallback is unnecessary.

### Fallback 3: Entity Name Resolution (lines 572-580)
**Current:**
```python
def _resolve_entity_name(self) -> Optional[str]:
    """Resolve entity name for journal lookups."""
    if self.entity_name:
        return self.entity_name
    if self.name != self.base_flow_name and self.name.startswith(
        f"{self.base_flow_name}_"
    ):
        return self.name[len(f"{self.base_flow_name}_") :]
    return None
```
**Issues:**
- Fragile name parsing: tries to extract entity name from flow name pattern `{base_flow_name}_{entity_name}`
- Multiple fallback layers: checks `entity_name`, then tries name parsing, then returns None
- Since entity metadata always exists, `entity_name` should always be set, making all fallbacks unnecessary

## Key Changes

### 1. Entity Metadata - Keep but Clarify Purpose

**Current:**
```python
def __init__(
    self,
    name: str,
    home: Home,
    store: Store,
    options: Optional[Dict[str, Any]] = None,
    # ... other params ...
    base_flow_name: Optional[str] = None,  # Entity-related
    entity_name: Optional[str] = None,     # Entity-related
    # ...
):
    self.base_flow_name = base_flow_name or name
    self.entity_name = entity_name
```

**Proposed:**
```python
def __init__(
    self,
    name: str,
    home: Home,
    store: Store,
    options: Optional[Dict[str, Any]] = None,
    # ... other params ...
    # Entity metadata (every flow processes an entity)
    flow_template: Optional[str] = None,  # Renamed from base_flow_name
    entity_name: Optional[str] = None,   # Entity name from entity metadata (always exists)
    # ...
):
    self.name = name
    self.home = home
    self.store = store
    # ... core flow setup ...

    # Entity metadata (for journal tracking - every flow processes an entity)
    # Entity metadata always exists (from entity yml file or flow.yml)
    self.flow_template = flow_template or name  # Renamed for clarity
    self.entity_name = entity_name  # Entity name from entity metadata (always present)
```

### 2. Simplify Entity Name Resolution

**Current:**
```python
def _resolve_entity_name(self) -> Optional[str]:
    """Resolve entity name for journal lookups."""
    if self.entity_name:
        return self.entity_name
    if self.name != self.base_flow_name and self.name.startswith(
        f"{self.base_flow_name}_"
    ):
        return self.name[len(f"{self.base_flow_name}_") :]
    return None
```

**Proposed:**
```python
def _get_entity_name(self) -> Optional[str]:
    """Get entity name for journal tracking. Every flow processes an entity."""
    # Entity name always exists (from entity metadata in yml files or flow.yml)
    # Type is Optional for compatibility, but entity_name is always set at runtime
    return self.entity_name
```

### 3. Rename Entity-Specific Methods to Be Generic

**Current:**
```python
async def _record_entity_run(self, status: str, message: Optional[str] = None):
    """Record entity run in journal (if enabled)."""
    entity = self._resolve_entity_name()
    # ...
    await self.journal.record_entity_run(
        flow=self.base_flow_name,
        entity=entity or self.base_flow_name,
        # ...
    )
```

**Proposed:**
```python
async def _record_run_in_journal(self, status: str, message: Optional[str] = None):
    """Record flow run in journal (if enabled). Every flow processes an entity."""
    if not self.journal:
        return

    # Every flow processes an entity (entity metadata always exists)
    entity_name = self._get_entity_name()
    flow_name = self.flow_template

    # ...
    await self.journal.record_entity_run(
        flow=flow_name,
        entity=entity_name,  # Always has a value (from entity metadata)
        # ...
    )
```

### 4. Simplify Watermark Context Preparation

**Current:**
```python
async def _prepare_incremental_context(self) -> None:
    """Resolve watermark context for incremental runs."""
    # ...
    entity_identifier = self._resolve_entity_name() or self.base_flow_name
    watermark_info = await self.journal.get_watermark(
        self.base_flow_name,
        entity=entity_identifier,
        # ...
    )
```

**Proposed:**
```python
async def _prepare_incremental_context(self) -> None:
    """Resolve watermark context for incremental runs."""
    if self.run_type != "incremental":
        return
    if not self.journal or not self.watermark_config:
        return

    # Every flow processes an entity (entity metadata always exists)
    entity_identifier = self._get_entity_name()

    try:
        watermark_info = await self.journal.get_watermark(
            self.flow_template,
            entity=entity_identifier,
            primary_key=self.watermark_config.get("primary_key"),
            watermark_column=self.watermark_config.get("watermark_column"),
        )
        # ...
```

### 5. Rename Entity-Specific Variables

**Current:**
```python
self.entity_start_time: Optional[datetime] = None
# ...
self.entity_start_time = datetime.now(timezone.utc)
```

**Proposed:**
```python
self.run_start_time: Optional[datetime] = None  # More generic
# ...
self.run_start_time = datetime.now(timezone.utc)
```

## Summary of Changes

1. **Remove all fallback logic** - entity metadata always exists, no fallbacks needed
   - Remove `or self.base_flow_name` fallback in watermark context (line 409)
   - Remove `or self.base_flow_name` fallback in journal recording (line 554)
   - Remove name parsing fallback in `_resolve_entity_name()` (lines 572-580)
2. **Renamed `base_flow_name` → `flow_template`** - clearer that it's the template name
   - Update in `flow.py` (constructor, all references)
   - Update in `factory.py` (all references to `base_flow_name`)
3. **Simplified entity name resolution** - no fragile name parsing, entity name always comes from entity metadata
   - Rename `_resolve_entity_name()` → `_get_entity_name()`
   - Remove all fallback logic (name parsing, None returns)
   - Update all call sites (lines 409, 506 in `flow.py`)
4. **Renamed `_record_entity_run()` → `_record_run_in_journal()`** - every flow processes an entity
   - Update all call sites (lines 185, 197 in `flow.py`)
5. **Renamed `entity_start_time` → `run_start_time`** - more generic
6. **Added clear comments** - every flow processes an entity (entity metadata always exists, from entity yml files or flow.yml)

## Result

- Flow's core execution logic is entity-agnostic (doesn't care about entity details)
- Every Flow processes an entity (entity metadata always exists)
- No fragile name parsing - entity name comes from entity metadata (yml files or flow.yml)
- Flow remains focused on home → store orchestration

## Validation Strategy

To ensure `entity_name` is always set, add validation at Flow creation:

### 1. FlowFactory.from_config() - Should not exist for flows with entities
**Location:** `src/hygge/core/flow/factory.py` line ~164

**Current:**
```python
return FlowCls(
    # ...
    entity_name=None,  # ❌ Should always be set
    # ...
)
```

**Issue:** `from_config()` is called when FlowConfig has no entities list, but every FlowConfig should have entities. This suggests `from_config()` shouldn't be used, or it should validate that entities exist.

**Proposed:**
```python
# FlowConfig should always have entities list
# If entities list is missing, that's a configuration error
if not flow_config.entities or len(flow_config.entities) == 0:
    raise ConfigError(
        f"Flow '{flow_name}' has no entities. "
        "Every flow must have at least one entity (from entity yml files or flow.yml)."
    )

# from_config() should only be used for single-entity flows
# For flows with multiple entities, Coordinator should use from_entity() for each
# Extract entity name from first entity
entity = flow_config.entities[0]
if isinstance(entity, str):
    entity_name = entity
elif isinstance(entity, dict):
    entity_name = entity.get("name")
    if not entity_name:
        raise ConfigError(
            f"Entity in flow '{flow_name}' missing 'name' field"
        )
else:
    raise ConfigError(
        f"Invalid entity format in flow '{flow_name}': {type(entity)}"
    )

return FlowCls(
    # ...
    entity_name=entity_name,  # ✅ Always set from entity metadata
    # ...
)
```

**Note:** Coordinator should be updated to always use `from_entity()` for each entity, even for single-entity flows, to maintain consistency.

### 2. FlowFactory.from_entity() - Already has entity_name
**Location:** `src/hygge/core/flow/factory.py` line ~378

**Current:** Already receives `entity_name` parameter - ✅ Good

**Proposed:** Add validation to ensure it's not None:
```python
if not entity_name:
    raise ConfigError(
        f"Entity name is required for entity flow. "
        f"Flow: {base_flow_name}, Entity config: {entity_config}"
    )
```

### 3. Flow.__init__() - Runtime validation (optional safety check)
**Location:** `src/hygge/core/flow/flow.py` line ~71

**Proposed:** Add optional validation (can be removed later if we're confident):
```python
self.entity_name = entity_name

# Runtime validation (safety check - should never trigger if factories are correct)
if self.entity_name is None:
    raise ValueError(
        f"Flow '{name}' created without entity_name. "
        "Entity metadata is required. This is a configuration error."
    )
```

## Implementation Notes

- `entity_name` type remains `Optional[str]` for now (type system compatibility)
- At runtime, `entity_name` is always set (from entity metadata or flow name)
- Validation added at FlowFactory level to catch configuration errors early
- Optional runtime check in Flow.__init__() as safety net
- All fallback logic removed - code assumes entity_name exists
