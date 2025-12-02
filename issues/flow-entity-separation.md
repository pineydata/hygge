# Flow Entity Separation

> **Symptom of**: [entity-configuration-lifecycle.md](entity-configuration-lifecycle.md)
>
> This issue is a specific symptom of the root problem: FlowConfig is treated as both template and instance, causing Flow to have entity knowledge.

## Problem

Flow currently has entity knowledge embedded in its core, violating Single Responsibility Principle. Flow should orchestrate home → store data movement, but it also:
- Knows about entities (which entities to process)
- Has fallback logic for entity name resolution (fragile name parsing)
- Mixes entity-specific logic with orchestration logic

## Current Issues

### 1. Entity Knowledge in Flow Core
Flow has entity-related fields and logic that don't belong in the orchestrator:
- `base_flow_name` / `entity_name` fields
- `_resolve_entity_name()` with fragile name parsing
- Entity-specific fallback logic throughout

### 2. Fragile Fallback Logic
Three places where fallback logic exists (should be removed):

**Fallback 1: Watermark Context (line 409)**
```python
entity_identifier = self._resolve_entity_name() or self.base_flow_name
```

**Fallback 2: Journal Recording (line 554)**
```python
entity=entity or self.base_flow_name,
```

**Fallback 3: Entity Name Resolution (lines 572-580)**
```python
def _resolve_entity_name(self) -> Optional[str]:
    if self.entity_name:
        return self.entity_name
    if self.name != self.base_flow_name and self.name.startswith(
        f"{self.base_flow_name}_"
    ):
        return self.name[len(f"{self.base_flow_name}_") :]
    return None
```

**Issues:**
- Fragile name parsing: tries to extract entity name from flow name pattern
- Multiple fallback layers
- Entity metadata always exists (from entity yml files or flow.yml), so fallbacks are unnecessary

## Solution

Remove entity logic from Flow's core, keeping only metadata needed for journal tracking.

### Key Principles

1. **Flow = Orchestrator** - Flow orchestrates home → store data movement (producer-consumer, batching, handoff)
2. **Entity Metadata = Configuration** - Entity metadata is passed to Flow for journal tracking only
3. **No Fallbacks** - Entity metadata always exists, no fallback logic needed
4. **Validation at Creation** - Ensure `entity_name` is always set when Flow is created

## Changes Required

### 1. Remove Fallback Logic

**Remove `or self.base_flow_name` fallbacks:**
- Line 409: Watermark context preparation
- Line 554: Journal recording

**Simplify `_resolve_entity_name()`:**
- Remove name parsing logic
- Remove multiple fallback layers
- Rename to `_get_entity_name()` for clarity
- Just return `self.entity_name` directly

### 2. Rename for Clarity

- `base_flow_name` → `flow_template` (clearer purpose)
- `_resolve_entity_name()` → `_get_entity_name()` (simpler, no resolution needed)
- `_record_entity_run()` → `_record_run_in_journal()` (works for all flows)
- `entity_start_time` → `run_start_time` (more generic)

### 3. Add Validation

**FlowFactory.from_config():**
- Validate that FlowConfig has entities list
- Extract entity name from first entity
- Always set `entity_name` (never None)

**FlowFactory.from_entity():**
- Validate that `entity_name` is not None

**Flow.__init__() (optional):**
- Runtime safety check that `entity_name` is not None

### 4. Update All References

- Update `base_flow_name` → `flow_template` in `flow.py` and `factory.py`
- Update all call sites for renamed methods
- Update all references to renamed variables

## Implementation Details

### Files to Modify

1. **src/hygge/core/flow/flow.py**
   - Remove fallback logic (lines 409, 554, 572-580)
   - Rename `base_flow_name` → `flow_template`
   - Rename `_resolve_entity_name()` → `_get_entity_name()`
   - Rename `_record_entity_run()` → `_record_run_in_journal()`
   - Rename `entity_start_time` → `run_start_time`
   - Update all references

2. **src/hygge/core/flow/factory.py**
   - Update `base_flow_name` → `flow_template` in all references
   - Add validation in `from_config()` to ensure entities exist
   - Add validation in `from_entity()` to ensure entity_name is not None
   - Always set `entity_name` (never None)

3. **src/hygge/core/coordinator.py**
   - Update references to `base_flow_name` → `flow_template` (if any)

## Expected Outcome

- Flow's core execution logic is entity-agnostic
- Entity metadata is only used for journal tracking
- No fragile name parsing or fallback logic
- Flow remains focused on home → store orchestration
- All entity configuration handled at FlowFactory/Coordinator level

## Testing

- All existing tests should pass
- Verify entity_name is always set in Flow instances
- Verify no fallback logic remains
- Verify journal tracking still works correctly
