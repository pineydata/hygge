# Entity Configuration Lifecycle

## Problem

The system lacks a clear model of when and where entity configuration happens, leading to validation timing issues and architectural violations.

## Root Cause

**No clear separation between template and instance:**
- FlowConfig is treated as both a template (with entities list) and a complete config (ready for validation)
- Entity configs can override flow-level configs, but validation happens before merging
- Flow has entity knowledge because the system doesn't clearly separate "flow template" from "flow instance configured for entity"

**Entity expansion/merging happens at the wrong time:**
- Validation happens before entity configs are merged with flow configs
- Flow has entity knowledge because expansion isn't clearly defined
- No single point where entity expansion happens (scattered across Workspace, Coordinator, FlowFactory)

**Unclear entity configuration lifecycle:**
- When do entity configs get merged with flow configs?
- Where does entity expansion happen? (Workspace? Coordinator? FlowFactory?)
- What does FlowConfig represent? (Template? Instance? Both?)
- When should validation happen? (Before or after entity config merging?)

## Current State

### FlowConfig Ambiguity
FlowConfig is used in two ways:
1. **As a template** - has entities list, incomplete configs (e.g., store config without `key_columns`)
2. **As a complete config** - validated immediately, assumed to be ready for use

This ambiguity causes:
- Validation errors when FlowConfig is incomplete (waiting for entity configs)
- Flow having entity knowledge (because template/instance aren't separated)

### Entity Expansion Scattered
Entity expansion happens in multiple places:
- **Workspace**: Reads entity yml files, adds to FlowConfig.entities
- **Coordinator**: Loops through entities, calls FlowFactory.from_entity()
- **FlowFactory**: Merges entity configs with flow configs

No single clear point where "FlowConfig + Entity → FlowInstance" happens.

### Validation Timing Issues
Validation happens at FlowConfig creation time (in Workspace), but:
- Entity configs haven't been merged yet
- Store configs might be incomplete (waiting for entity overrides)
- FlowConfig is treated as complete when it's actually a template

## Solution

Define a clear entity configuration lifecycle:

### 1. Clear Separation: Template vs Instance

**FlowConfig = Template**
- Has entities list (from entity yml files or flow.yml)
- May have incomplete configs (waiting for entity-specific overrides)
- Not ready for validation until entity configs are merged

**FlowInstance = FlowConfig + Entity → Fully Configured**
- FlowConfig merged with entity config
- All configs complete (entity-specific overrides applied)
- Ready for validation and execution

### 2. Single Point of Entity Expansion

**Entity expansion happens in Workspace or Coordinator:**
- Workspace reads FlowConfig (template with entities list)
- For each entity: FlowConfig + Entity → FlowInstance
- FlowInstance is fully configured and validated

**FlowFactory creates FlowInstance:**
- Receives FlowConfig + Entity (already merged)
- Creates Home/Store with entity-specific configs
- Creates Flow with entity metadata (for journal tracking only)

### 3. Validation at the Right Time

**Validation happens after entity config merging:**
- FlowConfig (template) validation: Only validate structure, not completeness
- FlowInstance validation: Validate completeness (all required fields present)
- Store validation: After entity configs merged, at store creation time

## Proposed Architecture

### Phase 1: Entity Expansion in Workspace

**Workspace.prepare() expands entities:**
```python
# Workspace reads FlowConfig (template)
flow_config = FlowConfig(...)  # Has entities list

# Expand entities: FlowConfig + Entity → FlowInstance
flow_instances = []
for entity in flow_config.entities:
    # Merge entity config with flow config
    merged_config = merge_entity_config(flow_config, entity)
    # Create FlowInstance (fully configured)
    flow_instance = FlowInstance(merged_config, entity)
    flow_instances.append(flow_instance)

# Return list of FlowInstances (not FlowConfigs)
return flow_instances
```

**Benefits:**
- Clear separation: FlowConfig = template, FlowInstance = configured
- Single point of expansion: Workspace handles it
- Validation happens on FlowInstance (after merging)

### Phase 2: FlowFactory Creates Flow from FlowInstance

**FlowFactory receives FlowInstance:**
```python
# FlowInstance already has entity config merged
flow_instance = FlowInstance(flow_config, entity)

# FlowFactory creates Flow with entity-specific Home/Store
flow = FlowFactory.from_instance(flow_instance)
```

**Benefits:**
- FlowFactory doesn't need to merge configs (already done)
- Flow receives entity metadata (for journal tracking only)
- Flow doesn't need entity knowledge (just metadata)

## Related Issues

This root problem manifests as two specific symptoms:

1. **[store-config-entity-inference.md](store-config-entity-inference.md)**: Validation happens too early (before entity configs merged)
2. **[flow-entity-separation.md](flow-entity-separation.md)**: Flow has entity knowledge (because template/instance aren't separated)

## Implementation Strategy

### Option A: Workspace Expands Entities (Recommended)
- Workspace.prepare() returns FlowInstances (not FlowConfigs)
- Coordinator receives fully configured FlowInstances
- FlowFactory creates Flow from FlowInstance
- **Pros**: Clear separation, single expansion point
- **Cons**: Larger refactor, changes Workspace API

### Option B: Coordinator Expands Entities (Incremental)
- Keep Workspace.prepare() returning FlowConfigs
- Coordinator expands entities before creating Flows
- FlowFactory merges entity configs (current behavior)
- **Pros**: Smaller changes, incremental improvement
- **Cons**: Expansion still scattered, FlowFactory still merges

### Option C: FlowFactory Expands Entities (Current)
- Keep current architecture
- Fix validation timing (validate after merging)
- Remove entity knowledge from Flow
- **Pros**: Minimal changes
- **Cons**: Doesn't solve root problem, just treats symptoms

## Recommendation

**Start with Option C** (treat symptoms), then **move to Option A** (fix root cause):
1. Fix validation timing (store-config issue)
2. Remove entity knowledge from Flow (flow-entity issue)
3. Then refactor to clear template/instance separation (this issue)

This allows incremental progress while working toward the proper architecture.

## Questions to Resolve

1. **Where should entity expansion happen?**
   - Workspace (recommended) - single point, clear separation
   - Coordinator - incremental improvement
   - FlowFactory - current, but not ideal

2. **What should FlowConfig represent?**
   - Template only (recommended) - has entities, may be incomplete
   - Both template and instance - current, causes ambiguity

3. **When should validation happen?**
   - After entity config merging (recommended)
   - Before merging - current, causes issues

4. **Should we introduce FlowInstance class?**
   - Yes (recommended) - clear separation
   - No - use FlowConfig for both (current, ambiguous)
