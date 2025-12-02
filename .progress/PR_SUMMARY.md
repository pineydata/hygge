---
title: PR Summary - Entity Lifecycle Refactoring: Complete Architecture Overhaul
---

## Overview

- Introduced `Entity` class to clearly separate flow templates (FlowConfig) from configured instances (Entity), eliminating validation timing issues
- Moved entity expansion to Workspace as single point of truth, removing scattered expansion logic across Coordinator and FlowFactory
- Made FlowConfig validation lenient (structure only), allowing incomplete configs until entity configs are merged
- Simplified Coordinator to pure orchestrator (~130 lines removed), FlowFactory handles flow creation from Entities
- Removed all entity fallback logic from Flow class, replaced with fail-fast validation for clearer error handling
- Extracted shared FlowFactory logic into reusable methods, improved error messages, and strengthened type safety

## Key Changes

### Entity Architecture

- `src/hygge/core/flow/entity.py` (new):
  - Created `Entity` class representing fully configured flow (FlowConfig + EntityConfig merged)
  - Contains: `flow_name`, `base_flow_name`, `entity_name`, merged `flow_config`, original `entity_config`
  - Clear separation: FlowConfig = template, Entity = configured instance

- `src/hygge/core/flow/config.py`:
  - Made validation lenient: validates structure only, not completeness
  - Allows incomplete store configs (e.g., missing `key_columns` for Open Mirroring) until entity configs merge
  - Full validation happens when Entity is created, not at FlowConfig creation

### Workspace Entity Expansion

- `src/hygge/core/workspace.py`:
  - `WorkspaceConfig` now uses `List[Entity]` instead of `Dict[str, FlowConfig]`
  - `prepare()` expands entities: FlowConfig + EntityConfig → Entity (single point of expansion)
  - Handles entity config merging: home/store paths, run_type, watermark, journal overrides
  - Returns fully configured entities ready for execution

### FlowFactory Simplification

- `src/hygge/core/flow/factory.py`:
  - Added `from_entity()` method: receives Entity (already merged), creates Home/Store and Flow
  - Removed all config merging logic (Entity already has merged config)
  - Extracted shared logic into private methods: `_create_home_instance()`, `_create_store_instance()`, `_get_or_create_journal()`, `_build_flow_options()`, `_extract_run_type()` (reduces duplication)
  - `from_config()` always sets `entity_name=flow_name` for non-entity flows (no more `None`)
  - Improved error messages: specific validation errors with context (e.g., "home config is missing or invalid. Expected dict with 'type' field, got {type}")
  - Clean separation: FlowFactory creates flows, doesn't merge configs

### Coordinator Orchestration

- `src/hygge/core/coordinator.py`:
  - Rewrote `_create_flows()` to iterate over `config.entities` instead of `config.flows`
  - Removed ~130 lines of entity expansion logic (moved to Workspace)
  - Pure orchestrator: loops through entities, creates flows via FlowFactory
  - Replaced fallback `base_flow = flow.base_flow_name or flow.name` with fail-fast validation

### Flow Cleanup

- `src/hygge/core/flow/flow.py`:
  - Removed `_resolve_entity_name()` method (fragile name parsing)
  - Removed all `or self.base_flow_name` fallbacks from watermark and journal tracking
  - Added fail-fast validation: raises `FlowError` if `entity_name` is None (enforces FlowFactory usage)
  - Entity metadata only for journal tracking, no entity knowledge in Flow core

### CLI Entity Support

- `src/hygge/cli.py`:
  - Updated `debug` command to work with `config.entities`, groups by base flow name
  - Updated `go` command to apply run_type overrides using entity-based flow lookup

### Tests

- Updated all tests to work with Entity pattern:
  - Workspace tests: Use `config.entities` instead of `config.flows`
  - Coordinator tests: Create Entity objects, use entity-based flow creation
  - FlowFactory tests: Test `from_entity()` method with Entity objects
  - Flow tests: All Flow instances now provide `entity_name` and `base_flow_name` (removed fallback, enforces pattern)
  - CLI tests: Updated for entity-based configuration output
- All 610+ tests passing

## Testing

- All tests passing: `pytest` (620+ tests collected, all passing)
- No breaking changes to public APIs - changes are internal architecture improvements
- Backward compatible: existing YAML configs work unchanged
- All test Flow instances updated to provide `entity_name` (enforces correct pattern)
