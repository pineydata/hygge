# Flow Creation Extraction: Move Flow Creation Logic to Flow Class

## Problem

The `Coordinator` class still contains ~300 lines of flow creation logic that should be handled by the `Flow` class itself. This includes:

- Flow instance creation from configuration (~135 lines in `_create_flows()`)
- Entity flow creation with config merging (~170 lines in `_create_entity_flow()`)
- Configuration merging logic (entity configs, home/store configs, path merging)
- Helper methods for overrides, journal creation, connection pool injection

This violates separation of concerns - the `Coordinator` should orchestrate flows, not create them. Flows should know how to create themselves from their configuration.

## Current Behavior

Flow creation logic is in `Coordinator`:

- `_create_flows()` - Creates Flow instances from configuration (handles entity vs non-entity flows)
- `_create_entity_flow()` - Creates entity-specific flows with config merging
- `_apply_flow_overrides()` - Applies CLI overrides to flow configs
- `_get_or_create_journal_instance()` - Creates/caches journal instances
- `_inject_store_pool()` - Injects connection pools into stores
- `_validate_run_type_incremental_alignment()` - Validates run_type/store alignment

The Coordinator knows too much about how flows are created, including:
- How to merge entity configs with flow configs
- How to merge home/store configs with entity-specific overrides
- How to handle path merging for entity subdirectories
- How to create Home/Store instances for entities
- How to set up journals, connection pools, and run IDs

## Use Cases

1. **Easier Testing**: Flow creation logic can be tested independently
2. **Better Maintainability**: Changes to flow creation don't affect Coordinator orchestration
3. **Clearer Responsibilities**: Flow class handles flow creation, Coordinator handles orchestration
4. **Future Extensibility**: New flow creation patterns are easier to add
5. **Simpler Coordinator**: Coordinator becomes a pure orchestrator, not a flow factory

## Proposed Solution

Extract flow creation logic to class methods on the `Flow` class. Flows should know how to create themselves from their configuration.

### Flow Class Methods

Add class methods to `Flow` class in `src/hygge/core/flow.py`:

#### `Flow.from_config()`
```python
@classmethod
def from_config(
    cls,
    flow_name: str,
    flow_config: FlowConfig,
    coordinator_run_id: str,
    coordinator_name: str,
    connection_pools: Dict[str, ConnectionPool],
    journal_cache: Dict[str, Journal],
    flow_overrides: Optional[Dict[str, Any]] = None,
) -> "Flow":
    """Create a Flow instance from its configuration.

    Creates Home and Store instances, wires up journals and connection pools,
    and makes everything ready to run.

    Args:
        flow_name: Name of the flow
        flow_config: Flow configuration
        coordinator_run_id: Coordinator run ID
        coordinator_name: Coordinator name
        connection_pools: Available connection pools
        journal_cache: Journal instance cache
        flow_overrides: Optional CLI overrides for flow config

    Returns:
        Flow instance ready to run
    """
```

#### `Flow.from_entity()`
```python
@classmethod
def from_entity(
    cls,
    flow_name: str,
    base_flow_name: str,
    flow_config: FlowConfig,
    entity_name: str,
    entity_config: Union[Dict[str, Any], str],
    coordinator_run_id: str,
    coordinator_name: str,
    connection_pools: Dict[str, ConnectionPool],
    journal_cache: Dict[str, Journal],
) -> "Flow":
    """Create an entity Flow instance from configuration.

    Merges entity configuration with flow configuration, creates Home and Store
    instances with entity-specific paths, and makes everything ready to run.

    Args:
        flow_name: Full flow name (base_flow_name + entity_name)
        base_flow_name: Base flow name
        flow_config: Flow configuration
        entity_name: Entity name
        entity_config: Entity configuration (dict or string)
        coordinator_run_id: Coordinator run ID
        coordinator_name: Coordinator name
        connection_pools: Available connection pools
        journal_cache: Journal instance cache

    Returns:
        Entity Flow instance ready to run
    """
```

### Helper Methods (Private)

Add private helper methods to `Flow` class:

- `_merge_entity_config()` - Merges entity config with flow config
- `_merge_home_config()` - Merges entity home config with flow home config
- `_merge_store_config()` - Merges entity store config with flow store config
- `_apply_overrides()` - Applies CLI overrides to flow config
- `_get_or_create_journal()` - Gets or creates journal instance (may need Coordinator context)
- `_inject_connection_pool()` - Injects connection pool into store/home
- `_validate_run_type_alignment()` - Validates run_type/store incremental alignment

### Coordinator Simplification

Update `Coordinator._create_flows()` to use Flow class methods:

```python
def _create_flows(self) -> None:
    """Create Flow instances from configuration."""
    self.flows = []

    for base_flow_name, flow_config in self.config.flows.items():
        try:
            # Check if flow should be included
            if self.flow_filter and not self._should_include_flow(
                base_flow_name, flow_config
            ):
                continue

            # Apply CLI overrides
            if self.flow_overrides:
                flow_config = Flow._apply_overrides(
                    flow_config, base_flow_name, self.flow_overrides
                )

            # Check if entities are defined
            if flow_config.entities and len(flow_config.entities) > 0:
                # Create one flow per entity
                for entity in flow_config.entities:
                    entity_name = self._extract_entity_name(entity, base_flow_name)
                    entity_flow_name = f"{base_flow_name}_{entity_name}"

                    # Check if entity flow should be included
                    if self.flow_filter and not self._should_include_entity_flow(
                        base_flow_name, entity_flow_name, self.flow_filter
                    ):
                        continue

                    # Create entity flow using Flow class method
                    flow = Flow.from_entity(
                        entity_flow_name,
                        base_flow_name,
                        flow_config,
                        entity_name,
                        entity,
                        self.coordinator_run_id,
                        self.coordinator_name,
                        self.connection_pools,
                        self._journal_cache,
                    )
                    self.flows.append(flow)
            else:
                # Create single flow without entities
                flow = Flow.from_config(
                    base_flow_name,
                    flow_config,
                    self.coordinator_run_id,
                    self.coordinator_name,
                    self.connection_pools,
                    self._journal_cache,
                    self.flow_overrides.get(base_flow_name) if self.flow_overrides else None,
                )
                self.flows.append(flow)

        except Exception as e:
            raise ConfigError(f"Failed to create flow {base_flow_name}: {str(e)}")
```

## Implementation Details

### Configuration Merging

Flow creation involves complex configuration merging:

1. **Entity Config Merging**: Merge entity config with flow config
2. **Home Config Merging**: Merge entity home config with flow home config (special path merging)
3. **Store Config Merging**: Merge entity store config with flow store config
4. **Path Merging**: Use `PathHelper.merge_paths()` for entity subdirectories
5. **Default Merging**: Apply flow defaults to entity configs

### Journal Handling

Journal instances need to be cached and shared:

- Journals are created per flow/entity based on journal config
- Journal cache key is based on journal config (JSON serialized)
- Journals need access to store instance, store config, home config
- Journal creation logic currently in `Coordinator._get_or_create_journal_instance()`

### Connection Pool Injection

Connection pools need to be injected into stores/homes:

- Stores that need connection pools (e.g., MSSQL) have `set_pool()` method
- Homes that need connection pools (e.g., MSSQL) receive pool in constructor
- Connection pools are passed from Coordinator to Flow creation methods

### Run Type Validation

Run type and store incremental alignment needs validation:

- Flow run_type must align with store incremental setting
- Validation happens during flow creation
- Warnings are logged when alignment doesn't match
- Currently in `Coordinator._validate_run_type_incremental_alignment()`

## File Locations

### Existing Files (Modify)
- **Flow**: `src/hygge/core/flow.py` (add class methods)
  - Add `Flow.from_config()` class method
  - Add `Flow.from_entity()` class method
  - Add helper methods: `_merge_entity_config()`, `_merge_home_config()`, `_merge_store_config()`, etc.

### Existing Files (Refactor)
- **Coordinator**: `src/hygge/core/coordinator.py` (simplify)
  - Replace `_create_flows()` with simpler version using Flow class methods
  - Remove `_create_entity_flow()` method
  - Remove `_apply_flow_overrides()` method (move to Flow)
  - Keep `_get_or_create_journal_instance()` or move to Flow (TBD)
  - Keep `_inject_store_pool()` or move to Flow (TBD)
  - Keep `_validate_run_type_incremental_alignment()` or move to Flow (TBD)

### Tests
- `tests/unit/hygge/core/test_flow.py` (extend)
  - Test `Flow.from_config()` class method
  - Test `Flow.from_entity()` class method
  - Test configuration merging logic
  - Test entity config merging
  - Test path merging for entity subdirectories
  - Test journal creation and caching
  - Test connection pool injection
  - Test run type validation

## Benefits

1. **Clear Separation of Concerns**: Flow class handles flow creation, Coordinator handles orchestration
2. **Simplified Coordinator**: Removes ~300 lines of flow creation logic from Coordinator
3. **Better Testability**: Flow creation logic can be tested independently
4. **Easier Maintenance**: Changes to flow creation don't affect Coordinator
5. **Natural API**: `Flow.from_config()` and `Flow.from_entity()` feel natural and intuitive
6. **Follows Rails Philosophy**: "Flows know how to create themselves" - convention over configuration

## Testing Considerations

- Unit tests for `Flow.from_config()` with various configurations
- Unit tests for `Flow.from_entity()` with entity configs
- Unit tests for configuration merging (entity, home, store)
- Unit tests for path merging with entity subdirectories
- Unit tests for journal creation and caching
- Unit tests for connection pool injection
- Unit tests for run type validation
- Integration tests to verify Coordinator still works correctly
- Ensure existing tests continue to pass

## Related Issues

- See `coordinator-refactoring.md` for completed refactoring work (Phases 1, 3-5)
- See `cli-simplification.md` for related CLI refactoring
- See `watermark-tracker-extraction.md` for related Flow refactoring

## Priority

**Ready to Start** - This is the final phase of Coordinator refactoring. Phases 1, 3-5 are complete and stable. Flow creation logic extraction will further simplify Coordinator and complete the refactoring effort.

## Implementation Plan

1. **Extract Flow Creation Methods**
   - Add `Flow.from_config()` class method
   - Add `Flow.from_entity()` class method
   - Add helper methods for config merging

2. **Move Configuration Logic**
   - Move `_apply_flow_overrides()` to Flow class
   - Move configuration merging logic to Flow class
   - Move path merging logic to Flow class

3. **Handle Dependencies**
   - Decide on journal creation (keep in Coordinator or move to Flow)
   - Decide on connection pool injection (keep in Coordinator or move to Flow)
   - Decide on run type validation (keep in Coordinator or move to Flow)

4. **Update Coordinator**
   - Simplify `_create_flows()` to use Flow class methods
   - Remove `_create_entity_flow()` method
   - Update imports and method calls

5. **Add Tests**
   - Add unit tests for Flow class methods
   - Add tests for configuration merging
   - Update integration tests

6. **Verify**
   - Ensure all tests pass
   - Verify Coordinator still works correctly
   - Check for any regressions

## Open Questions

1. **Journal Creation**: Should journal creation stay in Coordinator (needs access to journal cache) or move to Flow (simpler, but needs cache passed in)?
   - **Recommendation**: Keep journal cache in Coordinator, pass to Flow methods. Flow can create journal instances, but cache management stays in Coordinator.

2. **Connection Pool Injection**: Should connection pool injection stay in Coordinator or move to Flow?
   - **Recommendation**: Move to Flow - Flow creation methods receive connection pools and handle injection.

3. **Run Type Validation**: Should run type validation stay in Coordinator or move to Flow?
   - **Recommendation**: Move to Flow - validation happens during flow creation, Flow can log warnings.

4. **Flow Overrides**: Should flow overrides be applied in Coordinator or Flow?
   - **Recommendation**: Move to Flow - Flow knows how to apply overrides to its config.
