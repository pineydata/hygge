# Flow Class Simplification Recommendation

## Current Problem

The `Flow` class violates hygge's "Comfort Over Complexity" principle with:

1. **Complex instantiation logic** - Multiple ways to create the same thing
2. **Too many responsibilities** - Handles both configuration parsing and flow execution
3. **Legacy support complexity** - Supports both Pydantic configs and dict configs
4. **Type union complexity** - `Union[Dict[str, Any], ParquetHomeConfig, SQLHomeConfig]`

## hygge Philosophy Violations

- **Comfort Over Complexity**: Too many ways to do the same thing
- **Clarity Over Cleverness**: Complex type unions and conditional logic
- **Convention Over Configuration**: Should have one clear way to create flows

## Recommended Solution

### 1. Simplify Flow Constructor

**Current (Complex):**
```python
def __init__(
    self,
    name: str,
    home: Optional[Home] = None,
    store: Optional[Store] = None,
    home_config: Optional[Union[Dict[str, Any], ParquetHomeConfig, SQLHomeConfig]] = None,
    store_config: Optional[Union[Dict[str, Any], ParquetStoreConfig]] = None,
    options: Optional[Dict[str, Any]] = None
):
```

**Recommended (Simple):**
```python
def __init__(
    self,
    name: str,
    home_config: Dict[str, Any],
    store_config: Dict[str, Any],
    options: Optional[Dict[str, Any]] = None
):
```

### 2. Move Configuration Parsing to Coordinator

The coordinator should handle all configuration complexity:

```python
# In Coordinator._setup_flows()
def _create_flow(self, name: str, config: Dict[str, Any]) -> Flow:
    """Create a flow with simplified configuration."""
    # Parse and validate config here
    home_config = self._parse_home_config(config['home'])
    store_config = self._parse_store_config(config['store'])

    return Flow(
        name=name,
        home_config=home_config,
        store_config=store_config,
        options=config.get('options', {})
    )
```

### 3. Simplify Home/Store Creation

**Current (Complex):**
```python
def _create_home(self, config: Union[Dict[str, Any], ParquetHomeConfig, SQLHomeConfig]) -> Home:
    # Handle Pydantic config objects
    if isinstance(config, (ParquetHomeConfig, SQLHomeConfig)):
        # ... complex logic
    # Handle dictionary configs (legacy support)
    home_type = config.get('type')
    # ... more complex logic
```

**Recommended (Simple):**
```python
def _create_home(self, config: Dict[str, Any]) -> Home:
    """Create home from simple dict config."""
    home_type = config['type']

    if home_type == 'parquet':
        return ParquetHome(name=self.name, path=config['path'], options=config.get('options', {}))
    elif home_type == 'sql':
        return SQLHome(name=self.name, connection=config['connection'], query=config['query'], options=config.get('options', {}))
    else:
        raise ValueError(f"Unknown home type: {home_type}")
```

### 4. Remove Legacy Support

- Remove Pydantic config object support from Flow
- Remove complex type unions
- Use simple dict configs throughout
- Let Pydantic validation happen at the coordinator level

### 5. Single Responsibility Principle

**Flow should only:**
- Execute data movement from home to store
- Handle flow-level error management
- Track progress and metrics

**Flow should NOT:**
- Parse configuration
- Handle multiple config formats
- Manage complex type unions

## Implementation Plan

1. **Phase 1**: Simplify Flow constructor to only accept dict configs
2. **Phase 2**: Move all configuration parsing to Coordinator
3. **Phase 3**: Remove legacy Pydantic config support
4. **Phase 4**: Add comprehensive tests for simplified Flow

## Benefits

- **Comfort**: One clear way to create flows
- **Clarity**: Simple, predictable instantiation
- **Maintainability**: Less complex code to maintain
- **Testability**: Easier to test with simple inputs
- **hygge Alignment**: Follows "Comfort Over Complexity" principle

## Migration Strategy

1. Keep current Flow as `LegacyFlow` for backward compatibility
2. Create new simplified `Flow` class
3. Update Coordinator to use new Flow
4. Update examples and documentation
5. Remove LegacyFlow after migration complete

This approach maintains hygge's philosophy while providing a clear path forward.
