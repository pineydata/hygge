# Config Architecture Design Notes

## Current Problem

The current FlowConfig approach is brittle and not scalable:

```python
# This breaks every time we add a new config type
home: Union[str, HomeConfig, ParquetHomeConfig] = Field(...)
store: Union[str, StoreConfig, ParquetStoreConfig] = Field(...)
```

**Issues:**
- Hardcoded type detection from file paths
- Manual updates required for every new config type
- Type mismatches between base classes and specific implementations
- Circular import issues

## Proposed Solution: Registry Pattern with Explicit Types

### Core Principles

1. **Explicit Types**: No guessing from file paths
2. **Hierarchical Structure**: Flow → Type → Entities
3. **Progressive Complexity**: Simple to complex configurations
4. **Scalable**: New types require zero changes to existing code
5. **Hygge Philosophy**: Clean, cozy, comfortable API

### Architecture Overview

A flow is defined by a single home/store combination with progressive complexity:

#### Level 1: Simple (Everything in flow.yml)
```yaml
# configs/flows/simple_flow/flow.yml
name: "simple_flow"
home:
  type: "parquet"
  path: "data/source"
store:
  type: "parquet"
  path: "data/destination"
entities:
  - name: "users"
  - name: "orders"
```

#### Level 2: Grouped Entities (flow.yml + entities.yml)
```
configs/flows/salesforce_to_lake/
├── flow.yml              # Flow definition + home + store
└── entities.yml          # Grouped entities with defaults
```

```yaml
# configs/flows/salesforce_to_lake/flow.yml
name: "salesforce_to_lake"
home:
  type: "salesforce"
  connection: "${SALESFORCE_URL}"
store:
  type: "parquet"
  path: "data/lake"
# No entities listed - they're in entities.yml
```

```yaml
# configs/flows/salesforce_to_lake/entities.yml
defaults:
  key: Id
  timestamp: SystemModstamp
  schema: salesforce

entities:
  - name: "Account"
  - name: "Contact"
  - name: "Opportunity"
  - name: "Lead"
```

#### Level 3: Individual Entity Files (flow.yml + entities/ directory)
```
configs/flows/complex_flow/
├── flow.yml              # Flow definition + home + store
└── entities/             # Individual entity files
    ├── users.yml
    ├── orders.yml
    └── products.yml
```

```yaml
# configs/flows/complex_flow/flow.yml
name: "complex_flow"
home:
  type: "sql"
  connection: "${DATABASE_URL}"
  schema: "public"
store:
  type: "parquet"
  path: "data/lake"
  compression: "snappy"
# No entities listed - they're in separate files
```

```yaml
# configs/flows/complex_flow/entities/users.yml
name: "users"
where: "created_at > '2024-01-01'"
batch_size: 10000
```

### File Structure Proposal

```
configs/
├── flows/
│   ├── simple_flow/
│   │   └── flow.yml              # Everything in one file
│   │
│   ├── salesforce_to_lake/
│   │   ├── flow.yml              # Flow definition + home + store
│   │   └── entities.yml          # Grouped entities with defaults
│   │
│   ├── complex_flow/
│   │   ├── flow.yml              # Flow definition + home + store
│   │   └── entities/              # Individual entity files
│   │       ├── users.yml
│   │       ├── orders.yml
│   │       └── products.yml
│   │
│   └── api_to_parquet/
│       ├── flow.yml
│       └── entities/
│           ├── users.yml
│           └── orders.yml
```

## Implementation Details

### Registry Pattern for Base Classes

#### Config Classes
```python
class HomeConfig(ABC):
    _registry: Dict[str, Type['HomeConfig']] = {}

    def __init_subclass__(cls, config_type: str = None):
        super().__init_subclass__()
        if config_type:
            cls._registry[config_type] = cls

    @classmethod
    def create(cls, data: Union[str, Dict]) -> 'HomeConfig':
        # Smart creation logic that works for any registered type
        pass

class StoreConfig(ABC):
    _registry: Dict[str, Type['StoreConfig']] = {}

    def __init_subclass__(cls, config_type: str = None):
        super().__init_subclass__()
        if config_type:
            cls._registry[config_type] = cls

    @classmethod
    def create(cls, data: Union[str, Dict]) -> 'StoreConfig':
        # Smart creation logic that works for any registered type
        pass
```

#### Home and Store Classes
```python
class Home(ABC):
    _registry: Dict[str, Type['Home']] = {}

    def __init_subclass__(cls, home_type: str = None):
        super().__init_subclass__()
        if home_type:
            cls._registry[home_type] = cls

    @classmethod
    def create(cls, name: str, config: HomeConfig) -> 'Home':
        home_type = config.type
        if home_type not in cls._registry:
            raise ValueError(f"Unknown home type: {home_type}")
        return cls._registry[home_type](name, config)

class Store(ABC):
    _registry: Dict[str, Type['Store']] = {}

    def __init_subclass__(cls, store_type: str = None):
        super().__init_subclass__()
        if store_type:
            cls._registry[store_type] = cls

    @classmethod
    def create(cls, name: str, config: StoreConfig) -> 'Store':
        store_type = config.type
        if store_type not in cls._registry:
            raise ValueError(f"Unknown store type: {store_type}")
        return cls._registry[store_type](name, config)
```

### Specific Config Classes

```python
class ParquetHomeConfig(HomeConfig, config_type="parquet"):
    def __init__(self, path: str, batch_size: int = 10000, **kwargs):
        super().__init__(type="parquet", path=path, **kwargs)
        self.batch_size = batch_size

class SQLHomeConfig(HomeConfig, config_type="sql"):
    def __init__(self, connection: str, table: str, **kwargs):
        super().__init__(type="sql", path=table, **kwargs)
        self.connection = connection
        self.table = table

class ParquetStoreConfig(StoreConfig, config_type="parquet"):
    def __init__(self, path: str, compression: str = "snappy", **kwargs):
        super().__init__(type="parquet", path=path, **kwargs)
        self.compression = compression

class CSVStoreConfig(StoreConfig, config_type="csv"):
    def __init__(self, path: str, delimiter: str = ",", **kwargs):
        super().__init__(type="csv", path=path, **kwargs)
        self.delimiter = delimiter
```

### Specific Home and Store Classes

```python
class ParquetHome(Home, home_type="parquet"):
    def __init__(self, name: str, config: ParquetHomeConfig):
        super().__init__(name, config.options)
        self.config = config

class SQLHome(Home, home_type="sql"):
    def __init__(self, name: str, config: SQLHomeConfig):
        super().__init__(name, config.options)
        self.config = config

class ParquetStore(Store, store_type="parquet"):
    def __init__(self, name: str, config: ParquetStoreConfig):
        super().__init__(name, config.options)
        self.config = config

class CSVStore(Store, store_type="csv"):
    def __init__(self, name: str, config: CSVStoreConfig):
        super().__init__(name, config.options)
        self.config = config
```

### FlowConfig Simplification

```python
class FlowConfig(BaseModel):
    name: str = Field(..., description="Flow name")
    home: Union[str, Dict] = Field(..., description="Home configuration")
    store: Union[str, Dict] = Field(..., description="Store configuration")
    entities: Optional[List[Union[str, Dict]]] = Field(default=None, description="Entities to move")

    @field_validator("home", mode="before")
    @classmethod
    def parse_home(cls, v):
        # Registry pattern creates the right HomeConfig
        config = HomeConfig.create(v)
        # Registry pattern creates the right Home instance
        return Home.create("flow_home", config)

    @field_validator("store", mode="before")
    @classmethod
    def parse_store(cls, v):
        # Registry pattern creates the right StoreConfig
        config = StoreConfig.create(v)
        # Registry pattern creates the right Store instance
        return Store.create("flow_store", config)
```

### Factory Simplification

```python
class Factory:
    """Simplified factory that delegates to registry patterns."""

    def create_home(self, name: str, config: HomeConfig) -> Home:
        """Create Home instance using registry pattern."""
        return Home.create(name, config)

    def create_store(self, name: str, config: StoreConfig) -> Store:
        """Create Store instance using registry pattern."""
        return Store.create(name, config)
```

### Clean Flow Object

```python
class Flow:
    """Flow orchestrates data movement between a Home and Store."""

    def __init__(self, name: str, home: Home, store: Store, **options):
        self.name = name
        self.home = home      # Just a Home instance (any type)
        self.store = store    # Just a Store instance (any type)
        self.options = options

    async def run(self):
        """Flow doesn't care about types - just orchestrates data movement."""
        async for batch in self.home.read():
            await self.store.write(batch)
```

### Progressive Discovery Logic

```python
def load_flow_config(flow_dir: Path) -> FlowConfig:
    flow_file = flow_dir / "flow.yml"
    flow_data = yaml.load(flow_file)

    # Check for entities in different locations
    if "entities" in flow_data:
        # Level 1: Entities in flow.yml
        return FlowConfig(**flow_data)
    elif (flow_dir / "entities.yml").exists():
        # Level 2: Entities in single entities.yml file
        entities_data = yaml.load(flow_dir / "entities.yml")
        entities = []

        # Process defaults
        defaults = entities_data.get("defaults", {})

        # Process entity list
        for entity in entities_data.get("entities", []):
            if isinstance(entity, str):
                # Simple name only
                entities.append({"name": entity, **defaults})
            else:
                # Complex entity config
                entities.append({**defaults, **entity})

        return FlowConfig(**flow_data, entities=entities)
    elif (flow_dir / "entities").exists():
        # Level 3: Entities in separate files
        entities_dir = flow_dir / "entities"
        entities = []
        for entity_file in entities_dir.glob("*.yml"):
            entity_data = yaml.load(entity_file)
            entities.append(entity_data)
        return FlowConfig(**flow_data, entities=entities)

    raise ValueError(f"No entities found for flow: {flow_dir}")
```

## Rework Assessment

### Current State
- ✅ **Base Classes**: `Home` and `Store` exist and are well-designed
- ✅ **Specific Implementations**: `ParquetHome` and `ParquetStore` exist
- ✅ **Config Classes**: `HomeConfig`, `StoreConfig`, `ParquetHomeConfig`, `ParquetStoreConfig` exist
- ✅ **Factory Pattern**: Currently implemented and working
- ❌ **Registry Pattern**: Not implemented yet

### Rework Required

#### 1. Base Classes (Medium Effort)
```python
# Current: Simple inheritance
class Home:
    def __init__(self, name: str, options: Optional[Dict[str, Any]] = None):

# New: Registry pattern
class Home(ABC):
    _registry: Dict[str, Type['Home']] = {}

    def __init_subclass__(cls, home_type: str = None):
        super().__init_subclass__()
        if home_type:
            cls._registry[home_type] = cls

    @classmethod
    def create(cls, name: str, config: HomeConfig) -> 'Home':
        # Registry logic
```

#### 2. Specific Implementations (Low Effort)
```python
# Current
class ParquetHome(Home):

# New: Just add the registry parameter
class ParquetHome(Home, home_type="parquet"):
```

#### 3. Config Classes (Medium Effort)
```python
# Current: Simple inheritance
class HomeConfig(BaseModel):

# New: Registry pattern + create method
class HomeConfig(ABC):
    _registry: Dict[str, Type['HomeConfig']] = {}

    def __init_subclass__(cls, config_type: str = None):
        # Registry logic

    @classmethod
    def create(cls, data: Union[str, Dict]) -> 'HomeConfig':
        # Smart creation logic
```

#### 4. Factory Simplification (Low Effort)
```python
# Current: Manual registry management
class Factory:
    def __init__(self):
        self._home_types: Dict[str, Type[Home]] = {
            "parquet": ParquetHome,
        }

# New: Delegates to registry
class Factory:
    def create_home(self, name: str, config: HomeConfig) -> Home:
        return Home.create(name, config)
```

#### 5. FlowConfig Updates (Medium Effort)
```python
# Current: Complex Union types
home: Union[str, HomeConfig, ParquetHomeConfig] = Field(...)

# New: Simple types with smart validators
home: Union[str, Dict] = Field(...)

@field_validator("home", mode="before")
@classmethod
def parse_home(cls, v):
    config = HomeConfig.create(v)
    return Home.create("flow_home", config)
```

### Estimated Effort

| Component | Effort | Risk |
|-----------|--------|------|
| **Base Classes** | Medium | Low |
| **Config Classes** | Medium | Medium |
| **Specific Implementations** | Low | Low |
| **Factory Updates** | Low | Low |
| **FlowConfig Updates** | Medium | Medium |
| **Testing** | High | Medium |

### Total Estimate: 2-3 days

**Why it's manageable:**
- ✅ Existing code is well-structured
- ✅ Clear patterns to follow
- ✅ Incremental changes possible
- ✅ Tests can be updated gradually

**Why it's significant:**
- ❌ Changes touch core architecture
- ❌ Need to update all tests
- ❌ Need to ensure backward compatibility during transition

## Open Questions

### 1. Entity Discovery Pattern
- **Level 1**: Entities listed in flow.yml
- **Level 2**: Entities in single entities.yml file with defaults
- **Level 3**: Entities in separate files in entities/ directory
- **Question**: Should we support all three patterns?

### 2. Config Resolution
- How do we merge defaults with entity-specific configs?
- Should entity configs inherit from home/store configs?
- How do we handle config validation across the hierarchy?

### 3. Flow Granularity
- A flow handles one home→store combination
- Multiple entities can be moved in a single flow
- How do we handle dependencies between entities?

### 4. File Naming Conventions
- Entity files use simple names: `users.yml`, `orders.yml`
- How do we handle special characters in entity names?
- Should we support nested entity configs?

### 5. Config Inheritance
- Should entity configs inherit from home/store configs?
- How do we handle overrides and conflicts?
- Should we support config templates?

## Benefits of This Approach

1. **Explicit Types**: No guessing from file paths
2. **Scalable**: New types require zero changes to existing code
3. **Progressive Complexity**: Start simple, grow into complexity as needed
4. **Maintainable**: Clear separation of concerns
5. **Flexible**: Supports both simple and complex configurations
6. **Hygge Philosophy**: Clean, cozy, comfortable API using "entities"
7. **Self-contained**: Each flow directory contains everything it needs
8. **Grouped Entities**: Support for shared defaults across multiple entities
9. **Familiar Patterns**: Matches existing config2 structure

## Next Steps

1. **Implement Registry Pattern**: Start with base HomeConfig and StoreConfig classes
2. **Update FlowConfig**: Simplify to use explicit types and progressive complexity
3. **Design YAML Structure**: Implement the flows/ directory structure with entities/
4. **Create Examples**: Build sample configs for both simple and complex flows
5. **Test Integration**: Ensure coordinator can load and validate new structure
6. **Update Documentation**: Reflect the new "entities" terminology and hygge philosophy

## References

- [dbt Project Structure](https://docs.getdbt.com/reference/project-configs/project-structure)
- [Python Registry Pattern](https://realpython.com/factory-method-python/)
- [Pydantic Field Validators](https://docs.pydantic.dev/2.0/usage/validators/)
