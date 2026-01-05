# hygge Adapter System

> Extend hygge with pip-installable adapters, like dbt's `dbt-snowflake` model.

---

## Overview

Adapters are Python packages that add new homes and stores to hygge. They're pip-installable, bring their own dependencies, and register automatically via entry points.

```bash
pip install hygge            # Core (parquet only)
pip install hygge-mssql      # Adds MSSQL home and store
pip install hygge-azure      # Adds ADLS, OneLake, OpenMirroring stores
```

---

## User Experience

### Installation

```bash
# Install what you need
pip install hygge hygge-mssql hygge-azure
```

### Configuration

Adapters are auto-discovered. Just use the types:

```yaml
# hygge.yml
flows:
  users_to_lake:
    home:
      type: mssql           # From hygge-mssql
      connection: ${DB_URL}
      table: users
    store:
      type: adls            # From hygge-azure
      path: landing/users/
```

### Helpful Errors

If an adapter is missing:

```text
Error: Unknown home type: 'salesforce'
       You may need to install an adapter.
       See: https://hygge.dev/adapters
```

### CLI

```bash
hygge adapters  # List installed adapters
```

---

## How It Works

### Registration Flow

```text
pip install hygge-mssql
         │
         ▼
┌─────────────────────────────────────────┐
│ pyproject.toml declares entry point:    │
│ [project.entry-points."hygge.adapters"] │
│ mssql = "hygge_mssql:register"          │
└─────────────────────────────────────────┘
         │
         ▼ (on hygge import or first config parse)
┌─────────────────────────────────────────┐
│ hygge calls:                            │
│ importlib.metadata.entry_points(        │
│     group="hygge.adapters"              │
│ )                                       │
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│ For each entry point:                   │
│   register_func = ep.load()             │
│   register_func()  # imports classes    │
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│ Import triggers __init_subclass__:      │
│                                         │
│ class MssqlHome(Home, home_type="mssql")│
│ class MssqlHomeConfig(..., config_type="mssql")
│                              │          │
│ Home._registry["mssql"] = MssqlHome     │
│ HomeConfig._registry["mssql"] = MssqlHomeConfig
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│ Home.create() finds "mssql" in registry │
│ → returns MssqlHome instance            │
└─────────────────────────────────────────┘
```

**Important**: There are four registries that get populated:

- `Home._registry` - Home classes
- `Store._registry` - Store classes
- `HomeConfig._registry` - Home config classes
- `StoreConfig._registry` - Store config classes

Adapters must register both the class and its config. The `register()` function should import all of them.

---

## Implementation

### Core: Adapter Discovery

```python
# src/hygge/core/adapters.py
"""Adapter discovery for hygge."""
import importlib.metadata
from hygge.messages import get_logger

logger = get_logger("hygge.adapters")

_adapters_loaded = False
_loaded_adapters: list[str] = []


def load_adapters() -> list[str]:
    """Discover and load all installed hygge adapters."""
    global _adapters_loaded, _loaded_adapters

    if _adapters_loaded:
        return _loaded_adapters

    try:
        eps = importlib.metadata.entry_points(group="hygge.adapters")
    except Exception:
        _adapters_loaded = True
        return []

    for ep in eps:
        try:
            register = ep.load()
            register()
            _loaded_adapters.append(ep.name)
            logger.debug(f"Loaded adapter: {ep.name}")
        except Exception as e:
            logger.warning(f"Failed to load adapter '{ep.name}': {e}")

    _adapters_loaded = True
    return _loaded_adapters
```

### Core: Trigger Loading Early

Adapters must be loaded before config parsing (not just before `Home.create()`):

```python
# src/hygge/__init__.py
from .core.adapters import load_adapters

# Load adapters at import time so configs work
load_adapters()
```

### Core: Update Error Messages

Use generic error messages - no hardcoded adapter mappings:

```python
# src/hygge/core/home.py (update create method)
@classmethod
def create(cls, name: str, config: "HomeConfig", entity_name: str = None) -> "Home":
    home_type = config.type
    if home_type not in cls._registry:
        raise ValueError(
            f"Unknown home type: '{home_type}'. "
            f"You may need to install an adapter. "
            f"See: https://hygge.dev/adapters"
        )

    # ... rest unchanged
```

### Core: CLI Command

```python
# src/hygge/cli.py (add command)
@hygge.command()
def adapters():
    """List installed adapters."""
    from hygge.core.adapters import load_adapters, _loaded_adapters

    load_adapters()
    if _loaded_adapters:
        click.echo("Installed adapters:")
        for name in sorted(_loaded_adapters):
            click.echo(f"  • {name}")
    else:
        click.echo("No additional adapters installed.")
    click.echo("\nBuilt-in: parquet (home, store)")
```

---

## Creating an Adapter

### Package Structure

```text
hygge-mssql/
├── src/
│   └── hygge_mssql/
│       ├── __init__.py      # register() function
│       ├── homes/
│       │   ├── __init__.py
│       │   └── home.py      # MssqlHome, MssqlHomeConfig
│       └── stores/
│           ├── __init__.py
│           └── store.py     # MssqlStore, MssqlStoreConfig
├── tests/
├── pyproject.toml
└── README.md
```

### pyproject.toml

```toml
[project]
name = "hygge-mssql"
version = "0.1.0"
description = "MSSQL adapter for hygge"
dependencies = [
    "hygge>=0.5.0",
    "sqlalchemy>=2.0.0",
    "pyodbc>=5.1.0",
]

[project.entry-points."hygge.adapters"]
mssql = "hygge_mssql:register"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### Register Function

```python
# src/hygge_mssql/__init__.py
def register():
    """Register MSSQL homes and stores with hygge."""
    # Importing triggers __init_subclass__ which registers with hygge
    from .homes import MssqlHome, MssqlHomeConfig  # noqa: F401
    from .stores import MssqlStore, MssqlStoreConfig  # noqa: F401

# Also export for direct use
from .homes import MssqlHome, MssqlHomeConfig
from .stores import MssqlStore, MssqlStoreConfig

__all__ = ["MssqlHome", "MssqlHomeConfig", "MssqlStore", "MssqlStoreConfig"]
```

### Home/Store Implementation

Use the existing registry pattern:

```python
# src/hygge_mssql/homes/home.py
from hygge.core import Home, HomeConfig

class MssqlHomeConfig(HomeConfig, config_type="mssql"):
    connection: str
    table: str
    # ...

class MssqlHome(Home, home_type="mssql"):
    # ... implementation
```

---

## Open Design Issue: Connection Pool Management

> **⚠️ Needs further design work before extracting adapters.**

The current `FlowFactory` has hardcoded logic for MSSQL connection pool injection:

```python
# Current problematic pattern in FlowFactory._create_home_instance()
from hygge.homes.mssql import MssqlHome  # Direct import!

if home_config.type == "mssql":
    pool = connection_pools.get(home_config.connection)
    return MssqlHome(..., pool=pool)  # Type-specific instantiation
```

This breaks the adapter abstraction - FlowFactory shouldn't know about specific adapter types.

### Requirements

1. Adapters that need connection pools must be able to receive them
2. FlowFactory must not have type-specific if/else logic
3. Pattern should work for both homes and stores
4. Should be consistent with existing `Store.set_pool()` method

### Possible Approaches

**Option A: Config declares pool need**

Configs expose a `pool_name` property. FlowFactory checks generically:

```python
class MssqlHomeConfig(HomeConfig):
    connection: str

    @property
    def pool_name(self) -> str | None:
        return self.connection

# FlowFactory (generic, no type checking)
home = Home.create(name, config, entity_name)
pool_name = getattr(config, 'pool_name', None)
if pool_name:
    pool = connection_pools.get(pool_name)
    if pool:
        home.set_pool(pool)
```

**Option B: Post-creation configure hook**

Homes get a `configure(context)` method called after creation:

```python
class Home(ABC):
    def configure(self, context: dict) -> None:
        """Called after creation with coordinator context."""
        pass

class MssqlHome(Home):
    def configure(self, context: dict):
        pools = context.get("connection_pools", {})
        self._pool = pools.get(self.config.connection)
```

**Option C: Service registry**

Homes look up pools from a global registry instead of receiving injection.

### Decision Needed

Before extracting MSSQL to an adapter, we need to:

1. Choose an approach for generic pool injection
2. Add `set_pool()` to `Home` base class (stores already have it)
3. Refactor `FlowFactory` to remove type-specific logic

---

## Migration Plan

### Phase 1: Core Infrastructure

- [ ] Create `src/hygge/core/adapters.py`
- [ ] Update `__init__.py` to load adapters at import
- [ ] Update `Home.create()` and `Store.create()` error messages
- [ ] Add `hygge adapters` CLI command
- [ ] Keep existing homes/stores in core temporarily

### Phase 2: Refactor FlowFactory

- [ ] Design and implement generic pool injection
- [ ] Add `set_pool()` to Home base class
- [ ] Remove type-specific logic from FlowFactory
- [ ] Test with built-in homes/stores

### Phase 3: Extract MSSQL Adapter

- [ ] Create `hygge-mssql` repository
- [ ] Move MSSQL code from core
- [ ] Publish to PyPI
- [ ] Remove from core

### Phase 4: Extract Azure Adapter

- [ ] Create `hygge-azure` repository
- [ ] Move ADLS, OneLake, OpenMirroring
- [ ] Publish to PyPI

### Phase 5: Extract SQLite Adapter

- [ ] Create `hygge-sqlite` repository
- [ ] Move SQLite store
- [ ] Publish to PyPI

---

## Official Adapters

| Package | Types | Status |
|---------|-------|--------|
| `hygge` (core) | `parquet` | ✓ Built-in |
| `hygge-mssql` | `mssql` | Planned |
| `hygge-azure` | `adls`, `onelake`, `open_mirroring` | Planned |
| `hygge-sqlite` | `sqlite` | Planned |

## Community Adapters (future)

| Package | Types |
|---------|-------|
| `hygge-salesforce` | `salesforce` |
| `hygge-snowflake` | `snowflake` |
| `hygge-bigquery` | `bigquery` |
| `hygge-duckdb` | `duckdb` |
| `hygge-postgres` | `postgres` |
| `hygge-s3` | `s3` |

---

## Benefits

1. **Core stays lean** - Only parquet in base package
2. **Dependencies isolated** - Each adapter brings what it needs
3. **Standard Python** - pip, PyPI, virtual environments
4. **Community extensible** - Anyone can publish adapters
5. **Familiar pattern** - Like dbt adapters

---

## Next Steps

1. Resolve connection pool management design
2. Implement Phase 1 (core infrastructure)
3. Implement Phase 2 (FlowFactory refactor)
4. Create adapter template repository
5. Extract MSSQL as proof of concept
