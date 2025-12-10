# Python Philosophy for hygge

## Core Principles (Zen of Python)

### 1. Beautiful is Better Than Ugly
- Code should be aesthetically pleasing and readable
- Elegant solutions over brute force approaches
- Well-designed APIs that feel natural to use
- **hygge application**: `flow.run()` reads naturally, not `FlowExecutor.execute(flow_instance)`

### 2. Explicit is Better Than Implicit
- Make intentions clear in code and APIs
- Avoid hidden magic that surprises developers
- Clear configuration over implicit behavior
- **hygge application**: `home: path` explicitly declares the source, not inferred from context

### 3. Simple is Better Than Complex
- Prefer straightforward solutions over intricate ones
- Break complex problems into simple pieces
- Avoid unnecessary abstractions
- **hygge application**: Minimal configs with smart defaults, not complex setup requirements

### 4. Complex is Better Than Complicated
- Some problems require complexity, but keep it organized
- Complex systems can be elegant if well-structured
- Complicated systems are messy and hard to understand
- **hygge application**: Polars + PyArrow integration is complex but elegant, not a complicated mess

### 5. Flat is Better Than Nested
- Avoid deep hierarchies and nested structures
- Prefer flat namespaces and shallow call stacks
- Keep module organization simple
- **hygge application**: `configs/` subdirectories keep structure flat, not deeply nested config trees

### 6. Sparse is Better Than Dense
- Use whitespace and clear formatting
- Don't pack too much into single lines or functions
- Give code room to breathe
- **hygge application**: Clear, readable config files with sensible spacing, not dense YAML

### 7. Readability Counts
- Code is read more often than written
- Prioritize clarity over cleverness
- Use descriptive names and clear logic
- **hygge application**: `Home`, `Store`, `Flow` names are self-documenting, not cryptic abbreviations

### 8. Special Cases Aren't Special Enough to Break the Rules
- Consistency matters more than handling every edge case
- Don't create exceptions that violate core principles
- Prefer general solutions over special-case handling
- **hygge application**: Unified `Home`/`Store` interfaces work for all types, not special cases per source

### 9. Although Practicality Beats Purity
- Pragmatic solutions over theoretical perfection
- Real-world needs matter more than ideological purity
- Ship working code, iterate toward ideal
- **hygge application**: Async patterns where needed, sync where simpler, not dogmatic about either

### 10. Errors Should Never Pass Silently
- Fail fast and fail clearly
- Don't hide problems or return None silently
- Raise exceptions with helpful messages
- **hygge application**: Custom `HomeError`, `StoreError`, `FlowError` with clear context, not generic exceptions

### 11. Unless Explicitly Silenced
- Sometimes you need to handle errors gracefully
- But make it explicit when you do
- Don't silently swallow exceptions by default
- **hygge application**: Retry decorators are explicit opt-in, not hidden error swallowing

### 12. In the Face of Ambiguity, Refuse the Temptation to Guess
- Don't make assumptions about user intent
- Ask for clarification or fail clearly
- Better to error than to guess wrong
- **hygge application**: Pydantic validation rejects ambiguous configs, not guessing what users meant

### 13. There Should Be One-- and Preferably Only One --Obvious Way to Do It
- Provide clear, recommended patterns
- Don't offer too many equivalent options
- Make the right way obvious
- **hygge application**: `flow.run()` is the obvious way, not multiple execution methods

### 14. Although That Way May Not Be Obvious at First Unless You're Dutch
- Some patterns take time to learn
- But once learned, they should feel natural
- Good defaults help guide users
- **hygge application**: `home`/`store` terminology becomes natural with use, smart defaults guide beginners

### 15. Now is Better Than Never
- Ship working solutions, iterate
- Don't wait for perfect implementations
- Progress over perfection
- **hygge application**: Core functionality ships first, polish comes later

### 16. Although Never is Often Better Than *Right* Now
- Don't rush breaking changes
- Think before implementing
- Some features are better left unimplemented
- **hygge application**: Careful consideration before adding complexity, not every feature request gets implemented

### 17. If the Implementation is Hard to Explain, it's a Bad Idea
- Complex code that's hard to explain is a red flag
- Simple solutions are usually better
- If you can't explain it, refactor it
- **hygge application**: Core data movement logic should be explainable, not black-box magic

### 18. If the Implementation is Easy to Explain, it's a Good Idea
- Clear, simple implementations are valuable
- Good code tells a story
- Easy to explain means easy to maintain
- **hygge application**: `Home.read()` → `Store.write()` is easy to explain and understand

### 19. Namespaces are One Honking Great Idea -- Let's Do More of Those!
- Use modules and packages effectively
- Organize code into logical namespaces
- Avoid global state and pollution
- **hygge application**: `hygge.core`, `hygge.homes`, `hygge.stores` provide clear namespaces

## What is Pythonic Code?

"Pythonic" code follows Python's idioms, conventions, and philosophy. It's code that feels natural to Python developers, leverages the language's strengths, and reads like well-written English.

### Core Characteristics of Pythonic Code

#### 1. Readable and Expressive
Pythonic code reads like natural language and clearly expresses intent.

**Non-Pythonic:**
```python
def proc_data(d):
    r = []
    for i in range(len(d)):
        if d[i] > 0:
            r.append(d[i] * 2)
    return r
```

**Pythonic:**
```python
def process_data(data: list[float]) -> list[float]:
    """Double all positive values in the dataset."""
    return [value * 2 for value in data if value > 0]
```

**hygge application**: `flow.run()` is immediately understandable, not `FlowExecutor.execute(flow_instance)`

#### 2. Leverages Built-in Features
Pythonic code uses Python's built-in functions, types, and language features effectively.

**Non-Pythonic:**
```python
if config.get("home") is not None and config.get("home").get("path") is not None:
    path = config.get("home").get("path")
else:
    path = None
```

**Pythonic:**
```python
path = config.get("home", {}).get("path") if config.get("home") else None
# Or even better with modern Python:
path = (config.get("home") or {}).get("path")
```

**hygge application**: Use Pydantic's `Field` defaults and validators, not manual validation

#### 3. Uses Comprehensions and Generators
Pythonic code prefers list/dict/set comprehensions and generators for transformations.

**Non-Pythonic:**
```python
results = []
for flow in flows:
    if flow.enabled:
        results.append(flow.name)
```

**Pythonic:**
```python
results = [flow.name for flow in flows if flow.enabled]
```

**hygge application**: Generator expressions for streaming data batches, not building full lists in memory

#### 4. Embraces Duck Typing
Pythonic code focuses on what objects can do (their interface), not what they are (their type).

**Non-Pythonic:**
```python
if isinstance(home, ParquetHome):
    data = home.read_parquet()
elif isinstance(home, SqlHome):
    data = home.read_sql()
```

**Pythonic:**
```python
data = home.read()  # Trust that home implements the Home protocol
```

**hygge application**: `Home` and `Store` protocols define interfaces, implementations provide behavior

#### 5. Uses Context Managers
Pythonic code uses `with` statements for resource management.

**Non-Pythonic:**
```python
conn = database.connect()
try:
    data = conn.query("SELECT * FROM users")
finally:
    conn.close()
```

**Pythonic:**
```python
async with database.connect() as conn:
    data = await conn.query("SELECT * FROM users")
```

**hygge application**: `async with home:` and `async with store:` ensure proper resource cleanup

#### 6. Follows EAFP (Easier to Ask for Forgiveness than Permission)
Pythonic code tries operations and handles exceptions, rather than checking preconditions.

**Non-Pythonic:**
```python
if os.path.exists(file_path):
    if os.access(file_path, os.R_OK):
        with open(file_path) as f:
            return f.read()
    else:
        raise PermissionError("Cannot read file")
else:
    raise FileNotFoundError("File does not exist")
```

**Pythonic:**
```python
try:
    with open(file_path) as f:
        return f.read()
except FileNotFoundError:
    raise HomeError(f"Source file not found: {file_path}")
except PermissionError:
    raise HomeError(f"Permission denied: {file_path}")
```

**hygge application**: Attempt operations, catch specific exceptions, provide helpful error messages

#### 7. Uses Type Hints (Modern Pythonic)
Modern Pythonic code includes type hints for clarity and tooling support.

**Non-Pythonic:**
```python
def create_flow(home, store, config):
    return Flow(home, store, config)
```

**Pythonic:**
```python
def create_flow(
    home: Home,
    store: Store,
    config: FlowConfig,
) -> Flow:
    """Create a new data flow from home to store."""
    return Flow(home, store, config)
```

**hygge application**: Full type hints on all public APIs, Pydantic models for configs

#### 8. Uses Enums and Constants
Pythonic code uses enums for constants and magic values.

**Non-Pythonic:**
```python
if store_type == "parquet":
    # ...
elif store_type == "s3":
    # ...
```

**Pythonic:**
```python
class StoreType(str, Enum):
    PARQUET = "parquet"
    S3 = "s3"
    SQL = "sql"

if store_type == StoreType.PARQUET:
    # ...
```

**hygge application**: Use enums for store types, compression options, etc.

#### 9. Leverages Dataclasses and Pydantic
Pythonic code uses modern data structures for configuration and models.

**Non-Pythonic:**
```python
class HomeConfig:
    def __init__(self, path, type="parquet"):
        self.path = path
        self.type = type
        # Manual validation...
```

**Pythonic:**
```python
class HomeConfig(BaseModel):
    path: str = Field(..., description="Path to data source")
    type: StoreType = Field(default=StoreType.PARQUET)

    @field_validator("path")
    def validate_path(cls, v):
        if not v:
            raise ValueError("Path cannot be empty")
        return v
```

**hygge application**: Pydantic models throughout for configs with automatic validation

#### 10. Uses f-strings and Modern String Formatting
Pythonic code uses f-strings for string formatting (Python 3.6+).

**Non-Pythonic:**
```python
message = "Flow {} failed: {}".format(flow_name, error)
# or
message = "Flow %s failed: %s" % (flow_name, error)
```

**Pythonic:**
```python
message = f"Flow {flow_name} failed: {error}"
```

**hygge application**: All error messages and logs use f-strings for clarity

#### 11. Uses `pathlib` for Paths
Pythonic code uses `pathlib.Path` instead of string paths.

**Non-Pythonic:**
```python
import os
file_path = os.path.join(base_dir, "data", "users.parquet")
if os.path.exists(file_path):
    # ...
```

**Pythonic:**
```python
from pathlib import Path
file_path = Path(base_dir) / "data" / "users.parquet"
if file_path.exists():
    # ...
```

**hygge application**: Use `Path` objects for file-based homes and stores

#### 12. Uses `dataclasses` for Simple Data Containers
Pythonic code uses `@dataclass` for simple data structures.

**Non-Pythonic:**
```python
class FlowResult:
    def __init__(self, rows_processed, duration, success):
        self.rows_processed = rows_processed
        self.duration = duration
        self.success = success
```

**Pythonic:**
```python
@dataclass
class FlowResult:
    rows_processed: int
    duration: float
    success: bool
```

**hygge application**: Use dataclasses for simple result objects, Pydantic for configs

#### 13. Uses `__str__` and `__repr__` Appropriately
Pythonic code provides meaningful string representations.

**Non-Pythonic:**
```python
class Flow:
    pass  # No string representation
```

**Pythonic:**
```python
class Flow:
    def __repr__(self) -> str:
        return f"Flow(home={self.home!r}, store={self.store!r})"

    def __str__(self) -> str:
        return f"Flow: {self.home} → {self.store}"
```

**hygge application**: All core classes have meaningful `__repr__` for debugging

#### 14. Uses Property Decorators
Pythonic code uses `@property` for computed attributes.

**Non-Pythonic:**
```python
def get_total_rows(self):
    return self.processed_rows + self.failed_rows
```

**Pythonic:**
```python
@property
def total_rows(self) -> int:
    """Total number of rows processed."""
    return self.processed_rows + self.failed_rows
```

**hygge application**: Use properties for computed values like `flow.status`, `store.size`

#### 15. Follows PEP 8 Style Guide
Pythonic code follows PEP 8 conventions for naming, spacing, and structure.

**Non-Pythonic:**
```python
def ProcessData(data,Config):
    # ...
```

**Pythonic:**
```python
def process_data(data: DataFrame, config: Config) -> DataFrame:
    # ...
```

**hygge application**: Consistent naming: `snake_case` for functions/variables, `PascalCase` for classes

### Pythonic Anti-Patterns to Avoid

1. **Overusing `isinstance()` checks** - Use duck typing and protocols instead
2. **Manual iteration with indices** - Use `enumerate()` or direct iteration
3. **String concatenation with `+`** - Use f-strings or `.join()`
4. **Catching bare `Exception`** - Catch specific exceptions
5. **Using `== None` or `!= None`** - Use `is None` or `is not None`
6. **Mutable default arguments** - Use `None` and assign in function body
7. **Importing with `*`** - Use explicit imports
8. **Not using context managers** - Always use `with` for resources
9. **Premature optimization** - Write clear code first, optimize if needed
10. **Ignoring type hints** - Use type hints for clarity and tooling

### hygge's Pythonic Standards

For hygge development, pythonic code means:

- **Clear, readable APIs** that feel natural: `flow.run()` not `FlowExecutor.execute()`
- **Type hints everywhere** for public APIs and configs
- **Pydantic models** for all configuration with validation
- **Async/await** for I/O operations, sync for computation
- **Protocols** for interfaces (`Home`, `Store`), not abstract base classes
- **Context managers** for all resource management
- **f-strings** for all string formatting
- **`pathlib.Path`** for file paths
- **Specific exceptions** (`HomeError`, `StoreError`) not generic ones
- **Generator patterns** for streaming large datasets
- **Comprehensions** for simple transformations
- **EAFP** - try operations, handle specific exceptions

## Pythonic Patterns for hygge

### Context Managers
- Use `async with` for resource management
- Ensure proper cleanup of connections and files
- **hygge application**: `async with home:` and `async with store:` for proper resource handling

### Type Hints
- Use type hints throughout for clarity
- Leverage Pydantic for runtime validation
- Make APIs self-documenting
- **hygge application**: Full type hints on all public APIs, Pydantic models for configs

### Async/Await
- Use async patterns for I/O operations
- Keep async boundaries clear
- Don't mix sync and async unnecessarily
- **hygge application**: Async for database connections and file I/O, sync for pure computation

### Duck Typing
- Focus on interfaces, not inheritance
- Use protocols for structural typing
- Trust that objects have the right methods
- **hygge application**: `Home` and `Store` protocols define interfaces, implementations provide behavior

### List Comprehensions and Generators
- Use generators for large datasets
- Prefer comprehensions for transformations
- Lazy evaluation where appropriate
- **hygge application**: Generator patterns for streaming data, Polars lazy evaluation

### EAFP (Easier to Ask for Forgiveness than Permission)
- Try operations, handle exceptions
- Don't check preconditions excessively
- Trust the system, handle failures
- **hygge application**: Attempt operations, catch specific exceptions, provide helpful error messages

## Key Takeaways for hygge Development

- **Readability over cleverness**: Code should be clear and maintainable
- **Explicit over implicit**: Make intentions obvious in APIs and configs
- **Simple over complex**: But complex over complicated when needed
- **Practical over pure**: Ship working solutions, iterate
- **One obvious way**: Provide clear patterns, not endless options
- **Fail fast and clearly**: Don't hide errors, provide helpful messages
- **Namespaces matter**: Organize code into logical modules
- **Type safety**: Use type hints and Pydantic for validation
- **Async where needed**: Use async/await for I/O, sync for computation

## Pythonic + Rails Philosophy Alignment

Many Pythonic principles align beautifully with Rails philosophy:

- **Beautiful code** (Python) ↔ **Exalt Beautiful Code** (Rails)
- **Simple is better** (Python) ↔ **Convention over Configuration** (Rails)
- **One obvious way** (Python) ↔ **The Menu is Omakase** (Rails)
- **Readability counts** (Python) ↔ **Optimize for Programmer Happiness** (Rails)
- **Practicality beats purity** (Python) ↔ **No One Paradigm** (Rails)
- **Namespaces** (Python) ↔ **Value Integrated Systems** (Rails)

hygge benefits from both philosophies: Pythonic clarity and Rails-inspired comfort.
