# hygge

A cozy, comfortable data movement framework that makes data feel at home.

## Philosophy

hygge (pronounced "hoo-ga") is a Danish word representing comfort, coziness, and well-being. This framework brings those qualities to data movement:

- **Comfort**: Data should feel at home wherever it lives
- **Simplicity**: Clean, intuitive APIs that feel natural
- **Reliability**: Robust, predictable behavior without surprises
- **Flow**: Smooth, efficient movement without friction

## Core Concepts

### Home
Where data starts its journey. A home is a comfortable, familiar place that data lives before moving:

```python
home = SQLHome(
    "users",
    connection=conn,
    options={'batch_size': 10_000}
)

async for batch in home.read_batches():
    # Data flows naturally
```

### Store
Where data rests after its journey. A store provides a cozy place for data to settle:

```python
store = ParquetStore(
    "users",
    path="data/users",
    options={'compression': 'snappy'}
)

await store.write(df)  # Data finds its place
```

### Flow
The natural movement of data from home to store:

```python
flow = Flow(
    home=SQLHome("users"),
    store=ParquetStore("users")
)

await flow.start()  # Data moves comfortably
```

### Coordinator
Orchestrates multiple flows with care:

```python
coordinator = Coordinator("flows.yml")
await coordinator.start()  # Everything works together
```

## Design Principles

1. **Comfort Over Complexity**
   - APIs should feel natural and intuitive
   - Configuration should be simple but flexible
   - Defaults should "just work"

2. **Flow Over Force**
   - Data should move smoothly between systems
   - Batching and buffering should happen naturally
   - Progress should be visible but unobtrusive

3. **Reliability Over Speed**
   - Prefer robust, predictable behavior
   - Handle errors gracefully
   - Make recovery simple

4. **Clarity Over Cleverness**
   - Simple, clear code over complex optimizations
   - Explicit configuration over implicit behavior
   - Clear logging and progress tracking

## Template-Driven

hygge uses YAML templates to define data flows:

```yaml
homes:
  users:
    type: sql
    connection: ${DB_CONNECTION}
    options:
      table: users
      batch_size: 10000

stores:
  users_store:
    type: parquet
    path: data/users
    options:
      compression: snappy

flows:
  users_to_parquet:
    home: users
    store: users_store
    options:
      incremental: true
```

## Extensibility

Adding new homes and stores is comfortable:

```python
class MyHome(Home):
    """A cozy new home for data."""
    async def read_batches(self):
        # Implement your reading logic

class MyStore(Store):
    """A cozy new store for data."""
    async def write(self, df):
        # Implement your writing logic
```

## Development Philosophy

- Keep it simple and focused
- Make common tasks easy
- Make complex tasks possible
- Prioritize user experience
- Write clear, maintainable code
- Test thoroughly but sensibly

hygge isn't just about moving data - it's about making data movement feel natural, comfortable, and reliable.
