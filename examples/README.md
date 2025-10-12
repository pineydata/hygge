# hygge Examples

Simple examples demonstrating hygge's registry pattern and data movement workflows.

## Available Examples

### `parquet_example.py`
Basic parquet-to-parquet data movement with registry pattern.

```bash
python examples/parquet_example.py
```

### `parquet_to_mssql_example.py` (NEW!)
Load test data from parquet into Azure SQL Server.

```bash
# Set environment variables first
export AZURE_SQL_SERVER="yourserver.database.windows.net"
export AZURE_SQL_DATABASE="yourdatabase"

python examples/parquet_to_mssql_example.py
```

### `entity_pattern_example.py`
Demonstrates entity pattern for processing multiple tables.

## What It Does

The example:
1. **Creates sample data** - Generates 1,000 rows of test data
2. **Runs YAML example** - Uses Coordinator with YAML configuration
3. **Runs programmatic example** - Uses Flow directly with registry pattern
4. **Shows results** - Displays created files and data movement

## Key Features Demonstrated

- **Registry Pattern**: Automatic type detection from paths
- **Simple Configuration**: Minimal setup with smart defaults
- **Real Data Movement**: Actual parquet file processing
- **Progress Tracking**: Real-time metrics and logging
- **Error Handling**: Graceful error handling and recovery

## Registry Pattern in Action

```python
# Explicit type configuration
home_config = HomeConfig.create({
    "type": "parquet",
    "path": "data/source.parquet"
})
store_config = StoreConfig.create({
    "type": "parquet",
    "path": "data/destination"
})

# Create and run flow
flow = Flow("my_flow", home_config, store_config)
await flow.start()
```

## YAML Configuration

```yaml
flows:
  my_flow:
    home:
      type: parquet
      path: data/source.parquet
    store:
      type: parquet
      path: data/destination
```

This is hygge's philosophy - making data movement feel natural, comfortable, and reliable.
