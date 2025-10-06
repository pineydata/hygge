# hygge Examples

Simple examples demonstrating hygge's registry pattern and parquet-to-parquet data movement.

## Quick Start

Run the complete example:

```bash
python examples/parquet_example.py
```

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
