# Configuration Samples

Copy and modify these sample configurations for your data flows.

## Quick Start

1. Copy one of the sample YAML files below
2. Update the paths to your data
3. Run: `python -m hygge.coordinator your_config.yaml`

## Available Samples

- **`minimal_flow.yaml`** - Absolute minimum configuration (Rails spirit!)
- **`simple_parquet_flow.yaml`** - Basic parquet-to-parquet flow
- **`multiple_flows.yaml`** - Multiple flows with mixed configurations
- **`advanced_home_store.yaml`** - Full home/store configuration examples

## Configuration Structure

### Minimal Configuration (Rails Spirit!)

```yaml
flows:
  flow_name:
    home: /path/to/source.parquet
    store: /path/to/destination
    # That's it! Everything else uses smart defaults
```

### Advanced Configuration

```yaml
flows:
  flow_name:
    home: /path/to/source.parquet
    store: /path/to/destination
    options:
      queue_size: 10
    store:
      options:
        compression: zstd
        batch_size: 200000
```

## Common Options

### Home Options
- `batch_size`: Number of rows to read at once (default: 10,000)

### Store Options
- `batch_size`: Number of rows to accumulate before writing (default: 100,000)
- `file_pattern`: Pattern for output files (default: "{sequence:020d}.parquet")
- `compression`: Compression type (snappy, gzip, brotli, lz4, zstd)

### Flow Options
- `queue_size`: Size of internal queue (default: 10)

## Need Help?

If you get validation errors, check:
1. YAML syntax (proper indentation)
2. Required fields (type, path for parquet)
3. File paths exist and are accessible
4. Options are valid values

For more help, see the main README.md or check the examples/ directory.
