# Development Guidelines

## Code Standards
- **Rails-inspired**: Convention over configuration
- **Clean separation**: Configs in `configs/` subdirectories
- **Type safety**: Pydantic validation throughout
- **Async patterns**: Use async/await for I/O operations

## Configuration System
- Centralized defaults in individual config classes
- Smart defaults with validation
- Support both minimal (`home: path`) and advanced configs
- Remove redundant settings files

## Error Handling
- Custom exception hierarchy: `HomeError`, `StoreError`, `FlowError`
- Retry decorator with exponential backoff
- Graceful failure handling

## Testing Approach
- Focus on behavior that matters
- Test user experience and data integrity
- Verify defaults "just work"
- Don't compromise codebase for tests

## File Organization
```
src/hygge/core/
├── configs/           # Centralized configuration
├── homes/            # Data sources
│   └── configs/      # Home-specific configs
└── stores/           # Data destinations
    └── configs/      # Store-specific configs
```

## Samples Structure
- `minimal_flow.yaml`: Rails spirit with `home`/`store`
- `advanced_home_store.yaml`: Full configuration examples
- Always use `home`/`store` terminology (not `from`/`to`)
