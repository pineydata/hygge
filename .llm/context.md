# hygge Development Context

## Project Overview
hygge (pronounced "hoo-ga") is a data movement framework for solo developers/small teams. **Not a commercial product** - focus on comfort over enterprise rigor.

## Core Architecture
- **Home**: Data source (parquet files, SQL databases)
- **Store**: Data destination (parquet files, cloud storage)
- **Flow**: Single data movement from Home to Store
- **Coordinator**: Manages multiple flows from YAML config

## Key Principles (Rails-inspired)
1. **Convention over Configuration**: Smart defaults, minimal setup
2. **Programmer Happiness**: APIs should feel natural and comfortable
3. **Separation of Concerns**: Clean architecture with configs/ subdirectories

## Development Philosophy
- Keep it simple and focused
- Make common tasks easy
- Make complex tasks possible
- Prioritize user experience
- Write clear, maintainable code
- Test thoroughly but sensibly

**hygge isn't just about moving data - it's about making data movement feel natural, comfortable, and reliable.**

## Current Status
See [HYGGE_PROGRESS.md](../.progress/HYGGE_PROGRESS.md) for detailed assessment.

## Technical Stack
- Python 3.11+, Polars, PyArrow, Pydantic
- Async/await patterns, YAML configuration


## Development Approach
See [development.md](development.md) for detailed guidelines.

**Quick summary**: Rails conventions, centralized configs, type safety, async patterns.
