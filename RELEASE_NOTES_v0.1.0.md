# hygge v0.1.0 Release Notes

**Release Date**: October 9, 2025
**Version**: 0.1.0
**Status**: First Release

## 🎉 What's New

hygge v0.1.0 is the first stable release of a modern data movement framework built on **Polars + PyArrow**. This release focuses on parquet-to-parquet data movement with an innovative **entity pattern** for landing zone scenarios.

## ✨ Key Features

- **Entity Pattern**: One flow for multiple entities in landing zones
- **Parallel Processing**: All entities run simultaneously
- **Registry Pattern**: Easy to add new data sources and destinations
- **Flow-Scoped Logging**: Clear visibility into parallel execution
- **Convention over Configuration**: Smart defaults with progressive complexity
- **CLI Commands**: `hygge init`, `hygge start`, `hygge debug`

## 🚀 Performance

- **Real data tested**: 1.5M+ rows moved successfully
- **Throughput**: 2.8M rows/sec on large entities
- **Memory efficient**: Polars + PyArrow columnar processing

## 📦 Installation

```bash
pip install hygge
```

## 🏃‍♂️ Quick Start

### CLI Usage
```bash
# Initialize new project
hygge init my_project

# Run flows with automatic project discovery
hygge start

# Debug configuration
hygge debug
```

### Programmatic Usage
```python
from hygge import Coordinator

# Automatic project discovery
coordinator = Coordinator()
await coordinator.run()

# Or specify config explicitly
coordinator = Coordinator("flows.yml")
await coordinator.run()
```

## 🧪 Testing

- **158+ tests** covering all functionality
- Unit and integration tests
- Entity pattern tests with 8 scenarios

## 📚 Documentation

- **README.md**: Getting started guide
- **samples/**: Example configurations
- **examples/**: Runnable demos

## 🔧 Technical Details

- **Core**: `Coordinator`, `Flow`, `Home`, `Store`
- **Registry Pattern**: Type-safe instantiation
- **Polars + PyArrow**: Columnar data processing
- **YAML-based**: Human-readable configurations
- **Pydantic validation**: Type safety and error messages

## 🎯 What's Next

- **SQL Home implementation**: MS SQL Server connector
- **Cloud storage support**: S3, Azure Blob, GCS
- **Advanced error recovery**: Retry strategies, dead letter queues

## 🤝 Contributing

hygge follows Rails-inspired principles:
- **Comfort over complexity**
- **Convention over configuration**
- **Programmer happiness**
- **Flow over force**

## 📄 License

[Add your license information here]

## 🙏 Acknowledgments

Built with love for data engineers who want their data to feel at home.

---

**hygge isn't just about moving data - it's about making data movement feel natural, comfortable, and reliable.** 🏡
