# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-10-09

### Added
- Entity pattern for landing zone scenarios with multiple entities
- Registry pattern for scalable Home/Store type system
- Polars + PyArrow foundation for all data movement
- Flow-scoped logging with parallel execution visibility
- Parallel processing via Coordinator-level parallelization
- Configuration system with convention over configuration
- CLI commands: `hygge init`, `hygge start`, `hygge debug`
- Project-centric workflow with automatic `hygge.yml` discovery
- Comprehensive testing suite (158+ tests)
- Documentation and examples

### Fixed
- README documentation accuracy
- Test coverage for edge cases

### Changed
- Flattened core architecture for maximum cohesion
- Consistent naming pattern throughout
- Explicit type system over magic inference

### Security
- No security issues identified in this release

---

*For detailed implementation history, see [HYGGE_DONE.md](HYGGE_DONE.md)*
