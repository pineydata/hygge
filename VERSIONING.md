# Versioning Strategy

hygge follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) (SemVer).

## Version Format

`MAJOR.MINOR.PATCH` (e.g., `1.2.3`)

- **MAJOR**: Breaking changes to the public API
- **MINOR**: New features that are backward compatible
- **PATCH**: Bug fixes that are backward compatible

## Current Version

**v0.1.0** - First stable release with entity pattern

## Release History

### v0.1.0 (2024-12-19)
- **First stable release**
- Entity pattern for landing zones
- Parallel processing with Coordinator
- Registry pattern for Home/Store types
- Polars + PyArrow foundation
- Flow-scoped logging
- 158+ tests passing
- Complete documentation and examples

## Future Releases

### v0.2.0 (Planned)
- **SQL Home implementation**
- MS SQL Server connector
- Connection pooling and management
- Query optimization and batch fetching
- Integration with existing ParquetStore

### v0.3.0 (Planned)
- **Cloud storage support**
- S3, Azure Blob, GCS connectors
- Cloud-native error handling
- Performance optimizations

### v1.0.0 (Planned)
- **Production ready**
- Advanced error recovery
- Metrics and monitoring
- Performance benchmarking
- Production deployment guides

## Pre-Release Versions

During development, we use pre-release versions:

- **v0.1.0-alpha.1**: Alpha release for testing
- **v0.1.0-beta.1**: Beta release for feedback
- **v0.1.0-rc.1**: Release candidate for final testing

## Version Bumping Guidelines

### Patch (0.1.0 → 0.1.1)
- Bug fixes
- Documentation updates
- Test improvements
- Performance optimizations

### Minor (0.1.0 → 0.2.0)
- New features
- New Home/Store implementations
- Configuration enhancements
- Backward compatible changes

### Major (0.1.0 → 1.0.0)
- Breaking API changes
- Configuration format changes
- Removal of deprecated features
- Significant architecture changes

## Git Tags

Each release is tagged in git:

```bash
git tag v0.1.0 -m "First stable release with entity pattern"
git push origin v0.1.0
```

## Release Process

1. **Update version** in `pyproject.toml`
2. **Update CHANGELOG.md** with new features/fixes
3. **Create release notes** in `RELEASE_NOTES_vX.X.X.md`
4. **Tag the release** in git
5. **Create GitHub release** with notes
6. **Update documentation** if needed

## Version Compatibility

### Python Version
- **Minimum**: Python 3.11+
- **Tested**: Python 3.11, 3.12
- **Future**: Python 3.13+ (when available)

### Dependencies
- **Polars**: >= 1.21.0
- **Pydantic**: >= 2.6.1
- **PyYAML**: >= 6.0.2
- **Tenacity**: >= 9.0.0
- **Colorama**: >= 0.4.6
- **Click**: >= 8.0.0

## Migration Guides

When breaking changes occur, migration guides will be provided:

- **v0.1.0 → v0.2.0**: SQL Home migration guide
- **v0.2.0 → v0.3.0**: Cloud storage migration guide
- **v0.3.0 → v1.0.0**: Production deployment guide

---

*For detailed change history, see [CHANGELOG.md](CHANGELOG.md)*
