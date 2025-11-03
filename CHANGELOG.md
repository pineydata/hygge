# Changelog

All notable changes to hygge will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2025-11-03

### Added

#### Open Mirroring Store for Microsoft Fabric

- Full support for Microsoft Fabric Open Mirroring
- Automatic `__rowMarker__` column injection (required for Open Mirroring)
- `_metadata.json` file generation with key columns configuration
- `_partnerEvents.json` file support at database level
- Timestamp + sequence file naming with microseconds for better uniqueness
- Sequential file naming (20-digit format per Open Mirroring spec)
- Full drop mode - deletes and recreates table folders
- Schema support for organized table structure
- Robust directory deletion with Azure SDK version compatibility

#### Collapsed Entities Pattern

- Entities can now be defined inline in flow configurations
- No need for separate entity files - all config in one place
- Universal defaults at flow level, entity-specific overrides
- Supports large-scale scenarios (60+ entities in single flow)
- Clear hierarchy: flow defaults → entity overrides
- Demonstrates DRY principle - minimal repetition

### Improved

#### Azure Data Lake Storage Operations

- Centralized directory deletion in `azure_onelake.py`
- Handles Azure SDK version differences gracefully
- Better error handling for directory operations
- Retry logic for Azure operations

#### File Naming

- Timestamp-based file naming now includes microseconds
- Reduces timestamp collisions within same second
- Format: `YYYYMMDD_HHMMSS_microseconds_sequence.parquet`

#### Code Quality

- Removed empty `additionalInformation` from JSON output
- Moved imports to module level (better performance and consistency)
- Fixed linting issues and line length compliance

### Fixed

- Directory deletion parameter conflicts with Azure SDK
- Empty JSON objects in `_partnerEvents.json`
- Test failures due to timestamp format changes

## [0.2.0] - 2025-10-18

### Added

#### Azure SQL Database Support

- Full support for Azure SQL Database with MSSQL home and store implementations
- Read from Azure SQL as a data source (home)
- Write to Azure SQL as a destination (store)
- Connection pooling for optimal performance
- Support for named connection configurations
- Direct insert and future write strategies
- Works seamlessly with Azure SQL Database and SQL Server

#### Azure Data Lake Storage Gen2 Store (ADLS)

- Streaming uploads to ADLS Gen2 without local disk I/O
- Support for Microsoft Fabric OneLake (same underlying storage)
- Multiple authentication methods: managed identity, service principal, storage key
- Optimal batch sizes (100K rows) for 10-20MB parquet files
- Atomic writes using temp → move pattern
- Centralized temporary storage under `_tmp/` directory

#### Microsoft Fabric OneLake Store

- Lightweight wrapper around ADLS store with Fabric-specific path conventions
- Auto-builds Lakehouse paths: `Files/{entity}/`
- Support for Mirrored Databases: `Files/LandingZone/{entity}/`
- Convention over configuration - minimal setup required
- Extends all ADLS Gen2 functionality

### Changed

#### Path Management

- Centralized temporary storage: `Files/_tmp/entity/` instead of `Files/entity/_tmp`
- Cloud-native paths - works seamlessly with Azure services
- Dynamic path building based on Fabric type (Lakehouse vs Mirrored DB)

#### Store Architecture

- Generic ADLS store (`ADLSStore`) - base implementation for any Azure Data Lake Storage Gen2 account
- OneLake store (`OneLakeStore`) - extends ADLS with Fabric-specific conventions
- Consistent pattern - follows hygge's file-staging approach with `get_staging_directory()` and `get_final_directory()`

### Improved

#### SQL Integration

- Connection pooling with configurable pool sizes
- Multiple authentication methods (Windows, SQL, Azure AD)
- Optimized batch reads and writes
- Support for table hints and write strategies

#### Performance

- Optimized batch sizes (100K rows default) for optimal parquet file sizes
- Reduced network overhead with larger files (10-20MB compressed)
- Streaming patterns for memory efficiency

#### Testing

- Added comprehensive unit tests for ADLS and OneLake stores
- All unit tests use mocks - no dependency on Azure credentials
- Integration tests properly skipped if credentials not configured
- 288 total tests (279 passing, 9 skipped for integration)

### Dependencies

#### New Dependencies

- `azure-identity` - Azure authentication
- `azure-storage-file-datalake` - Azure Data Lake Storage Gen2 SDK

## [0.1.0] - 2025-10-09

### Added

#### Core Framework

- **Entity Pattern**: One flow for multiple entities in landing zones
- **Parallel Processing**: All entities run simultaneously
- **Registry Pattern**: Easy to add new data sources and destinations
- **Flow-Scoped Logging**: Clear visibility into parallel execution
- **Convention over Configuration**: Smart defaults with progressive complexity
- **CLI Commands**: `hygge init`, `hygge go`, `hygge debug`

#### Data Movement

- Parquet-to-parquet data movement with Polars + PyArrow
- File-based staging system with atomic moves
- Support for multiple file patterns and naming conventions

#### Performance

- Real data tested: 1.5M+ rows moved successfully
- Throughput: 2.8M rows/sec on large entities
- Memory efficient: Polars + PyArrow columnar processing

#### Testing

- 158+ tests covering all functionality
- Unit and integration tests
- Entity pattern tests with 8 scenarios

#### Documentation

- Comprehensive README with getting started guide
- Sample configurations in `samples/` directory
- Runnable demos in `examples/` directory

## [Unreleased] Future Plans

Future plans for upcoming releases:

- S3 store support (coming in v0.3.0)
- Additional cloud storage options
- Enhanced error handling and retries
- More store-specific optimizations
- SQL Home implementation: MS SQL Server connector (completed in v0.2.0)
- Cloud storage support: S3, Azure Blob, GCS (ADLS/OneLake completed in v0.2.0)
- Advanced error recovery: Retry strategies, dead letter queues

[Unreleased]: https://github.com/pineydata/hygge/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/pineydata/hygge/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/pineydata/hygge/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/pineydata/hygge/releases/tag/v0.1.0
