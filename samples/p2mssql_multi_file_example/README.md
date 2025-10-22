# Parquet-to-MSSQL Multi-File Entity Pattern Example

This example demonstrates the **Entity-First approach** for loading parquet files into Azure SQL Database using hygge's multi-file entity pattern.

## 🏠 What This Demonstrates

- **Entity-First Architecture**: Flow configurations as templates, entity-specific details in separate files
- **Multi-Scale Processing**: Small (5K), medium (50K), and large (150K) tables
- **CCI Optimization**: Batch sizing optimized for Columnstore Indexes
- **Parallel Processing**: 3 entities running simultaneously
- **Auto-Table Creation**: Automatic table creation with proper data types

## 📊 Sample Data

**3 Entities Total (~205K rows):**
- `opportunities` - 5,000 rows (small table, quick demo)
- `accounts` - 50,000 rows (medium table, typical business data)
- `contacts` - 150,000 rows (large table, demonstrates CCI batching)

## 🚀 Quick Start

```bash
cd samples/p2mssql_multi_file_example

# 1. Set up environment variables
export AZURE_SQL_SERVER="your-server.database.windows.net"
export AZURE_SQL_DATABASE="your-database"

# 2. Generate sample data
python generate_data.py

# 3. Run the Entity-First approach
hygge start
```

## 📁 Project Structure

```
p2mssql_multi_file_example/
├── hygge.yml                           # Project configuration
├── generate_data.py                     # Data generation script
├── flows/
│   └── parquet_to_sql/
│       ├── flow.yml                     # Flow template (shared config)
│       └── entities/
│           ├── accounts.yml             # Entity-specific config
│           ├── contacts.yml             # Entity-specific config
│           └── opportunities.yml        # Entity-specific config
└── data/
    └── source/
        ├── accounts/
        │   └── accounts_2025_01.parquet
        ├── contacts/
        │   └── contacts_2025_01.parquet
        └── opportunities/
            └── opportunities_2025_01.parquet
```

## 🔧 Configuration Highlights

### Flow Template (`flow.yml`)
- **Shared home/store config** - base paths and connection
- **Flow defaults** - batch size, parallelism, timeouts
- **Entity-agnostic** - works with any number of entities

### Entity Configs (`entities/*.yml`)
- **Entity-specific details** - table names, paths, options
- **Inherits flow defaults** - automatic configuration
- **Clean separation** - easy to add/remove entities

### Performance Settings
- **Batch size**: 50K rows (CCI-optimized)
- **Parallel workers**: 3 (matches entity count)
- **Connection pool**: 3 connections
- **Timeout**: 10 minutes per entity

## 🎯 Expected Results

```
✅ All flows completed successfully!
📊 Total rows processed: ~205,000
⏱️  Total time: ~2-3 minutes
🚀 Parallel processing: 3 entities simultaneously
```

## 🔍 What to Observe

1. **Parallel Execution**: All 3 entities start simultaneously
2. **Smart Batching**: Large tables use multiple batches
3. **Auto-Table Creation**: Tables created with proper schemas
4. **Performance Scaling**: Different speeds for different table sizes
5. **Clean Completion**: All flows finish successfully

This example showcases hygge's **Entity-First approach** in a production-ready, repo-friendly format! 🏠✨
