# hygge

![hygge logo](hygge.svg)

A cozy, comfortable data movement framework that makes data feel at home.

## Philosophy

hygge (pronounced "hoo-ga") is a Danish word representing comfort, coziness, and well-being. This framework brings those qualities to data movement:

- **Comfort**: You should relax while you move some tables.
- **Simplicity**: Clean, intuitive APIs that feel natural
- **Reliability**: Robust, predictable behavior without surprises
- **Flow**: Smooth, efficient movement without friction

### Built on Polars + PyArrow

hygge is built on **Polars with PyArrow backend** for optimal data movement performance. This combination provides:

- Efficient columnar memory format for large datasets
- Automatic batching and streaming capabilities
- Broad database compatibility via SQLAlchemy
- Zero-copy operations where possible
- Clean, intuitive API that feels natural

We chose Polars because it provides the best balance of performance, developer experience, and compatibility for extract-and-load workflows.

### Standing on Cozy Shoulders

hygge wraps proven community tools rather than reinventing them. For Salesforce, we use `simple-salesforce`. For cloud storage, we use the official SDKs (`boto3`, `google-cloud-storage`, `azure-storage`). For databases, we use mature drivers like `pyodbc` and `duckdb`.

This means you get battle-tested reliability where it matters most, wrapped in hygge's comfortable patterns.

## Quick Start

Get started with hygge in three simple steps. We'll walk you through each one.

### Step 1: Initialize Your Project

Create a new hygge project with example configuration:

```bash
hygge init my-project
cd my-project
```

This creates a cozy, comfortable project structure:

```text
my-project/
├── hygge.yml           # Project-level configuration
└── flows/              # Flow definitions
    └── example_flow/
        ├── flow.yml    # Flow configuration (home, store, defaults)
        └── entities/   # Entity definitions
            └── users.yml
```

**What you get:**

- `hygge.yml` - Project configuration file with example settings
- `flows/example_flow/flow.yml` - A complete example flow configuration
- `flows/example_flow/entities/users.yml` - An example entity definition

### Step 2: Configure Your Flow

Edit `flows/example_flow/flow.yml` to point to your data sources and destinations:

```yaml
name: "example_flow"
home:
  type: "parquet"
  path: "data/source"        # Where your source data lives
store:
  type: "parquet"
  path: "data/destination"    # Where data should be written

defaults:
  key_column: "id"          # Primary key for watermark tracking
  batch_size: 10000         # Rows per batch
```

**Understanding the flow:**

- `home` - Your data source (where data starts its journey)
- `store` - Your data destination (where data settles)
- `defaults` - Settings that apply to all entities in this flow

### Step 3: Define Your Entities

Entities are the specific tables or datasets you want to move. Edit `flows/example_flow/entities/users.yml`:

```yaml
name: "users"
columns:
  - id
  - name
  - email
  - created_at
```

**For multiple entities**, create additional files in `flows/example_flow/entities/`:

- `users.yml`
- `orders.yml`
- `products.yml`

Each entity will run in parallel when you execute the flow.

### Step 4: Preview What Would Run (Optional)

See what flows would do before executing:

```bash
# Quick preview - one line per flow
hygge go --dry-run

# Detailed preview with full configuration
hygge go --dry-run --verbose
```

**What this shows:**

- Which flows/entities would run
- Source → destination mapping
- Incremental vs full load mode
- Configuration warnings

**Example output:**

```
🏡 hygge dry-run preview

Would run 2 flow(s)

✓ example_flow_users      parquet → parquet (incremental)
✓ example_flow_orders     parquet → parquet (full load)

📊 Summary:
   ✓ 2 flow(s) configured

💡 Next steps:
   • Test connections: hygge debug
   • Run flows: hygge go
```

### Step 5: Validate Connections

Test that hygge can connect to your sources and destinations:

```bash
hygge debug
```

**What this checks:**

- Configuration file validity
- Database connections (if configured)
- File paths exist and are accessible
- Provides actionable guidance for any issues

### Step 6: Run Your Flows

Execute all flows in your project:

```bash
hygge go  # Run all flows
```

**Common options:**
- `--flow NAME` - Run specific flow(s)
- `--incremental` - Append data instead of truncating
- `--verbose` - Detailed progress information

See the [CLI Commands](#cli-commands) section for complete usage details.

**What happens:**

- All flows run in parallel
- Each entity processes independently
- Progress is shown for each flow
- Results are logged with success/failure status

**Expected output:**

```text
Starting all flows...
[1 of 2] FINISHED flow example_flow completed in 2.3s (1,234 rows)
[2 of 2] FINISHED flow another_flow completed in 4.5s (5,678 rows)

Finished running 2 flows in 6.80 seconds.
Completed successfully
Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
Total rows processed: 6,912
Overall rate: 1,016 rows/s

All flows completed successfully!
```

### Next Steps

Once your first flow is working:

1. **Add more entities** - Create additional entity files in `flows/example_flow/entities/`
2. **Connect to databases** - Configure SQL Server or other database connections in `hygge.yml`
3. **Use cloud storage** - Configure Azure Data Lake or Microsoft Fabric destinations
4. **Enable incremental processing** - Set up watermarks for efficient incremental loads

See the [Common Workflows](#common-workflows) section below for more detailed examples.

## CLI Commands

### `hygge init PROJECT_NAME`

Create a cozy new hygge project:

```bash
hygge init my-project                    # Create new project
hygge init my-project --flows-dir pipelines  # Custom flows directory
hygge init my-project --force            # Overwrite existing project
```

Sets up a comfortable project structure with example configuration files to get you started.

### `hygge go`

Let your data flow comfortably through all your flows:

```bash
# Run all flows
hygge go

# Run specific flows (comma-separated)
hygge go --flow users_to_lake,orders_to_lake

# Or use multiple flags
hygge go --flow users_to_lake --flow orders_to_lake

# Run specific entities within a flow (comma-separated)
hygge go --entity salesforce.Involvement,salesforce.Account

# Or use multiple flags
hygge go --entity salesforce.Involvement --entity salesforce.Account

# Override run type (incremental or full-drop)
hygge go --incremental    # Append data instead of truncating
hygge go --full-drop      # Truncate destination before loading

# Override flow configuration
hygge go --var flow.mssql_to_mirrored_db.full_drop=true

# Control concurrency
hygge go --concurrency 4

# Enable verbose logging
hygge go --verbose
```

**Flow filtering:**
- `--flow` accepts base flow names (e.g., `salesforce`) or entity flow names (e.g., `salesforce_Involvement`)
- `--entity` uses format `flow.entity` (e.g., `salesforce.Involvement`)
- Both support comma-separated values: `--flow flow1,flow2,flow3` OR multiple flags: `--flow flow1 --flow flow2`

**Run type overrides:**
- `--incremental`: Append data to destination (uses journal + watermarks)
- `--full-drop`: Truncate destination before loading
- Cannot specify both flags together

**Flow overrides (`--var`):**
- Format: `flow.<flow_name>.field=value`
- Example: `flow.users_to_lake.store.compression=snappy`
- Supports nested field paths for advanced overrides

Runs all flows in parallel, keeping you informed with cozy progress updates and results.

### `hygge debug`

Take a cozy look at your project configuration:

```bash
hygge debug              # Shows project details and discovered flows
```

Use this to make sure everything feels right before running your flows.

## Design Principles

1. **Comfort Over Complexity**
   - APIs should feel natural and intuitive
   - Configuration should be simple but flexible
   - Defaults should "just work"

2. **Flow Over Force**
   - Data should move smoothly between systems
   - Batching and buffering should happen naturally
   - Progress should be visible but unobtrusive

3. **Reliability Over Speed**
   - Prefer robust, predictable behavior
   - Handle errors gracefully
   - Make recovery simple

4. **Clarity Over Cleverness**
   - Simple, clear code over complex optimizations
   - Explicit configuration over implicit behavior
   - Clear logging and progress tracking

## Core Concepts

hygge organizes data movement around three simple concepts:

### Home → Flow → Store

- **Home**: Where data feels at home (source - database, parquet files, etc.)
- **Store**: Where data settles comfortably (destination - parquet, Azure, Fabric, etc.)
- **Flow**: The cozy journey from home to store (configured in YAML, executed via CLI)

All configuration happens in YAML files. You define flows, and hygge handles the execution:

```bash
# Define your flows in YAML, then run them
hygge go
```

See the `samples/` directory for complete configuration examples.

## Supported Data Sources & Destinations

### Data Sources (Homes)

Where your data feels at home:

**Parquet Files:**

```yaml
home:
  type: parquet
  path: data/source
```

**MS SQL Server:**

Configure connections in `hygge.yml`:

```yaml
# hygge.yml
connections:
  my_database:
    type: mssql
    server: myserver.database.windows.net
    database: mydatabase
    pool_size: 8
```

Then define flows in `flows/<flow_name>/flow.yml`:

```yaml
# flows/users_flow/flow.yml
name: users_flow
home:
  type: mssql
  connection: my_database
  table: dbo.users
store:
  type: parquet
  path: data/users
```

**Features:**

- Azure AD authentication (Managed Identity, Azure CLI, Service Principal)
- Connection pooling for efficient concurrent access
- Entity pattern for extracting 10-200+ tables
- Watermark-aware incremental reads

**Prerequisites:**

- ODBC Driver 18 for SQL Server (`brew install msodbcsql18` on macOS)
- Azure AD authentication configured

### Data Destinations (Stores)

Where your data settles comfortably:

**Parquet Files:**

```yaml
store:
  type: parquet
  path: data/destination
```

**Microsoft Fabric Open Mirroring:**

```yaml
store:
  type: open_mirroring
  account_url: https://onelake.dfs.fabric.microsoft.com
  filesystem: my-workspace
  mirror_name: my-mirror
  key_columns: ["id"]
```

For `full_drop` runs, hygge deletes the LandingZone folder to trigger Open Mirroring to drop the table, then waits for Open Mirroring to process the deletion before writing new data. The wait time is configurable:

```yaml
store:
  type: open_mirroring
  folder_deletion_wait_seconds: 180  # Wait 3 minutes (default: 120s)
```

This can also be set per entity for tables that need more time:

```yaml
entities:
  - name: LargeTable
    store:
      folder_deletion_wait_seconds: 300  # 5 minutes for large table
```

**Azure Data Lake Storage (ADLS Gen2):**

```yaml
store:
  type: adls
  account_url: https://mystorage.dfs.core.windows.net
  filesystem: my-container
  credential: managed_identity
```

See `samples/` directory for complete configuration examples.

## Common Workflows

### Moving Data from SQL Server to Parquet

A cozy workflow to get your data moving:

1. **Create connection configuration** in `hygge.yml`:

```yaml
connections:
  my_database:
    type: mssql
    server: myserver.database.windows.net
    database: mydatabase
    pool_size: 8
```

2. **Define your flow** in `flows/users_to_parquet/flow.yml`:

```yaml
name: users_to_parquet
home:
  type: mssql
  connection: my_database
  table: dbo.users
store:
  type: parquet
  path: data/users
```

3. **Run it**:

```bash
hygge go
```

### Moving Data to Microsoft Fabric

Get your data comfortably settled in Fabric:

1. **Configure Open Mirroring store** in your flow:

```yaml
# flows/my_flow/flow.yml
name: my_flow
store:
  type: open_mirroring
  account_url: https://onelake.dfs.fabric.microsoft.com
  filesystem: my-workspace
  mirror_name: my-mirror
  key_columns: ["id"]
```

2. **Run your flow**:

```bash
hygge go
```

hygge automatically handles all the cozy details - metadata files, schema manifests, and atomic operations - so your data feels right at home in Fabric.

For `full_drop` runs, hygge safely extracts data to a staging area first, then deletes the LandingZone folder (triggering Open Mirroring to drop the table), waits for Open Mirroring to process (~2 minutes by default), and moves the new data into place. If extraction fails, the existing table is untouched.

```yaml
# flows/my_flow/flow.yml
run_type: full_drop
store:
  type: open_mirroring
  account_url: https://onelake.dfs.fabric.microsoft.com
  filesystem: my-workspace
  mirror_name: my-mirror
  key_columns: ["id"]
  folder_deletion_wait_seconds: 120  # Default: 2 minutes
```

## Extensibility

Create your own cozy homes and stores by implementing the `Home` and `Store` interfaces. hygge automatically discovers and welcomes them, making them feel right at home in your YAML configurations.

## Incremental vs Full-Drop at a Glance

hygge flows now coordinate run strategy with the store. By default the store follows the flow’s `run_type` (`incremental` vs `full_drop`). You can override that behaviour per store:

```yaml
store:
  type: open_mirroring
  account_url: https://onelake.dfs.fabric.microsoft.com
  filesystem: my-workspace
  mirror_name: my-mirror
  incremental: false  # force truncate-and-reload even if the flow is incremental
```

| Flow `run_type` | Store `incremental` | Behaviour |
| --- | --- | --- |
| `incremental` | omitted / `null` | Append via journal + watermark |
| `incremental` | `true` | Append (explicit opt-in) |
| `incremental` | `false` | Force truncate-and-reload each run |
| `full_drop` | omitted / `null` | Truncate destination before reload |
| `full_drop` | `true` | Force append even on full-drop runs (use with care) |
| `full_drop` | `false` | Truncate (explicit opt-in) |

This alignment keeps the flow, store, and journal in sync and prevents accidental mixes of append/truncate semantics.

## Concurrency

hygge runs multiple entity flows in parallel, controlled by the `concurrency` setting:

```yaml
# hygge.yml
options:
  concurrency: 8  # Up to 8 flows run simultaneously
```

```bash
# Or override from CLI
hygge go --concurrency 4
```

**Smart concurrency for `full_drop` flows:** When a `full_drop` flow finishes extracting data, it releases its concurrency slot immediately — even if it's still waiting for Open Mirroring to process a folder deletion. This means other entities can start extracting data during that wait time instead of sitting idle. For projects with many entities, this can save significant time per run.

## Development Philosophy

- Keep it simple and cozy
- Make common tasks feel effortless
- Make complex tasks comfortable
- Prioritize a warm, welcoming user experience
- Write clear, maintainable code that feels good to read
- Test thoroughly but sensibly

hygge isn't just about moving data - it's about making data movement feel natural, comfortable, and reliable. Like a warm blanket for your data pipelines.
