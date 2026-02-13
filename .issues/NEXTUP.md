# What's Next Up - Cloud-Aware ParquetHome

**Status:** Ready to start
**Horizon:** 2 (New Homes)
**Priority:** GA360 support for marcomm team
**Effort:** 2-3 sessions

---

## The Priority

1. **Auth System** ← Foundation first (named credentials only, everywhere)
2. **Cloud-Aware ParquetHome** - uses auth
3. **BigQuery Home** (separate - database, not file storage)
4. **StagedWriteMixin** (for Stores, see reference below)
5. **Cloud-Aware Stores** (GCS, ADLS, S3 - apply same pattern)
6. **Migrate Connections** - Remove inline auth, use named credentials

---

## Auth System - Foundation First

**Auth is foundational** - connections, homes, and stores all need it. It should be top-level, not embedded in each component.

### Research Summary

Studied: dbt, Airflow, Dagster, Prefect, DuckDB, Dask/fsspec, Spark

**Common patterns across all:**
- Named credentials (define once, reference by name)
- Explicit `type` + `method` (never inferred)
- Separation of concerns (auth separate from data movement)
- Registry pattern (credentials registered centrally)

### Architecture

```
src/hygge/
├── auth/                    # NEW - foundational auth layer
│   ├── __init__.py
│   ├── base.py              # BaseCredential interface
│   ├── registry.py          # Credential registry + factory
│   ├── gcs.py               # GCS credentials (type: gcs)
│   ├── s3.py                # S3 credentials (type: s3)
│   └── azure_ad.py          # Azure AD credentials (type: azure_ad)
│                            # Used by: MSSQL connections, ADLS homes/stores
├── connections/             # USES auth/ - named credentials only
│   ├── mssql.py             # References azure_ad credential
├── homes/                   # USES auth/ for cloud storage
└── stores/                  # USES auth/ for cloud storage
```

**Design Principle:** Named credentials only, everywhere. No inline auth in homes, stores, or connections. Auth is a cross-cutting concern that doesn't belong embedded in data movement config.

### Config with named credentials

```yaml
# hygge.yml - ALL auth lives here, nowhere else
credentials:
  # GCS - start with service_account, scaffolding for more methods
  gcp:
    type: gcs
    method: service_account      # Explicit method (dbt-style)
    keyfile: ${GCP_SA_JSON_PATH}
    project: ${GCP_PROJECT_ID}   # Optional, usually in keyfile

  # ADLS - start with default (DefaultAzureCredential)
  azure:
    type: azure_ad
    method: default              # Uses DefaultAzureCredential chain
    # Note: DefaultAzureCredential auto-scopes tokens for SQL vs Storage

  # S3 - start with default (boto3 credential chain)
  aws:
    type: s3
    method: default
    region: ${AWS_REGION}        # Optional, can be inferred

# Connections reference credentials by name (no inline auth)
connections:
  salesforce_db:
    type: mssql
    server: myserver.database.windows.net
    database: salesforce
    credential: azure            # References credential above

# Flows reference credentials by name
flows:
  ga360_raw:
    home:
      type: parquet
      provider: gcs
      credential: gcp            # Reference, not inline auth
      bucket: my-ga360-exports
      path: ga_sessions/*.parquet
    store:
      type: adls
      credential: azure          # Same credential system for stores
      account: mystorageaccount
      container: data
      path: ga360/
```

### Anonymous/Public Access

For public buckets, omit the `credential` field:

```yaml
home:
  type: parquet
  provider: gcs
  bucket: public-data            # No credential = anonymous access
  path: open-datasets/*.parquet
```

Validation: If `credential` is omitted, fail fast on first read if the bucket isn't actually public.

### Auth Methods - Scaffolded for Future

**Phase 1 (now):** One method per provider to start
**Future:** Add methods as needed

| Provider | Phase 1 Method | Future Methods |
|----------|---------------|----------------|
| **GCS** | `service_account` | `adc`, `anon` |
| **S3** | `default` | `access_key`, `iam_role`, `profile` |
| **Azure AD** | `default` | `managed_identity`, `service_principal`, `cli` |

**Note on Azure AD:** Use `azure_ad` type for both MSSQL connections and ADLS storage. `DefaultAzureCredential` automatically scopes tokens appropriately (SQL Database scope vs Storage scope). This is handled by the underlying Azure SDK - no user action needed.

### Environment Variable Support

Credentials support `${VAR}` expansion:

```yaml
credentials:
  gcp_prod:
    type: gcs
    method: service_account
    keyfile: ${GCP_SA_JSON_PATH}    # Expanded at load time
```

### Design Decision: Named Credentials Only

**No inline auth anywhere** - not in homes, not in stores, not in connections.

**Why this is the right design:**
1. **Auth is a cross-cutting concern** - Doesn't belong embedded in data movement config
2. **DRY** - Define once, use in 10 flows
3. **Single point of validation** - `hygge debug` tests all auth in one pass
4. **Credential rotation** - Change one place, not 15
5. **Separation of concerns** - Flow configs describe data movement, not auth mechanics
6. **Consistent mental model** - Same pattern everywhere

**Migration:** Existing MSSQL connections with inline `authentication` will need to be updated to use named credentials. This is a breaking change, but the right design.

### `hygge debug` Credential Validation

**Actually tests credentials** (makes API calls):

```bash
$ hygge debug

🔍 Checking credentials...
  ✓ gcp: GCS service_account - authenticated as hygge-sa@project.iam
  ✓ azure: Azure AD default - authenticated via DefaultAzureCredential
  ✗ aws: S3 default - NoCredentialsError (check AWS config)

🔗 Checking connections...
  ✓ salesforce_db: MSSQL - connected to myserver.database.windows.net
```

**Focused credential check:**

```bash
$ hygge debug --credentials

🔍 Checking credentials...
  ✓ gcp: GCS service_account - authenticated
  ✗ azure: Azure AD default - authentication failed

  DefaultAzureCredential attempted these methods:
    1. Managed Identity: Not running in Azure (no MSI endpoint)
    2. Azure CLI: 'az' command not found
    3. Visual Studio Code: No VS Code Azure extension

  Suggestion: Run 'az login' or set AZURE_CLIENT_ID/AZURE_CLIENT_SECRET/AZURE_TENANT_ID
```

**Investment in error messages pays off.** Users hit auth errors constantly during setup. Good errors = happy users.

### Why top-level auth?

1. **DRY** - Define credentials once, use everywhere
2. **Separation of concerns** - Homes/Stores/Connections do data movement, not auth
3. **Testable** - `hygge debug` validates credentials with real API calls
4. **Secure** - Centralized credential management
5. **Reusable** - Same credential for Home, Store, and Connection
6. **Consistent** - Same pattern across all clouds and all component types
7. **Mid-market ready** - Scaffolding for multiple auth methods per provider
8. **Single point of rotation** - Change one credential, all flows update

### BaseCredential Interface

```python
class BaseCredential(ABC):
    """Base class for cloud credentials."""

    name: str           # Credential name from hygge.yml (e.g., "gcp", "azure")
    type: str           # gcs, s3, azure_ad
    method: str         # service_account, default, etc.

    @abstractmethod
    def get_storage_options(self) -> Dict[str, Any]:
        """Return storage_options dict for Polars/fsspec."""
        pass

    @abstractmethod
    async def validate(self) -> Tuple[bool, str]:
        """
        Validate credentials by making an API call.
        Returns (success, message) for hygge debug output.
        """
        pass
```

---

## The Insight

**Polars handles cloud storage natively** via `storage_options`:

```python
# One call works for all clouds
lf = pl.scan_parquet(path, storage_options={...})
```

This means we don't need separate `GCSHome`, `ADLSHome`, `S3Home` classes. We extend `ParquetHome` to be cloud-aware. **One class, all clouds, DRY.**

**Same principle applies to Stores later** - but that's a separate effort.

---

## The Design

### Config with explicit `provider` and `credential`

```yaml
# Local files (provider defaults to "local", no credential needed)
home:
  type: parquet
  path: data/users.parquet

# GCS - explicit provider, references GCS credential
home:
  type: parquet
  provider: gcs
  credential: gcp            # References GCS credential
  bucket: my-ga360-exports
  path: ga_sessions/*.parquet

# ADLS - explicit provider, references Azure AD credential
home:
  type: parquet
  provider: adls
  credential: azure          # References azure_ad credential
  account: mystorageaccount
  container: data
  path: users/*.parquet

# S3 - explicit provider, references S3 credential
home:
  type: parquet
  provider: s3
  credential: aws            # References S3 credential
  bucket: my-data-lake
  region: us-west-2          # Optional, can be inferred
  path: users/*.parquet
```

**No inline auth** - credentials are defined once in `hygge.yml`, referenced by name.

**Credential type mapping:**
| Provider | Valid Credential Types |
|----------|----------------------|
| `local` | None (no credential) |
| `gcs` | `gcs` |
| `adls` | `azure_ad` |
| `s3` | `s3` |

### What `provider` and `credential` enable

1. **Explicit** - config documents itself (Pythonic: explicit > implicit)
2. **Validation** - `provider: gcs` validates GCS-specific fields
3. **Path building** - constructs full URI from parts (bucket + path → `gs://bucket/path`)
4. **DRY auth** - credentials defined once, used everywhere
5. **Discoverability** - `provider: [local, gcs, adls, s3]` is a clear enum
6. **Separation** - data movement config separate from auth config

### Implementation

**One class** - extend existing `ParquetHome`:

```python
class ParquetHome(Home, home_type="parquet"):
    def __init__(self, name: str, config: "ParquetHomeConfig", credential: Optional[BaseCredential] = None):
        super().__init__(name, config.get_merged_options())
        self.config = config
        self.credential = credential  # Injected by Coordinator

    async def _get_batches(self) -> AsyncIterator[pl.DataFrame]:
        # Build full path from provider + bucket/container + path
        full_path = self.config.build_full_path()

        # Get storage_options from credential (if cloud provider)
        storage_options = self.credential.get_storage_options() if self.credential else {}

        # Polars handles all clouds natively
        lf = pl.scan_parquet(full_path, storage_options=storage_options)

        # ... existing batching logic unchanged
```

**Config class** - extend `ParquetHomeConfig`:

```python
from typing import Literal, Optional

class ParquetHomeConfig(HomeConfig, BaseHomeConfig, config_type="parquet"):
    # Existing fields
    path: str
    batch_size: int = 10_000

    # New fields for cloud support
    provider: Literal["local", "gcs", "adls", "s3"] = Field(default="local")
    credential: Optional[str] = Field(None, description="Name of credential from hygge.yml")

    # GCS/S3 fields
    bucket: Optional[str] = Field(None, description="Bucket name (GCS, S3)")
    project: Optional[str] = Field(None, description="GCP project (optional, usually in keyfile)")

    # ADLS fields
    container: Optional[str] = Field(None, description="Container name (ADLS)")
    account: Optional[str] = Field(None, description="Storage account (ADLS)")

    # S3-specific
    region: Optional[str] = Field(None, description="AWS region (optional, can be inferred)")

    @model_validator(mode="after")
    def validate_provider_fields(self):
        """Validate required fields based on provider."""
        # Credential required for non-local providers (unless anonymous access)
        # Anonymous access: credential is None AND provider supports it
        if self.provider == "gcs" and not self.bucket:
            raise ValueError("bucket is required for provider: gcs")
        if self.provider == "adls":
            if not self.credential:
                raise ValueError("credential is required for provider: adls (no anonymous access)")
            if not (self.account and self.container):
                raise ValueError("account and container are required for provider: adls")
        if self.provider == "s3" and not self.bucket:
            raise ValueError("bucket is required for provider: s3")
        return self

    def validate_credential_type(self, credential: "BaseCredential") -> None:
        """Validate credential type matches provider. Called by Coordinator."""
        valid_types = {
            "local": [],  # No credential needed
            "gcs": ["gcs"],
            "adls": ["azure_ad"],
            "s3": ["s3"],
        }
        expected = valid_types.get(self.provider, [])
        if expected and credential.type not in expected:
            raise ValueError(
                f"Credential '{credential.name}' (type: {credential.type}) "
                f"cannot be used with provider: {self.provider}. "
                f"Expected credential type: {expected}"
            )

    def build_full_path(self) -> str:
        """Build full URI from provider + bucket/container + path."""
        if self.provider == "local":
            return self.path
        elif self.provider == "gcs":
            return f"gs://{self.bucket}/{self.path}"
        elif self.provider == "adls":
            return f"abfss://{self.container}@{self.account}.dfs.core.windows.net/{self.path}"
        elif self.provider == "s3":
            return f"s3://{self.bucket}/{self.path}"
        raise ValueError(f"Unknown provider: {self.provider}")
```

---

## Implementation Outline

### Phase 0: Auth System Foundation (2-3 hours)

**Goal:** Create `src/hygge/auth/` with credential management and scaffolding

**Tasks:**
1. Create `src/hygge/auth/__init__.py`
2. Create `src/hygge/auth/base.py` - `BaseCredential` interface
3. Create `src/hygge/auth/registry.py` - credential registry and factory
4. Create `src/hygge/auth/gcs.py` - GCS credentials (start with `service_account` method)
5. Create `src/hygge/auth/s3.py` - S3 credentials (start with `default` method)
6. Create `src/hygge/auth/azure_ad.py` - Azure AD credentials (for MSSQL + ADLS)
7. Add `${VAR}` environment variable expansion support

**Scaffolding for future methods:**
```python
class GCSCredential(BaseCredential):
    SUPPORTED_METHODS = ["service_account"]  # Add more later: "adc", "anon"

    def __init__(self, method: str, **kwargs):
        if method not in self.SUPPORTED_METHODS:
            raise ValueError(f"GCS method '{method}' not yet supported. Use: {self.SUPPORTED_METHODS}")
```

**Tests:**
- Each credential type can be instantiated
- `get_storage_options()` returns valid dict
- `validate()` makes real API call and reports success/failure
- Unsupported methods raise helpful errors
- Environment variables expand correctly

### Phase 1: Credential Config Loading + Debug (1-1.5 hours)

**Goal:** Load credentials from `hygge.yml` and validate with `hygge debug`

**Tasks:**
1. Add `credentials:` section parsing to config loader
2. Create credential instances from config with `${VAR}` expansion
3. Make credentials available to Coordinator
4. Implement `hygge debug --credentials` for focused credential testing
5. Add detailed error messages for auth failures (see examples above)

**Tests:**
- Credentials load from YAML
- Environment variables expand
- Invalid credentials raise helpful errors
- `hygge debug --credentials` validates with real API calls
- Detailed error output for Azure AD auth chain failures

### Phase 2: Extend ParquetHomeConfig (30-45 min)

**Goal:** Add cloud support to config

**Tasks:**
1. Add `provider` field with `Literal["local", "gcs", "adls", "s3"]`
2. Add `credential` field (reference to named credential)
3. Add `bucket`, `container`, `account`, `region`, `project` fields
4. Add provider-specific validation
5. Add `validate_credential_type()` for cross-validation
6. Add `build_full_path()` method to construct URI

**Tests:**
- Config validation for each provider
- Path building for each provider
- Credential-provider type mismatch raises helpful error
- Anonymous access works for GCS/S3 public buckets

### Phase 3: Extend ParquetHome (30-45 min)

**Goal:** Use credentials for cloud access

**Tasks:**
1. Accept credential in constructor (injected by Coordinator)
2. Coordinator calls `config.validate_credential_type(credential)` before creating Home
3. Build full path using config's `build_full_path()`
4. Get `storage_options` from credential
5. Pass `storage_options` to `pl.scan_parquet()`
6. Handle cloud-specific errors gracefully with helpful messages

**Coordinator injection pattern:**
```python
# In Coordinator - credential is resolved and validated before Home creation
credential = self.registry.get(home_config.credential) if home_config.credential else None
if credential:
    home_config.validate_credential_type(credential)
home = ParquetHome(name, home_config, credential=credential)
```

**The change in ParquetHome is small** - most logic stays the same:

```python
# Before
lf = pl.scan_parquet(self.data_path)

# After
storage_options = self.credential.get_storage_options() if self.credential else {}
lf = pl.scan_parquet(self.full_path, storage_options=storage_options)
```

### Phase 4: Dependencies & Integration Testing (30-45 min)

**Goal:** Ensure cloud support works end-to-end

**Dependencies to add:**
- `gcsfs` - GCS filesystem support
- `adlfs` - ADLS filesystem support
- `s3fs` - S3 filesystem support
- `google-auth` - GCS authentication
- `azure-identity` - Azure AD authentication (for ADLS + MSSQL)
- `boto3` - S3 authentication

**Tests:**
- Unit tests with mocked cloud clients
- Config validation tests per provider
- Credential-provider type validation tests
- Integration test with real GCS bucket (requires credentials)
- Verify `hygge debug --credentials` output format

### Phase 5: Documentation (15 min)

**Goal:** Document cloud support and auth system

**Tasks:**
- Update README.md with cloud examples
- Document credential types and options
- Add config examples to samples/
- Update `hygge debug` help text

---

## Success Criteria

**Auth System:**
1. ✅ Credentials defined once in `hygge.yml`, reusable across flows and connections
2. ✅ `hygge debug` validates credentials with real API calls
3. ✅ `hygge debug --credentials` for focused credential testing
4. ✅ Clear, detailed errors for auth failures (especially Azure AD chain)
5. ✅ Environment variables (`${VAR}`) expand correctly
6. ✅ Scaffolding for future auth methods (graceful "not yet supported" errors)

**Named Credentials Only (Design Principle):**
7. ✅ No inline auth anywhere - homes, stores, connections all reference credentials
8. ✅ Credential-provider type mismatch caught with helpful error
9. ✅ Anonymous access works for public GCS/S3 buckets (credential omitted)

**Cloud-Aware ParquetHome:**
10. ✅ Existing local parquet configs work unchanged
11. ✅ `provider: gcs` + `credential: name` works with GCS buckets
12. ✅ `provider: adls` + `credential: name` works with Azure storage
13. ✅ `provider: s3` + `credential: name` works with S3 buckets
14. ✅ Config validation catches errors early
15. ✅ Unit tests pass

**Migration (Breaking Change):**
16. 📋 Existing MSSQL connections with inline auth need update to use named credentials

---

## After This: BigQuery Home

BigQuery is different - it's a database, not file storage. Needs its own Home class.

```yaml
home:
  type: bigquery
  project: my-gcp-project
  dataset: ga360_export
  table: events_*
  # Uses google-cloud-bigquery with Arrow export
```

**Key:** Use `google-cloud-bigquery` with Arrow export for zero-copy path to Polars.

---

## Reference: StagedWriteMixin (for Stores, later)

> **Note:** This is needed for cloud Stores, not Homes. Keeping here for reference.

### The Problem

Every cloud store duplicates staging/commit logic (~200 lines each × 4 stores = ~800 lines).

### The Solution

Extract `StagedWriteMixin` - same pattern we're applying to Homes:
- One place for staging logic
- Stores implement only store-specific parts

**This comes after cloud-aware Homes are working.**

---

## What Comes After

**Priority order (GA360 support):**

1. **Cloud-Aware ParquetHome** ← Current focus
2. **Migrate MSSQL Connections** - Remove inline auth, use named credentials (breaking change)
3. **BigQuery Home** - Read GA360 sessionized data
4. **StagedWriteMixin** - Extract staging pattern from stores
5. **Cloud-Aware Stores** - Apply same DRY pattern to stores

Your marcomm team is becoming a GA360 client. GA360 exports to:
- **BigQuery** (primary) - sessionized, processed analytics data
- **GCS** (optional) - raw hit-level exports

**The data flow:**

```
GA360 → BigQuery (processed data)
     → GCS (raw exports, optional)

hygge flows:
ParquetHome (provider: gcs) → Polars → existing Stores
BigQuery Home → Polars → wherever your data needs to go
```

---

## Notes

- **Auth is foundational** - Build it first, everything else uses it
- **Named credentials only** - No inline auth anywhere (breaking change for existing MSSQL connections)
- **Scaffolding for future** - One method per provider now, add more later
- **DRY** - Credentials defined once, one ParquetHome class for all clouds
- **Explicit** - `type`, `method`, `provider`, `credential` keys make config self-documenting
- **Validation that works** - `hygge debug` makes real API calls to test credentials
- **Cross-validation** - Credential type must match provider (GCS credential can't be used with ADLS)
- **Error messages matter** - Invest in detailed auth failure messages, especially for Azure AD
- **Environment variables** - `${VAR}` expansion for secrets
- **Separation of concerns** - Auth logic in `auth/`, data movement in Homes/Stores
- **Mid-market ready** - Design supports multiple auth methods per provider
- **Pythonic** - Explicit is better than implicit, use `Literal` for provider enum
- **Stand on cozy shoulders** - Polars + gcsfs/adlfs/s3fs + cloud SDKs do the heavy lifting
- **Coordinator injects credentials** - Homes don't know about the registry, they receive credentials

---

**Ready to start? Begin with Phase 0 - Auth System Foundation.**
