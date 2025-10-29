"""
Integration test for ADLS Gen2 store.

Tests writing data to Azure Data Lake Storage Gen2 (works for both standard
ADLS accounts and Fabric OneLake).

Prerequisites:
1. Azure ADLS Gen2 account OR Fabric OneLake
2. Environment variables set (see requirements below)
3. Azure credentials (managed identity, service principal, or storage key)

To run:
```bash
# Set required environment variables:
export ADLS_ACCOUNT_URL="https://mystorageaccount.dfs.core.windows.net"
export ADLS_FILESYSTEM="mycontainer"
export ADLS_TEST_PATH="test/{entity}/"

# For managed identity (Azure VM):
pytest tests/integration/test_adls_store.py

# For service principal:
export ADLS_CREDENTIAL="service_principal"
export TENANT_ID="your-tenant-id"
export CLIENT_ID="your-client-id"
export CLIENT_SECRET="your-secret"

# For storage key:
export ADLS_CREDENTIAL="storage_key"
export ADLS_STORAGE_KEY="your-key"
```
"""
import os

import polars as pl
import pytest

from hygge.stores.adls import ADLSStore, ADLSStoreConfig


@pytest.mark.skipif(
    not os.getenv("ADLS_ACCOUNT_URL") or not os.getenv("ADLS_FILESYSTEM"),
    reason="ADLS credentials not configured - set ADLS_ACCOUNT_URL and ADLS_FILESYSTEM",
)
@pytest.mark.asyncio
async def test_adls_store_write(tmp_path):
    """Test writing data to ADLS Gen2."""
    # Create test data
    test_data = pl.DataFrame(
        {
            "id": list(range(1, 101)),  # 100 rows
            "name": [f"User_{i}" for i in range(1, 101)],
            "email": [f"user{i}@test.com" for i in range(1, 101)],
            "value": [i * 1.5 for i in range(1, 101)],
        }
    )

    # Parse environment variables
    account_url = os.getenv("ADLS_ACCOUNT_URL")
    filesystem = os.getenv("ADLS_FILESYSTEM")
    test_path = os.getenv("ADLS_TEST_PATH", "test/{entity}/")
    credential = os.getenv("ADLS_CREDENTIAL", "managed_identity")

    # Build config with optional credentials
    config_data = {
        "account_url": account_url,
        "filesystem": filesystem,
        "path": test_path,
        "credential": credential,
        "batch_size": 20,  # Small batches to test multiple uploads
    }

    # Add service principal credentials if provided
    if credential == "service_principal":
        config_data["tenant_id"] = os.getenv("TENANT_ID")
        config_data["client_id"] = os.getenv("CLIENT_ID")
        config_data["client_secret"] = os.getenv("CLIENT_SECRET")

    # Add storage key if provided
    if credential == "storage_key":
        config_data["storage_account_key"] = os.getenv("ADLS_STORAGE_KEY")

    # Create store
    store_config = ADLSStoreConfig(**config_data)
    store = ADLSStore("test_adls", store_config, entity_name="test_users")

    # Write test data
    total_rows = 0
    batch_count = 0

    # Simulate streaming data (write in batches)
    for i in range(0, len(test_data), 30):  # Batches of 30
        batch = test_data.slice(i, 30)
        await store.write(batch)
        total_rows += len(batch)
        batch_count += 1

    await store.close()

    # Verify
    assert batch_count > 0, "No batches written"
    assert total_rows == 100, f"Expected 100 rows, got {total_rows}"
    assert len(store.uploaded_files) > 0, "No files uploaded"

    print(f"✓ Successfully wrote {total_rows} rows in {batch_count} batches")
    print(f"✓ Uploaded {len(store.uploaded_files)} files to ADLS")


@pytest.mark.skipif(
    not os.getenv("ADLS_ACCOUNT_URL") or not os.getenv("ADLS_FILESYSTEM"),
    reason="ADLS credentials not configured",
)
@pytest.mark.asyncio
async def test_adls_store_with_entity_pattern(tmp_path):
    """Test ADLS store with entity pattern (multiple entities)."""

    # Create multiple entity datasets
    entities_data = {
        "users": pl.DataFrame(
            {"id": range(1, 51), "name": [f"user_{i}" for i in range(50)]}
        ),
        "orders": pl.DataFrame(
            {"id": range(1, 51), "amount": [i * 10 for i in range(50)]}
        ),
    }

    account_url = os.getenv("ADLS_ACCOUNT_URL")
    filesystem = os.getenv("ADLS_FILESYSTEM")
    test_path = os.getenv("ADLS_TEST_PATH", "test/{entity}/")
    credential = os.getenv("ADLS_CREDENTIAL", "managed_identity")

    config_data = {
        "account_url": account_url,
        "filesystem": filesystem,
        "path": test_path,
        "credential": credential,
    }

    # Write each entity
    for entity_name, data in entities_data.items():
        store_config = ADLSStoreConfig(**config_data)
        store = ADLSStore(f"test_{entity_name}", store_config, entity_name=entity_name)

        await store.write(data)
        await store.close()

        print(f"✓ Uploaded {len(data)} rows for entity: {entity_name}")
