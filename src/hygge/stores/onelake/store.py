"""
OneLake store implementation for Microsoft Fabric Lakehouse.

Lightweight wrapper around ADLSStore that adds Fabric-specific path conventions
for Lakehouse tables: Files/{entity}/ or Files/{schema}.schema/{entity}/

For Mirrored Databases (Open Mirroring), use OpenMirroringStore instead.
"""
from typing import Optional

from pydantic import Field, model_validator

from hygge.stores.adls import ADLSStore, ADLSStoreConfig


class OneLakeStoreConfig(ADLSStoreConfig, config_type="onelake"):
    """
    Configuration for OneLake store (Fabric Lakehouse).

    Extends ADLSStoreConfig with Fabric-specific path conventions for Lakehouse.

    Examples:

        Basic Lakehouse:
        ```yaml
        store:
          type: onelake
          account_url: ${ONELAKE_ACCOUNT_URL}
          filesystem: MyLake
          # path automatically becomes: Files/{entity}/
          credential: managed_identity
        ```

        Lakehouse with Schema:
        ```yaml
        store:
          type: onelake
          account_url: ${ONELAKE_ACCOUNT_URL}
          filesystem: MyLake
          schema: dbo
          # path automatically becomes: Files/{schema}.schema/{entity}/
          credential: managed_identity
        ```

    Note: For Mirrored Databases (Open Mirroring), use type: open_mirroring instead.
    """

    type: str = Field(default="onelake", description="Store type")
    path: Optional[str] = Field(
        None, description="Base path for data files (auto-built if None)"
    )

    # Optional: Schema support
    schema: Optional[str] = Field(
        None,
        description=(
            "Schema name for organizing tables in Lakehouse. "
            "Creates paths like Files/{schema}.schema/{entity}/"
        ),
    )

    @model_validator(mode="after")
    def build_lakehouse_path(self):
        """Build the base path for Fabric OneLake Lakehouse."""
        # If custom path is provided, use it as-is
        if self.path is not None:
            return self

        # Build Lakehouse path (always uses Files/ prefix)
        # Get schema value safely (avoiding Pydantic's schema method shadowing)
        # Access field value via __dict__ to bypass method resolution
        schema_value = self.__dict__.get("schema", None)

        if schema_value:
            # With schema: Files/{schema}.schema/{entity}/
            self.path = f"Files/{schema_value}.schema/{{entity}}/"
        else:
            # Without schema: Files/{entity}/
            self.path = "Files/{entity}/"

        return self


class OneLakeStore(ADLSStore, store_type="onelake"):
    """
    OneLake data store for Microsoft Fabric Lakehouse.

    Lightweight wrapper around ADLSStore that adds Fabric-specific path conventions
    for Lakehouse tables.

    Example:
        ```python
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            credential="managed_identity"
        )
        store = OneLakeStore("test_store", config, entity_name="users")
        ```
    """

    def __init__(
        self,
        name: str,
        config: OneLakeStoreConfig,
        flow_name: Optional[str] = None,
        entity_name: Optional[str] = None,
    ):
        # Let ADLSStore handle all the initialization
        super().__init__(name, config, flow_name, entity_name)
