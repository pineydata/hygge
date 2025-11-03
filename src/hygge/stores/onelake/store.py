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

    def _get_schema_value(self) -> Optional[str]:
        """
        Get schema field value safely, avoiding Pydantic's schema method shadowing.

        The 'schema' field name shadows Pydantic's built-in schema() method.
        This helper accesses the field value directly via __dict__ to bypass
        method resolution.

        Returns:
            Schema name string if set, None otherwise
        """
        # Access field value via __dict__ to bypass method resolution
        # Check if schema is in __dict__ and is a string (field value, not method)
        if "schema" in self.__dict__:
            schema_value = self.__dict__["schema"]
            # Skip if it's callable (the Pydantic schema() method)
            if callable(schema_value):
                # This is the method, not the field value - skip to fallback
                pass
            elif isinstance(schema_value, str):
                # It's the actual field value (string schema name)
                return schema_value
            elif schema_value is None:
                # Explicitly set to None
                return None
            else:
                # Unexpected type - return None
                return None

        # Fallback: use model_dump() to get actual field values
        # This properly handles Pydantic field access and avoids method shadowing
        try:
            dumped = self.model_dump()
            if "schema" in dumped:
                schema_value = dumped["schema"]
                if isinstance(schema_value, str):
                    return schema_value
                # Explicitly None or not set
                return None
        except Exception:
            # If model_dump fails, return None
            pass

        return None

    @model_validator(mode="after")
    def build_lakehouse_path(self):
        """Build the base path for Fabric OneLake Lakehouse."""
        # If custom path is provided, use it as-is
        if self.path is not None:
            return self

        # Build Lakehouse path (always uses Files/ prefix)
        schema_value = self._get_schema_value()

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
