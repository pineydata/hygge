"""
OneLake store implementation for Microsoft Fabric.

Lightweight wrapper around ADLSStore that adds Fabric-specific path conventions:
- Files/{entity}/ for Lakehouse
- Files/LandingZone/{entity}/ for Mirrored Databases

Extends ADLSStore to inherit all ADLS Gen2 functionality while adding
Fabric-specific path handling on top.
"""
from typing import Optional

from pydantic import Field, model_validator

from hygge.stores.adls import ADLSStore, ADLSStoreConfig


class OneLakeStoreConfig(ADLSStoreConfig, config_type="onelake"):
    """
    Configuration for OneLake store (Fabric-specific ADLS Gen2).

    Extends ADLSStoreConfig with Fabric-specific path conventions.

    Examples:

        Lakehouse (default):
        ```yaml
        store:
          type: onelake
          account_url: ${ONELAKE_ACCOUNT_URL}
          filesystem: MyLake
          # path automatically becomes: Files/{entity}/
          credential: managed_identity
        ```

        Mirrored Database:
        ```yaml
        store:
          type: onelake
          account_url: ${ONELAKE_ACCOUNT_URL}
          filesystem: MyLake
          mirror_name: MyMirrorName
          # path automatically becomes: Files/LandingZone/{entity}/
          credential: managed_identity
        ```
    """

    type: str = Field(default="onelake", description="Store type")
    path: Optional[str] = Field(
        None, description="Base path for data files (auto-built if None)"
    )

    # Path configuration - either specify mirror_name OR path
    mirror_name: Optional[str] = Field(
        None,
        description=(
            "Name of the Mirrored Database. "
            "If set, writes to Files/LandingZone/{entity}/. "
            "If not set (None), uses Files/{entity}/ for Lakehouse."
        ),
    )

    @model_validator(mode="after")
    def build_fabric_path(self):
        """Build the base path for Fabric OneLake."""
        # If custom path is provided, use it as-is
        if self.path is not None:
            return self

        # Build path based on mirror_name
        if self.mirror_name:
            # Mirrored DB: Files/LandingZone/{entity}/
            self.path = "Files/LandingZone/{entity}/"
        else:
            # Lakehouse: Files/{entity}/
            self.path = "Files/{entity}/"

        return self


class OneLakeStore(ADLSStore, store_type="onelake"):
    """
    OneLake data store for Microsoft Fabric.

    Lightweight wrapper around ADLSStore that adds Fabric-specific path conventions.

    Example:
        ```python
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
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
