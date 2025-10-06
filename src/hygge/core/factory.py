"""
Factory for creating Home and Store instances.

The factory pattern allows us to:
- Centralize component creation logic
- Support dependency injection
- Make testing easier with mock components
- Extend with new component types easily
"""
from typing import Dict, Optional, Type

from ..homes.parquet import ParquetHome
from ..stores.parquet import ParquetStore
from .home import Home, HomeConfig
from .store import Store, StoreConfig


class Factory:
    """
    Factory for creating Home and Store instances.

    This factory centralizes the creation of Home and Store instances,
    making it easier to extend with new types and test with mocks.
    """

    def __init__(self):
        """Initialize factory with component registries."""
        self._home_types: Dict[str, Type[Home]] = {
            "parquet": ParquetHome,
        }

        self._store_types: Dict[str, Type[Store]] = {
            "parquet": ParquetStore,
        }

    def create_home(self, name: str, config: HomeConfig) -> Home:
        """
        Create a Home instance from configuration.

        Args:
            name: Name for the home instance
            config: Home configuration

        Returns:
            Home instance

        Raises:
            ValueError: If home type is not supported
        """
        home_type = config.type

        if home_type not in self._home_types:
            raise ValueError(f"Unsupported home type: {home_type}")

        home_class = self._home_types[home_type]
        return home_class(name, config)

    def create_store(
        self, name: str, config: StoreConfig, flow_name: Optional[str] = None
    ) -> Store:
        """
        Create a Store instance from configuration.

        Args:
            name: Name for the store instance
            config: Store configuration
            flow_name: Optional flow name for file naming patterns

        Returns:
            Store instance

        Raises:
            ValueError: If store type is not supported
        """
        store_type = config.type

        if store_type not in self._store_types:
            raise ValueError(f"Unsupported store type: {store_type}")

        store_class = self._store_types[store_type]
        return store_class(name, config, flow_name)

    def register_home_type(self, home_type: str, home_class: Type[Home]) -> None:
        """
        Register a new home type.

        Args:
            home_type: Type identifier
            home_class: Home class to register
        """
        self._home_types[home_type] = home_class

    def register_store_type(self, store_type: str, store_class: Type[Store]) -> None:
        """
        Register a new store type.

        Args:
            store_type: Type identifier
            store_class: Store class to register
        """
        self._store_types[store_type] = store_class

    def get_supported_home_types(self) -> list[str]:
        """Get list of supported home types."""
        return list(self._home_types.keys())

    def get_supported_store_types(self) -> list[str]:
        """Get list of supported store types."""
        return list(self._store_types.keys())
