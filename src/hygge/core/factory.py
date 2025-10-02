"""
Factory for creating Home and Store instances from configurations.

This factory follows the Rails-inspired principle of "Convention over Configuration"
by providing sensible defaults and clean instantiation patterns.
"""
from typing import Dict, Type

from .home import Home
from .homes import ParquetHome, SQLHome
from .store import Store
from .stores import ParquetStore


class HyggeFactory:
    """
    Factory for creating Home and Store instances.

    This factory encapsulates the type mapping and instantiation logic,
    making it easy to extend with new Home/Store types and keeping
    the Coordinator focused on orchestration.
    """

    # Type mappings - easily extensible
    HOME_TYPES: Dict[str, Type[Home]] = {
        'sql': SQLHome,
        'parquet': ParquetHome
    }

    STORE_TYPES: Dict[str, Type[Store]] = {
        'parquet': ParquetStore
    }

    @classmethod
    def create_home(cls, name: str, config) -> Home:
        """
        Create a Home instance from configuration.

        Args:
            name: Name of the home instance
            config: Home configuration (HomeConfig or subclass)

        Returns:
            Home: Configured Home instance

        Raises:
            ValueError: If home type is not supported
        """
        home_type = config.type
        home_class = cls.HOME_TYPES.get(home_type)

        if not home_class:
            available_types = ', '.join(cls.HOME_TYPES.keys())
            raise ValueError(
                f"Unknown home type: {home_type}. "
                f"Available types: {available_types}"
            )

        return home_class(name=name, config=config)

    @classmethod
    def create_store(cls, name: str, flow_name: str, config) -> Store:
        """
        Create a Store instance from configuration.

        Args:
            name: Name of the store instance
            flow_name: Name of the flow (used by some store types)
            config: Store configuration (StoreConfig or subclass)

        Returns:
            Store: Configured Store instance

        Raises:
            ValueError: If store type is not supported
        """
        store_type = config.type
        store_class = cls.STORE_TYPES.get(store_type)

        if not store_class:
            available_types = ', '.join(cls.STORE_TYPES.keys())
            raise ValueError(
                f"Unknown store type: {store_type}. "
                f"Available types: {available_types}"
            )

        return store_class(name=name, config=config, flow_name=flow_name)

    @classmethod
    def register_home_type(cls, type_name: str, home_class: Type[Home]) -> None:
        """
        Register a new Home type.

        Args:
            type_name: Name of the home type
            home_class: Home class to register

        Example:
            HyggeFactory.register_home_type('bigquery', BigQueryHome)
        """
        cls.HOME_TYPES[type_name] = home_class

    @classmethod
    def register_store_type(cls, type_name: str, store_class: Type[Store]) -> None:
        """
        Register a new Store type.

        Args:
            type_name: Name of the store type
            store_class: Store class to register

        Example:
            HyggeFactory.register_store_type('bigquery', BigQueryStore)
        """
        cls.STORE_TYPES[type_name] = store_class

    @classmethod
    def get_supported_home_types(cls) -> list[str]:
        """Get list of supported home types."""
        return list(cls.HOME_TYPES.keys())

    @classmethod
    def get_supported_store_types(cls) -> list[str]:
        """Get list of supported store types."""
        return list(cls.STORE_TYPES.keys())
