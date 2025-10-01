"""
Coordinator orchestrates data flows based on configuration.
"""
import asyncio
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from ..exceptions import ConfigError
from ..utility.logger import get_logger
from .flow import Flow
from .homes import ParquetHome, SQLHome


class Coordinator:
    """
    Coordinates multiple flows based on template configuration.

    Example config:
    ```yaml
    homes:
      users_sql:
        type: sql
        connection: ${DB_CONNECTION}
        options:
          table: users
          schema: dbo
          batch_size: 10000

      orders_parquet:
        type: parquet
        path: data/orders.parquet
        options:
          batch_size: 10000

    flows:
      users_to_lake:
        home: users_sql
        store: users_lake
        options:
          incremental: true
    ```
    """

    HOME_TYPES = {
        'sql': SQLHome,
        'parquet': ParquetHome
    }

    def __init__(
        self,
        config_path: str,
        options: Optional[Dict[str, Any]] = None
    ):
        self.config_path = Path(config_path)
        self.options = options or {}
        self.logger = get_logger("hygge.coordinator")
        self.homes = {}
        self.flows = []

    async def setup(self) -> None:
        """Load and validate configuration."""
        try:
            # Load config
            with open(self.config_path) as f:
                config = yaml.safe_load(f)

            # Set up homes
            await self._setup_homes(config.get('homes', {}))

            # Set up flows
            await self._setup_flows(config.get('flows', {}))

        except Exception as e:
            raise ConfigError(f"Failed to setup coordinator: {str(e)}")

    async def _setup_homes(self, homes_config: Dict[str, Any]) -> None:
        """Set up configured homes."""
        for name, config in homes_config.items():
            home_type = config.get('type')
            if not home_type:
                raise ConfigError(f"Home {name} missing type")

            home_class = self.HOME_TYPES.get(home_type)
            if not home_class:
                raise ConfigError(f"Unknown home type: {home_type}")

            self.logger.info(f"Setting up {home_type} home: {name}")

            if home_type == 'sql':
                home = home_class(
                    name=name,
                    connection=config['connection'],
                    options=config.get('options')
                )
            elif home_type == 'parquet':
                home = home_class(
                    name=name,
                    path=config['path'],
                    options=config.get('options')
                )

            self.homes[name] = home

    async def _setup_flows(self, flows_config: Dict[str, Any]) -> None:
        """Set up configured flows."""
        for name, config in flows_config.items():
            home_name = config.get('home')
            if not home_name:
                raise ConfigError(f"Flow {name} missing home")

            home = self.homes.get(home_name)
            if not home:
                raise ConfigError(f"Unknown home: {home_name}")

            store_name = config.get('store')
            if not store_name:
                raise ConfigError(f"Flow {name} missing store")

            # Store setup will be implemented similarly

            self.logger.info(f"Setting up flow: {name}")
            flow = Flow(
                home=home,
                store=None,  # Store will be added when implemented
                options=config.get('options')
            )
            self.flows.append(flow)

    async def start(self) -> None:
        """Start all configured flows."""
        self.logger.info(f"Starting coordinator with {len(self.flows)} flows")

        max_concurrent = self.options.get('max_concurrent', 3)

        async with asyncio.TaskGroup() as group:
            running = []

            for flow in self.flows:
                # Respect concurrency limits
                while len(running) >= max_concurrent:
                    done, running = await asyncio.wait(
                        running,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    # Handle any completed tasks
                    for task in done:
                        await task

                # Start new flow
                task = group.create_task(
                    self._run_flow(flow),
                    name=flow.name
                )
                running.append(task)

        self.logger.info("All flows completed successfully")

    async def _run_flow(self, flow: Flow) -> None:
        """Run a single flow with error handling."""
        try:
            await flow.start()
        except Exception as e:
            self.logger.error(f"Error in flow {flow.name}: {e}")
            if not self.options.get('continue_on_error', False):
                raise