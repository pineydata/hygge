"""
Coordinator orchestrates data flows based on configuration.

The coordinator's single responsibility is to:
1. Read and parse configuration templates
2. Orchestrate flows in parallel
3. Handle flow-level error management

Home and Store instantiation is delegated to Flow.
"""
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from hygge.utility.exceptions import ConfigError
from hygge.utility.logger import get_logger

from .configs import HyggeConfig
from .configs.settings import settings
from .factory import HyggeFactory
from .flow import Flow


def validate_config(config: Dict[str, Any]) -> List[str]:
    """Validate configuration using Pydantic models."""
    try:
        HyggeConfig.from_dict(config)
        return []  # No errors
    except Exception as e:
        # Convert Pydantic validation errors to user-friendly messages
        if hasattr(e, 'errors'):
            errors = []
            for error in e.errors():
                field = '.'.join(str(x) for x in error['loc'])
                message = error['msg']
                errors.append(f"{field}: {message}")
            return errors
        else:
            return [str(e)]


def _apply_flow_settings(flow_config: Dict[str, Any]) -> Dict[str, Any]:
    """Apply flow-level settings to configuration."""
    # Get existing options or start with empty dict
    existing_options = flow_config.get('options', {})

    # Apply centralized settings
    flow_options = settings.apply_flow_settings(existing_options)

    # Return updated config with settings applied
    config_with_settings = flow_config.copy()
    config_with_settings['options'] = flow_options
    return config_with_settings




class Coordinator:
    """
    Coordinates multiple flows based on template configuration.

    The coordinator reads configuration and creates flows, using the
    HyggeFactory to instantiate Home and Store instances.

    Example config:
    ```yaml
    flows:
      users_to_lake:
        home:
          type: parquet
          path: data/users.parquet
          options:
            batch_size: 10000
        store:
          type: parquet
          path: data/lake/users
          options:
            batch_size: 100000
            compression: snappy
        options:
          queue_size: 5
    ```
    """

    def __init__(
        self,
        config_path: str,
        options: Optional[Dict[str, Any]] = None
    ):
        self.config_path = Path(config_path)
        self.options = options or {}
        self.logger = get_logger("hygge.coordinator")
        self.flows: List[Flow] = []

    async def setup(self) -> None:
        """Load and validate configuration."""
        try:
            # Load config
            with open(self.config_path) as f:
                config = yaml.safe_load(f)

            # Validate configuration
            errors = validate_config(config)
            if errors:
                error_msg = (
                    "Configuration validation failed:\n" +
                    "\n".join(f"  - {error}" for error in errors)
                )
                raise ConfigError(error_msg)

            # Set up flows - each flow handles its own home/store instantiation
            await self._setup_flows(config.get('flows', {}))

        except FileNotFoundError:
            raise ConfigError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ConfigError(f"Invalid YAML syntax: {str(e)}")
        except Exception as e:
            raise ConfigError(f"Failed to setup coordinator: {str(e)}")

    async def _setup_flows(self, flows_config: Dict[str, Any]) -> None:
        """Set up configured flows."""
        for name, config in flows_config.items():
            self.logger.info(f"Setting up flow: {name}")

            # Apply flow-level settings (home/store settings handled by Pydantic)
            config_with_settings = _apply_flow_settings(config)

            # Parse the flow configuration using Pydantic - applies all smart settings
            flow_config = HyggeConfig.from_dict(
                {'flows': {name: config_with_settings}}
            ).flows[name]

            # Create Home and Store instances using factory
            home = HyggeFactory.create_home(name, flow_config.home_config)
            store = HyggeFactory.create_store(name, name, flow_config.store_config)

            # Create flow with Home and Store instances
            flow = Flow(
                name=name,
                home=home,
                store=store,
                options=flow_config.options
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
                    done, still_running = await asyncio.wait(
                        running,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    # Update running list to still running tasks
                    running = list(still_running)
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