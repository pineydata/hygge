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

from .configs import FlowDefaults, HyggeConfig
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


def _apply_config_defaults(
    flow_name: str, flow_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Apply configuration defaults using Pydantic models."""
    # Parse the flow configuration
    flow = HyggeConfig.from_dict({'flows': {flow_name: flow_config}}).flows[flow_name]

    # Get home and store configurations with defaults applied
    home_config = flow.get_home_config(flow_name)
    store_config = flow.get_store_config(flow_name)

    # Apply flow defaults
    flow_defaults = FlowDefaults()
    flow_options = flow_defaults.dict()
    flow_options.update(flow.options)

    return {
        'home': home_config.dict(),
        'store': store_config.dict(),
        'options': flow_options
    }


class Coordinator:
    """
    Coordinates multiple flows based on template configuration.

    The coordinator reads configuration and creates flows, delegating
    home/store instantiation to the Flow class.

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

            # Apply configuration defaults
            full_config = _apply_config_defaults(name, config)

            # Create flow with configuration - Flow will handle instantiation
            flow = Flow(
                name=name,
                home_config=full_config['home'],
                store_config=full_config['store'],
                options=full_config['options']
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