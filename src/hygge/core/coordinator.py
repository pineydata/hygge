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
from pydantic import BaseModel, Field, field_validator

from hygge.utility.exceptions import ConfigError
from hygge.utility.logger import get_logger

from .flow import Flow, FlowConfig


def validate_config(config: Dict[str, Any]) -> List[str]:
    """Validate configuration using Pydantic models."""
    try:
        CoordinatorConfig.from_dict(config)
        return []
    except Exception as e:
        return [str(e)]


class Coordinator:
    """
    Orchestrates multiple data flows based on configuration.

    The coordinator:
    - Reads configuration from YAML files
    - Validates configuration using Pydantic models
    - Orchestrates flows in parallel
    - Handles flow-level error management
    """

    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            # Project discovery mode - look for hygge.yml
            config_path = self._find_project_config()

        self.config_path = Path(config_path)
        self.config = None
        self.flows: List[Flow] = []
        self.options: Dict[str, Any] = {}
        self.logger = get_logger("hygge.coordinator")

        # No longer need Factory - using registry pattern directly

    def _find_project_config(self) -> str:
        """Look for hygge.yml in current directory and parents."""
        current = Path.cwd()
        searched_paths = []

        while current != current.parent:
            project_file = current / "hygge.yml"
            searched_paths.append(str(project_file))
            if project_file.exists():
                return str(project_file)
            current = current.parent

        # dbt-style error message
        error_msg = f"""
No hygge.yml found in current path: {Path.cwd()}

Searched locations:
{chr(10).join(f"  - {path}" for path in searched_paths)}

To get started, run:
  hej init
"""
        raise ConfigError(error_msg)

    async def run(self) -> None:
        """Run all configured flows."""
        self.logger.info(f"Starting coordinator with config: {self.config_path}")

        # Load and validate configuration
        self._load_config()

        # Create flows
        self._create_flows()

        # Run flows in parallel
        await self._run_flows()

    def _load_config(self) -> None:
        """Load and validate configuration from file or directory."""
        try:
            if self.config_path.is_file():
                # Single file configuration (legacy)
                self._load_single_file_config()
            elif self.config_path.is_dir():
                # Directory-based configuration (new progressive approach)
                self._load_directory_config()
            else:
                raise ConfigError(
                    f"Configuration path must be file or directory: {self.config_path}"
                )

        except Exception as e:
            raise ConfigError(f"Failed to load configuration: {str(e)}")

    def _load_single_file_config(self) -> None:
        """Load configuration from single YAML file (legacy approach)."""
        with open(self.config_path, "r") as f:
            config_data = yaml.safe_load(f)

        # Validate configuration
        errors = validate_config(config_data)
        if errors:
            raise ConfigError(f"Configuration validation failed: {errors}")

        # Parse with Pydantic
        self.config = CoordinatorConfig.from_dict(config_data)

        # Extract options
        self.options = config_data.get("options", {})

        self.logger.info(
            f"Loaded single-file configuration with {len(self.config.flows)} flows"
        )

    def _load_directory_config(self) -> None:
        """Load configuration from directory structure (progressive approach)."""
        flows = {}

        # Look for flow directories
        for flow_dir in self.config_path.iterdir():
            if flow_dir.is_dir() and (flow_dir / "flow.yml").exists():
                flow_name = flow_dir.name
                try:
                    # Load flow config directly
                    flow_file = flow_dir / "flow.yml"
                    with open(flow_file, "r") as f:
                        flow_data = yaml.safe_load(f)
                    flow_config = FlowConfig(**flow_data)
                    flows[flow_name] = flow_config
                    self.logger.debug(f"Loaded flow: {flow_name}")
                except Exception as e:
                    raise ConfigError(f"Failed to load flow {flow_name}: {str(e)}")

        if not flows:
            raise ConfigError(f"No flows found in directory: {self.config_path}")

        # Create coordinator config
        self.config = CoordinatorConfig(flows=flows)

        # Load global options if they exist
        options_file = self.config_path / "options.yml"
        if options_file.exists():
            with open(options_file, "r") as f:
                self.options = yaml.safe_load(f) or {}
        else:
            self.options = {}

        self.logger.info(f"Loaded directory configuration with {len(flows)} flows")

    def _create_flows(self) -> None:
        """Create Flow instances from configuration."""
        self.flows = []

        for flow_name, flow_config in self.config.flows.items():
            try:
                # Get home and store instances from FlowConfig
                home = flow_config.home_instance
                store = flow_config.store_instance

                # Create flow with options
                flow_options = flow_config.options.copy()
                flow_options.update(
                    {
                        "queue_size": flow_config.queue_size,
                        "timeout": flow_config.timeout,
                    }
                )

                flow = Flow(flow_name, home, store, flow_options)
                self.flows.append(flow)

                self.logger.debug(f"Created flow: {flow_name}")

            except Exception as e:
                raise ConfigError(f"Failed to create flow {flow_name}: {str(e)}")

    async def _run_flows(self) -> None:
        """Run all flows in parallel."""
        if not self.flows:
            self.logger.warning("No flows to run")
            return

        self.logger.info(f"Running {len(self.flows)} flows in parallel")

        # Create tasks for all flows
        tasks = []
        for flow in self.flows:
            task = asyncio.create_task(self._run_flow(flow), name=f"flow_{flow.name}")
            tasks.append(task)

        # Run all flows concurrently
        try:
            await asyncio.gather(*tasks)
            self.logger.success("All flows completed successfully")
        except Exception as e:
            self.logger.error(f"Some flows failed: {str(e)}")
            # Cancel remaining tasks
            for task in tasks:
                if not task.done():
                    task.cancel()
            raise

    async def _run_flow(self, flow: Flow) -> None:
        """Run a single flow with error handling."""
        try:
            await flow.start()
        except Exception as e:
            self.logger.error(f"Error in flow {flow.name}: {e}")
            if not self.options.get("continue_on_error", False):
                raise


class CoordinatorConfig(BaseModel):
    """Main configuration model for hygge."""

    flows: Dict[str, FlowConfig] = Field(..., description="Flow configurations")

    @field_validator("flows")
    @classmethod
    def validate_flows_not_empty(cls, v):
        """Validate flows section is not empty."""
        if not v:
            raise ValueError("At least one flow must be configured")
        return v

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CoordinatorConfig":
        """Create configuration from dictionary."""
        return cls(**data)

    def get_flow_config(self, flow_name: str) -> FlowConfig:
        """Get configuration for a specific flow."""
        if flow_name not in self.flows:
            raise ValueError(f"Flow '{flow_name}' not found in configuration")
        return self.flows[flow_name]
