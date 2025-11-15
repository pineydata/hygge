"""
Workspace manages hygge project discovery and configuration loading.

The workspace is responsible for:
1. Finding hygge.yml by walking up directories
2. Reading workspace configuration (hygge.yml)
3. Discovering flows in the flows/ directory
4. Reading flow configurations and entity definitions
5. Expanding environment variables
6. Preparing CoordinatorConfig for execution

This separates workspace discovery and loading from the Coordinator's
orchestration responsibilities.
"""
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, field_validator

from hygge.messages import get_logger
from hygge.utility.exceptions import ConfigError

from .flow import FlowConfig
from .journal import JournalConfig


class WorkspaceConfig(BaseModel):
    """Configuration model for a hygge workspace/project."""

    flows: Dict[str, FlowConfig] = Field(..., description="Flow configurations")
    connections: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Named connection pool configurations"
    )
    journal: Optional[Union[Dict[str, Any], JournalConfig]] = Field(
        default=None,
        description="Journal configuration for tracking execution metadata",
    )

    @field_validator("flows")
    @classmethod
    def validate_flows_not_empty(cls, v):
        """Validate flows section is not empty."""
        if not v:
            raise ValueError("At least one flow must be configured")
        return v

    @field_validator("journal", mode="before")
    @classmethod
    def validate_journal(cls, v):
        """Validate and normalize journal configuration."""
        if v is None:
            return None
        if isinstance(v, JournalConfig):
            return v
        if isinstance(v, dict):
            return JournalConfig(**v)
        raise ValueError(
            "Journal configuration must be a dict or JournalConfig instance"
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkspaceConfig":
        """Create configuration from dictionary."""
        return cls(**data)

    def get_flow_config(self, flow_name: str) -> FlowConfig:
        """Get configuration for a specific flow."""
        if flow_name not in self.flows:
            raise ValueError(f"Flow '{flow_name}' not found in configuration")
        return self.flows[flow_name]


class Workspace:
    """
    Workspace represents a hygge project and manages configuration loading.

    The workspace:
    - Discovers hygge.yml by walking up directories
    - Reads workspace configuration
    - Finds and loads flows from the flows/ directory
    - Expands environment variables
    - Prepares CoordinatorConfig for execution
    """

    def __init__(self, hygge_yml: Path, name: str, flows_dir: str = "flows"):
        """
        Create a Workspace from hygge.yml path.

        Args:
            hygge_yml: Path to hygge.yml file
            name: Workspace name (from hygge.yml)
            flows_dir: Name of the flows directory (default: "flows")
        """
        self.hygge_yml = hygge_yml
        self.root = hygge_yml.parent
        self.name = name
        self.flows_dir = flows_dir
        self.flows_path = self.root / flows_dir
        self.config: Dict[str, Any] = {}
        self.connections: Dict[str, Any] = {}
        self.options: Dict[str, Any] = {}
        self.logger = get_logger("hygge.workspace")

    @staticmethod
    def find(start_path: Optional[Path] = None) -> "Workspace":
        """
        Find hygge.yml by walking up directories from start_path.

        Args:
            start_path: Directory to start searching from (default: current directory)

        Returns:
            Workspace instance

        Raises:
            ConfigError: If hygge.yml is not found
        """
        if start_path is None:
            start_path = Path.cwd()

        current = Path(start_path).resolve()
        searched_paths = []

        while current != current.parent:
            project_file = current / "hygge.yml"
            searched_paths.append(str(project_file))
            if project_file.exists():
                return Workspace.from_path(project_file)
            current = current.parent

        # dbt-style error message
        error_msg = f"""
No hygge.yml found in current path: {Path.cwd()}

Searched locations:
{chr(10).join(f"  - {path}" for path in searched_paths)}

To get started, run:
  hygge init <project_name>
"""
        raise ConfigError(error_msg)

    @classmethod
    def from_path(cls, hygge_yml: Path) -> "Workspace":
        """
        Create a Workspace from hygge.yml path.

        Reads hygge.yml to determine workspace name and flows_dir.

        Args:
            hygge_yml: Path to hygge.yml file

        Returns:
            Workspace instance
        """
        # Read hygge.yml to get workspace name and settings
        with open(hygge_yml, "r") as f:
            workspace_data = yaml.safe_load(f) or {}

        name = workspace_data.get("name", hygge_yml.parent.name)
        flows_dir = workspace_data.get("flows_dir", "flows")

        return cls(hygge_yml, name, flows_dir)

    def _read_workspace_config(self) -> None:
        """Read workspace configuration from hygge.yml."""
        with open(self.hygge_yml, "r") as f:
            raw_config = yaml.safe_load(f)
            self.config = raw_config or {}

        # Expand environment variables in workspace config
        self.config = self._expand_env_vars(self.config)

        # Extract connections and options
        self.connections = self.config.get("connections", {})
        self.options = self.config.get("options", {})

        project_name = self.config.get("name", "unnamed")
        self.logger.info(f"Loaded workspace config: {project_name}")
        self.logger.debug(f"Raw workspace config: {self.config}")

    def _find_flows(self) -> Dict[str, FlowConfig]:
        """
        Find all flows in the flows/ directory.

        Looks for flow directories (each containing a flow.yml) and loads them.

        Returns:
            Dictionary mapping flow names to FlowConfig instances

        Raises:
            ConfigError: If flows directory doesn't exist or no flows found
        """
        self.logger.debug(f"Looking for flows in: {self.flows_path}")
        self.logger.debug(f"Workspace config: {self.config}")

        if not self.flows_path.exists():
            raise ConfigError(f"Flows directory not found: {self.flows_path}")

        flows = {}

        # Look for flow directories
        for flow_dir in self.flows_path.iterdir():
            self.logger.debug(f"Checking directory: {flow_dir}")
            if flow_dir.is_dir() and (flow_dir / "flow.yml").exists():
                flow_name = flow_dir.name
                try:
                    # Load flow config
                    flow_config = self._read_flow_config(flow_dir)
                    flows[flow_name] = flow_config
                    self.logger.debug(f"Loaded flow: {flow_name}")
                except Exception as e:
                    self.logger.error(f"Failed to load flow {flow_name}: {str(e)}")
                    raise ConfigError(f"Failed to load flow {flow_name}: {str(e)}")

        if not flows:
            raise ConfigError(f"No flows found in directory: {self.flows_path}")

        return flows

    def _read_flow_config(self, flow_dir: Path) -> FlowConfig:
        """
        Read flow configuration from flow directory.

        Loads flow.yml and any entities from the entities/ subdirectory.

        Args:
            flow_dir: Path to flow directory (contains flow.yml)

        Returns:
            FlowConfig instance
        """
        flow_file = flow_dir / "flow.yml"
        with open(flow_file, "r") as f:
            flow_data = yaml.safe_load(f)

        # Expand environment variables in flow config
        flow_data = self._expand_env_vars(flow_data)

        # Load entities if they exist
        entities_dir = flow_dir / "entities"
        if entities_dir.exists():
            entities = self._read_entities(entities_dir, flow_data.get("defaults", {}))
            flow_data["entities"] = entities

        return FlowConfig(**flow_data)

    def _read_entities(
        self, entities_dir: Path, defaults: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Read entity definitions from entities directory.

        Loads all .yml files in the entities directory and merges them
        with flow-level defaults.

        Args:
            entities_dir: Path to entities directory
            defaults: Flow-level defaults to merge with each entity

        Returns:
            List of entity configuration dictionaries
        """
        entities = []

        for entity_file in entities_dir.glob("*.yml"):
            with open(entity_file, "r") as f:
                entity_data = yaml.safe_load(f)

            # Expand environment variables in entity config
            entity_data = self._expand_env_vars(entity_data)

            # Merge with defaults
            entity_data = {**defaults, **entity_data}
            entities.append(entity_data)

        return entities

    def _expand_env_vars(self, data: Any) -> Any:
        """
        Recursively expand environment variables in configuration data.

        Supports patterns like ${VAR_NAME} and ${VAR_NAME:-default_value}

        Args:
            data: Configuration data (dict, list, str, or other)

        Returns:
            Data with environment variables expanded

        Raises:
            ConfigError: If environment variable is not set and no default provided
        """
        if isinstance(data, str):
            # Pattern: ${VAR_NAME} or ${VAR_NAME:-default}
            pattern = r"\$\{([^:}]+)(?::-([^}]*))?\}"

            def replace_env_var(match):
                var_name = match.group(1)
                has_default = match.group(2) is not None
                default_value = match.group(2) if has_default else ""

                env_value = os.getenv(var_name)
                if env_value is not None:
                    return env_value
                elif has_default:
                    return default_value
                else:
                    raise ConfigError(
                        f"Environment variable '{var_name}' is not set and no default"
                    )

            return re.sub(pattern, replace_env_var, data)

        elif isinstance(data, dict):
            return {key: self._expand_env_vars(value) for key, value in data.items()}

        elif isinstance(data, list):
            return [self._expand_env_vars(item) for item in data]

        else:
            return data

    def prepare(self) -> WorkspaceConfig:
        """
        Prepare workspace configuration for Coordinator execution.

        Reads hygge.yml, discovers flows, and returns a validated
        WorkspaceConfig ready for execution.

        Returns:
            WorkspaceConfig with flows, connections, and journal config

        Raises:
            ConfigError: If configuration cannot be loaded
        """
        # Read workspace config
        self._read_workspace_config()

        # Find all flows
        flows = self._find_flows()

        # Create workspace config with connections and journal if present
        journal_config = self.config.get("journal")
        config = WorkspaceConfig(
            flows=flows, connections=self.connections, journal=journal_config
        )

        self.logger.info(
            f"Prepared workspace '{self.name}' with {len(flows)} flows "
            f"from {self.flows_path}"
        )

        return config
