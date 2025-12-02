"""
Entity represents a fully configured flow (FlowConfig + EntityConfig merged).

Entity is created after entity configuration has been merged with
flow configuration, making it ready for validation and execution.
"""
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from .config import FlowConfig


class Entity(BaseModel):
    """
    Fully configured entity (FlowConfig + EntityConfig merged).

    Entity represents a flow that has been configured for a specific
    dataset/table. It contains:
    - The merged FlowConfig (entity overrides applied)
    - Entity metadata (name, original config)
    - Flow name information

    Entity is created by Workspace during entity expansion and is
    ready for validation and execution. FlowFactory uses Entity to
    create Flow objects.

    The name "Entity" reflects that this is the core concept - the
    dataset/table being moved through the flow pattern (adapter).
    """

    # Flow identification
    flow_name: str = Field(
        ...,
        description=("Full flow name (base_flow_name + entity_name for entity flows)"),
    )
    base_flow_name: str = Field(..., description="Base flow name (template name)")
    entity_name: Optional[str] = Field(
        default=None,
        description=(
            "Entity name if this is an entity flow, None for non-entity flows"
        ),
    )

    # Merged configuration
    flow_config: FlowConfig = Field(
        ...,
        description=("FlowConfig with entity overrides applied (fully merged)"),
    )

    # Original entity config (for reference/debugging)
    entity_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Original entity configuration dict (before merging)",
    )

    class Config:
        """Pydantic configuration."""

        arbitrary_types_allowed = True  # Allow FlowConfig (Pydantic model)

    def __str__(self) -> str:
        """String representation of Entity."""
        if self.entity_name:
            return (
                f"Entity(flow_name='{self.flow_name}', "
                f"base_flow_name='{self.base_flow_name}', "
                f"entity_name='{self.entity_name}')"
            )
        return (
            f"Entity(flow_name='{self.flow_name}', "
            f"base_flow_name='{self.base_flow_name}')"
        )
