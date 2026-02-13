"""
Entity represents a fully configured flow ready for execution.

An Entity is a flow that has been configured for a specific dataset/table.
It represents the merging of FlowConfig (template) with EntityConfig (specific
overrides), making it ready for validation and execution.

Following hygge's philosophy, entities make it comfortable to work with multiple
datasets using the same flow template. Each entity gets its own configuration
while sharing the common flow pattern.
"""

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field

from .config import FlowConfig


class Entity(BaseModel):
    """
    Fully configured entity ready for execution.

    An Entity represents a flow that has been configured for a specific
    dataset/table. It's the result of merging a FlowConfig (template) with
    an EntityConfig (specific overrides), making it ready to run.

    Following hygge's philosophy, entities make it comfortable to work with
    multiple datasets using the same flow template. Each entity gets its own
    configuration (paths, watermarks, etc.) while sharing the common flow pattern.

    Entity contains:
    - The merged FlowConfig (entity overrides applied)
    - Entity metadata (name, original config)
    - Flow name information (base_flow_name, flow_name)

    Entity is created by Workspace during entity expansion and is ready for
    validation and execution. FlowFactory uses Entity to create Flow objects.

    The name "Entity" reflects that this is the core concept - the dataset/table
    being moved through the flow pattern.
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

    model_config = ConfigDict(
        arbitrary_types_allowed=True  # Allow FlowConfig (Pydantic model)
    )

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
