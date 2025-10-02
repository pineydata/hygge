"""
Main configuration model for hygge.
"""
from typing import Any, Dict

from pydantic import BaseModel, Field, field_validator

from .flow_config import FlowConfig


class HyggeConfig(BaseModel):
    """Main configuration model for hygge."""
    flows: Dict[str, FlowConfig] = Field(..., description="Flow configurations")

    @field_validator('flows')
    @classmethod
    def validate_flows_not_empty(cls, v):
        """Validate flows section is not empty."""
        if not v:
            raise ValueError("At least one flow must be configured")
        return v

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HyggeConfig':
        """Create configuration from dictionary."""
        return cls(**data)

    def get_flow_config(self, flow_name: str) -> FlowConfig:
        """Get configuration for a specific flow."""
        if flow_name not in self.flows:
            raise ValueError(f"Flow '{flow_name}' not found in configuration")
        return self.flows[flow_name]
