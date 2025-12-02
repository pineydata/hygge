"""
Flow module for data movement orchestration.

This module provides:
- Flow: Core data movement class (producer-consumer pattern)
- FlowConfig: Configuration model for flows
- FlowFactory: Factory for creating Flow instances from configuration

The public API maintains backward compatibility:
- Flow.from_config() and Flow.from_entity() are attached as classmethods
- Flow and FlowConfig are exported directly
"""

from .config import FlowConfig
from .entity import Entity
from .factory import FlowFactory
from .flow import Flow

# Attach factory methods to Flow class for Rails-style API
# This maintains backward compatibility: Flow.from_config() still works
# We use lambdas to wrap the static methods as classmethods
Flow.from_config = classmethod(
    lambda cls, *args, **kwargs: FlowFactory.from_config(*args, **kwargs)
)
Flow.from_entity = classmethod(
    lambda cls, *args, **kwargs: FlowFactory.from_entity(*args, **kwargs)
)

# FlowFactory is exported for direct access to factory helpers
# Internal helpers (_apply_overrides, _validate_run_type_alignment, etc.)
# are in FlowFactory, not attached to Flow to maintain clean separation
__all__ = ["Flow", "FlowConfig", "FlowFactory", "Entity"]
