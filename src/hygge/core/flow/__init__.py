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
from .factory import FlowFactory
from .flow import Flow


# Attach factory methods to Flow class for Rails-style API
# This maintains backward compatibility: Flow.from_config() still works
# We need to wrap the static methods to make them classmethods
def _from_config(cls, *args, **kwargs):
    """Wrapper to convert static method to classmethod."""
    return FlowFactory.from_config(*args, **kwargs)


def _from_entity(cls, *args, **kwargs):
    """Wrapper to convert static method to classmethod."""
    return FlowFactory.from_entity(*args, **kwargs)


Flow.from_config = classmethod(_from_config)
Flow.from_entity = classmethod(_from_entity)

# FlowFactory is exported for direct access to factory helpers
# Internal helpers (_apply_overrides, _validate_run_type_alignment, etc.)
# are in FlowFactory, not attached to Flow to maintain clean separation
__all__ = ["Flow", "FlowConfig", "FlowFactory"]
