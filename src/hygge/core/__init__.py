"""
Core components of the hygge framework.
"""

from .coordinator import Coordinator, CoordinatorConfig
from .flow import Flow, FlowConfig
from .home import Home, HomeConfig
from .store import Store, StoreConfig
from .workspace import Workspace, WorkspaceConfig

__all__ = [
    "Coordinator",
    "CoordinatorConfig",  # Backward compatibility alias
    "Flow",
    "FlowConfig",
    "Home",
    "HomeConfig",
    "Store",
    "StoreConfig",
    "Workspace",
    "WorkspaceConfig",
]
