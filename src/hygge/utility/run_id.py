"""
Run ID generation utility for comfortable, reliable journal tracking.

Generates deterministic, hash-based run IDs for coordinator, flow, and
entity runs. These IDs are used throughout hygge to track execution
history and enable incremental loads with watermarks.

Following hygge's philosophy, run IDs prioritize:
- **Comfort**: Deterministic IDs that are easy to understand and reproduce
- **Reliability**: Hash-based IDs ensure uniqueness and consistency
- **Natural flow**: IDs are generated automatically, you don't need to think about them
"""
import hashlib
from typing import List


def generate_run_id(components: List[str]) -> str:
    """
    Generate deterministic run ID from components.

    Uses SHA256 hash to create a fixed-length identifier that is:
    - Deterministic: Same inputs = same ID
    - Reproducible: Can reconstruct ID from components
    - Unique: Different inputs = different IDs (with high probability)

    Args:
        components: List of string components to hash
            (e.g., [coordinator, flow, entity, start_time])

    Returns:
        32-character hex string (first 32 chars of SHA256 hash)

    Example:
        >>> coordinator_run_id = generate_run_id(
        ...     ["main_coordinator", "2024-01-01T10:00:00Z"]
        ... )
        >>> flow_run_id = generate_run_id(
        ...     ["main_coordinator", "users_flow", "2024-01-01T10:00:00Z"]
        ... )
        >>> entity_run_id = generate_run_id([
        ...     "main_coordinator", "users_flow", "users",
        ...     "2024-01-01T10:00:00Z"
        ... ])
    """
    # Join components with delimiter
    combined = "|".join(str(c) for c in components)
    # Hash to fixed-length string
    hash_obj = hashlib.sha256(combined.encode("utf-8"))
    return hash_obj.hexdigest()[:32]  # 32-character hex string
