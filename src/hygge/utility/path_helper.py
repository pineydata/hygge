"""
Robust path manipulation utilities for comfortable, reliable path handling.

PathHelper makes it easy to work with paths across different storage backends
(local filesystem, ADLS, OneLake) with a consistent, comfortable interface.

Following hygge's philosophy, PathHelper prioritizes:
- **Comfort**: Simple, intuitive path operations that work across storage types
- **Reliability**: Robust handling of edge cases
  (trailing slashes, empty parts, etc.)
- **Natural flow**: Paths work smoothly whether local or remote,
  with or without entities

Uses pathlib internally for reliable path operations while working with strings
for compatibility with local filesystem and cloud storage.
"""

from pathlib import Path, PurePath
from typing import Optional, Union

from hygge.utility.exceptions import ConfigError


class PathHelper:
    """
    Path manipulation utilities for comfortable, reliable path handling.

    PathHelper makes it easy to work with paths across different storage backends
    (local filesystem, ADLS, OneLake) with a consistent, comfortable interface.
    It handles entity substitution, path joining, and staging path construction
    automatically.

    Following hygge's philosophy, PathHelper prioritizes:
    - **Comfort**: Simple, intuitive path operations that work across storage types
    - **Reliability**: Robust handling of edge cases
      (trailing slashes, empty parts, etc.)
    - **Natural flow**: Paths work smoothly whether local or remote,
      with or without entities

    All methods are static - use directly without instantiation.
    """

    STAGING_DIR = "_tmp"

    @staticmethod
    def substitute_entity(
        path: Union[str, Path], entity_name: Optional[str] = None
    ) -> str:
        """
        Substitute {entity} template with entity name.

        Args:
            path: Path that may contain "{entity}" template
            entity_name: Entity name to substitute

        Returns:
            Path with entity substituted, or original path if no entity_name
        """
        path_str = str(path) if path else ""
        if entity_name and "{entity}" in path_str:
            return path_str.replace("{entity}", entity_name)
        return path_str

    @staticmethod
    def join(*parts: Union[str, Path]) -> str:
        """
        Join path parts into a single path.

        Handles trailing/leading slashes and empty parts automatically.

        Args:
            *parts: Path parts to join

        Returns:
            Joined path string with normalized separators
        """
        clean_parts = [str(p).strip("/") for p in parts if p and str(p).strip()]
        if not clean_parts:
            return ""
        return PurePath(*clean_parts).as_posix()

    @staticmethod
    def get_filename(path: Union[str, Path]) -> str:
        """
        Extract filename from path.

        Args:
            path: Full path string or Path object

        Returns:
            Filename component (last part of path)
        """
        if not path:
            return ""
        return PurePath(path).name

    @staticmethod
    def get_parts(path: Union[str, Path]) -> list[str]:
        """
        Get path components as list.

        Args:
            path: Path string or Path object

        Returns:
            List of non-empty path components
        """
        if not path:
            return []
        return [p for p in PurePath(path).parts if p]

    @staticmethod
    def build_staging_path(
        base_path: Union[str, Path],
        entity_name: Optional[str],
        filename: str,
        staging_dir: str = STAGING_DIR,
    ) -> str:
        """
        Build staging path by inserting staging directory before entity.

        For "Files/Account/", creates "Files/_tmp/Account/filename".
        Validates that entity_name appears in base_path when provided.

        Args:
            base_path: Base path (e.g., "Files/Account/")
            entity_name: Entity name - must appear in base_path if provided
            filename: Filename to append
            staging_dir: Staging directory name (default: "_tmp")

        Returns:
            Staging path with staging_dir inserted before entity

        Raises:
            ConfigError: If entity_name provided but not found in base_path
        """
        # No entity: simple append
        if not entity_name:
            return PathHelper.join(base_path, staging_dir, filename)

        # Normalize base_path
        base_clean = str(base_path).rstrip("/") if base_path else ""
        if not base_clean:
            return PathHelper.join(staging_dir, entity_name, filename)

        # Find entity in path parts (must be exact match)
        parts = PathHelper.get_parts(base_clean)
        try:
            entity_idx = parts.index(entity_name)
        except ValueError:
            raise ConfigError(
                f"Entity '{entity_name}' not found in base_path '{base_path}'. "
                "Entity name must appear as a path segment."
            )

        # Insert staging_dir before entity
        if entity_idx > 0:
            # prefix/_tmp/entity/filename
            prefix = "/".join(parts[:entity_idx])
            return PathHelper.join(prefix, staging_dir, entity_name, filename)
        else:
            # entity is first: _tmp/entity/filename
            return PathHelper.join(staging_dir, entity_name, filename)

    @staticmethod
    def build_final_path(base_path: Union[str, Path], filename: str) -> str:
        """
        Build final path by joining base_path with filename.

        Handles trailing slashes in base_path automatically.

        Args:
            base_path: Base path (may have trailing slash)
            filename: Filename to append

        Returns:
            Final path string
        """
        base_clean = str(base_path).rstrip("/") if base_path else ""
        return PathHelper.join(base_clean, filename) if base_clean else filename

    @staticmethod
    def merge_paths(*paths: Union[str, Path]) -> str:
        """
        Merge multiple paths into single path.

        Handles leading/trailing slashes automatically.
        Preserves absolute path nature (leading slash on first path).

        Args:
            *paths: Path strings or Path objects to merge

        Returns:
            Merged path string (preserves absolute if first path was absolute)
        """
        if not paths:
            return ""

        clean_parts = []
        first_path = str(paths[0]) if paths[0] else ""
        is_absolute = first_path.startswith("/")

        for path in paths:
            if path:
                path_str = str(path).strip("/")
                if path_str:
                    clean_parts.append(path_str)

        if not clean_parts:
            return ""

        merged = PathHelper.join(*clean_parts)
        # Preserve absolute path
        if is_absolute and not merged.startswith("/"):
            merged = "/" + merged
        return merged
