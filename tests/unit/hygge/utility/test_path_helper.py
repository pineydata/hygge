"""
Tests for PathHelper path manipulation utilities.

Following hygge's testing principles:
- Test behavior that matters to users (path construction works correctly)
- Focus on data integrity (paths are constructed as expected)
- Test error scenarios (entity mismatches, invalid inputs)
- Verify edge cases (empty paths, None values, various path structures)
"""

from pathlib import Path

import pytest

from hygge.utility.exceptions import ConfigError
from hygge.utility.path_helper import PathHelper


class TestPathHelperEntitySubstitution:
    """Test entity template substitution."""

    def test_substitute_entity_with_template(self):
        """Test substituting entity in path template."""
        result = PathHelper.substitute_entity("Files/{entity}/", "Account")
        assert result == "Files/Account/"

    def test_substitute_entity_no_template(self):
        """Test path without template returns unchanged."""
        result = PathHelper.substitute_entity("Files/Account/", "Account")
        assert result == "Files/Account/"

    def test_substitute_entity_no_entity_name(self):
        """Test path with template but no entity_name returns unchanged."""
        result = PathHelper.substitute_entity("Files/{entity}/", None)
        assert result == "Files/{entity}/"

    def test_substitute_entity_with_path_object(self):
        """Test substitution works with Path objects."""
        # Note: Path objects normalize trailing slashes, so Path("Files/{entity}/")
        # becomes "Files/{entity}" when converted to string
        result = PathHelper.substitute_entity(Path("Files/{entity}/"), "Account")
        assert result == "Files/Account"


class TestPathHelperJoin:
    """Test path joining functionality."""

    def test_join_simple_paths(self):
        """Test joining simple path parts."""
        result = PathHelper.join("Files", "Account", "file.parquet")
        assert result == "Files/Account/file.parquet"

    def test_join_with_trailing_slashes(self):
        """Test joining handles trailing slashes."""
        result = PathHelper.join("Files/", "/Account/", "file.parquet")
        assert result == "Files/Account/file.parquet"

    def test_join_with_path_objects(self):
        """Test joining with Path objects."""
        result = PathHelper.join(Path("Files"), "Account", Path("file.parquet"))
        assert result == "Files/Account/file.parquet"

    def test_join_empty_parts_filtered(self):
        """Test empty parts are filtered out."""
        result = PathHelper.join("Files", "", "Account", None, "file.parquet")
        assert result == "Files/Account/file.parquet"

    def test_join_all_empty_returns_empty(self):
        """Test joining all empty parts returns empty string."""
        result = PathHelper.join("", None, "")
        assert result == ""


class TestPathHelperGetFilename:
    """Test filename extraction."""

    def test_get_filename_from_path(self):
        """Test extracting filename from full path."""
        result = PathHelper.get_filename("Files/Account/file.parquet")
        assert result == "file.parquet"

    def test_get_filename_no_path(self):
        """Test empty path returns empty string."""
        result = PathHelper.get_filename("")
        assert result == ""

    def test_get_filename_with_path_object(self):
        """Test extraction works with Path objects."""
        result = PathHelper.get_filename(Path("Files/Account/file.parquet"))
        assert result == "file.parquet"

    def test_get_filename_root_file(self):
        """Test filename at root level."""
        result = PathHelper.get_filename("file.parquet")
        assert result == "file.parquet"


class TestPathHelperGetParts:
    """Test path parts extraction."""

    def test_get_parts_simple_path(self):
        """Test extracting parts from simple path."""
        result = PathHelper.get_parts("Files/Account/file.parquet")
        assert result == ["Files", "Account", "file.parquet"]

    def test_get_parts_with_trailing_slash(self):
        """Test trailing slash doesn't create empty part."""
        result = PathHelper.get_parts("Files/Account/")
        assert result == ["Files", "Account"]

    def test_get_parts_empty_path(self):
        """Test empty path returns empty list."""
        result = PathHelper.get_parts("")
        assert result == []

    def test_get_parts_with_path_object(self):
        """Test extraction works with Path objects."""
        result = PathHelper.get_parts(Path("Files/Account/file.parquet"))
        assert result == ["Files", "Account", "file.parquet"]


class TestPathHelperBuildStagingPath:
    """Test staging path construction."""

    def test_build_staging_path_with_entity(self):
        """Test staging path with entity in middle of path."""
        result = PathHelper.build_staging_path(
            "Files/Account/", "Account", "file.parquet"
        )
        assert result == "Files/_tmp/Account/file.parquet"

    def test_build_staging_path_entity_at_start(self):
        """Test staging path when entity is first segment."""
        result = PathHelper.build_staging_path("Account/", "Account", "file.parquet")
        assert result == "_tmp/Account/file.parquet"

    def test_build_staging_path_nested_entity(self):
        """Test staging path with nested structure."""
        result = PathHelper.build_staging_path(
            "Files/Subfolder/Account/", "Account", "file.parquet"
        )
        assert result == "Files/Subfolder/_tmp/Account/file.parquet"

    def test_build_staging_path_no_entity(self):
        """Test staging path without entity."""
        result = PathHelper.build_staging_path("Files/", None, "file.parquet")
        assert result == "Files/_tmp/file.parquet"

    def test_build_staging_path_entity_not_found_raises_error(self):
        """Test that entity mismatch raises ConfigError."""
        with pytest.raises(ConfigError) as exc_info:
            PathHelper.build_staging_path("Files/Other/", "Account", "file.parquet")
        assert "Entity 'Account' not found" in str(exc_info.value)
        assert "Files/Other/" in str(exc_info.value)

    def test_build_staging_path_empty_base(self):
        """Test staging path with empty base path."""
        result = PathHelper.build_staging_path("", "Account", "file.parquet")
        assert result == "_tmp/Account/file.parquet"

    def test_build_staging_path_custom_staging_dir(self):
        """Test staging path with custom staging directory."""
        result = PathHelper.build_staging_path(
            "Files/Account/", "Account", "file.parquet", staging_dir="temp"
        )
        assert result == "Files/temp/Account/file.parquet"


class TestPathHelperBuildFinalPath:
    """Test final path construction."""

    def test_build_final_path_with_trailing_slash(self):
        """Test final path handles trailing slash."""
        result = PathHelper.build_final_path("Files/Account/", "file.parquet")
        assert result == "Files/Account/file.parquet"

    def test_build_final_path_no_trailing_slash(self):
        """Test final path without trailing slash."""
        result = PathHelper.build_final_path("Files/Account", "file.parquet")
        assert result == "Files/Account/file.parquet"

    def test_build_final_path_empty_base(self):
        """Test final path with empty base returns just filename."""
        result = PathHelper.build_final_path("", "file.parquet")
        assert result == "file.parquet"

    def test_build_final_path_with_path_object(self):
        """Test final path works with Path objects."""
        result = PathHelper.build_final_path(Path("Files/Account/"), "file.parquet")
        assert result == "Files/Account/file.parquet"


class TestPathHelperMergePaths:
    """Test path merging functionality."""

    def test_merge_paths_simple(self):
        """Test merging simple paths."""
        result = PathHelper.merge_paths("data", "users", "file.parquet")
        assert result == "data/users/file.parquet"

    def test_merge_paths_with_slashes(self):
        """Test merging handles leading/trailing slashes."""
        result = PathHelper.merge_paths("data/", "/users", "/file.parquet")
        assert result == "data/users/file.parquet"

    def test_merge_paths_coordinator_use_case(self):
        """Test coordinator path merging scenario."""
        flow_path = "data/users"
        entity_path = "orders"
        result = PathHelper.merge_paths(flow_path, entity_path)
        assert result == "data/users/orders"

    def test_merge_paths_with_path_objects(self):
        """Test merging works with Path objects."""
        result = PathHelper.merge_paths(Path("data"), "users")
        assert result == "data/users"

    def test_merge_paths_all_empty(self):
        """Test merging all empty paths returns empty."""
        result = PathHelper.merge_paths("", None, "")
        assert result == ""

    def test_merge_paths_preserves_absolute(self):
        """Test merging preserves absolute path nature."""
        result = PathHelper.merge_paths("/var/data", "users", "file.parquet")
        assert result == "/var/data/users/file.parquet"

    def test_merge_paths_relative_stays_relative(self):
        """Test merging relative paths stays relative."""
        result = PathHelper.merge_paths("data", "users")
        assert result == "data/users"


class TestPathHelperEdgeCases:
    """Test edge cases and error handling."""

    def test_none_path_values(self):
        """Test handling None path values."""
        assert PathHelper.get_filename(None) == ""
        assert PathHelper.get_parts(None) == []
        assert PathHelper.join(None, "file") == "file"

    def test_entity_with_path_separators(self):
        """Test entity name that contains path separators."""
        # Entity name should not contain separators, but if it does,
        # it won't match as a path segment (which is correct)
        with pytest.raises(ConfigError):
            PathHelper.build_staging_path(
                "Files/Account/", "Account/Sub", "file.parquet"
            )

    def test_staging_path_entity_partial_match(self):
        """Test entity that's a substring but not exact match."""
        # "Account" is substring of "AccountData" but won't match
        with pytest.raises(ConfigError):
            PathHelper.build_staging_path(
                "Files/AccountData/", "Account", "file.parquet"
            )
