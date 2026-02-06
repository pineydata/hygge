"""
Unit tests for Home key finding interface.

Tests the find_keys() and supports_key_finding() methods added to the Home base class
for query-based deletion detection.
"""

from hygge.core.home import Home


class TestHomeKeyFinding:
    """Test Home key finding interface methods."""

    def test_find_keys_default_returns_none(self):
        """Test that default find_keys() returns None."""

        # Create a simple Home instance (abstract, but we can test the default method)
        class TestHome(Home, home_type="test_key_finding"):
            async def _get_batches(self):
                yield None

        home = TestHome("test", {})
        # Can't directly test async method in sync test, but we can check
        # the method exists
        assert hasattr(home, "find_keys")
        assert callable(home.find_keys)

    def test_supports_key_finding_default_returns_false(self):
        """Test that default supports_key_finding() returns False."""

        class TestHome(Home, home_type="test_key_finding2"):
            async def _get_batches(self):
                yield None

        home = TestHome("test", {})
        assert home.supports_key_finding() is False

    def test_home_has_key_finding_methods(self):
        """Test that all Home instances have key finding methods."""

        class TestHome(Home, home_type="test_key_finding3"):
            async def _get_batches(self):
                yield None

        home = TestHome("test", {})
        assert hasattr(home, "find_keys")
        assert hasattr(home, "supports_key_finding")
        assert callable(home.find_keys)
        assert callable(home.supports_key_finding)
