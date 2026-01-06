"""
Error formatting for hygge - friendly, helpful error messages.

ErrorFormatter provides comfortable error messages that help users understand
what went wrong and how to fix it. It transforms technical exceptions into
friendly, actionable guidance that feels natural and helpful.
"""
from typing import Optional, Tuple

from hygge.utility.exceptions import HyggeError


class ErrorFormatter:
    """
    Formats errors into friendly, helpful messages.

    ErrorFormatter transforms technical exceptions into comfortable, actionable
    guidance. It prioritizes clarity and helpfulness over technical detail,
    making errors feel less intimidating and more solvable.

    Following hygge's philosophy:
    - **Comfort**: Errors feel helpful, not scary
    - **Clarity**: Clear messages that explain what happened
    - **Actionability**: Suggestions for how to fix the problem
    """

    @staticmethod
    def format_error(
        error: Exception, verbose: bool = False
    ) -> Tuple[str, Optional[str]]:
        """
        Format an error into a friendly message and optional suggestion.

        Args:
            error: The exception to format
            verbose: If True, include technical details

        Returns:
            Tuple of (friendly_message, suggestion)
        """
        # If it's a HyggeError with friendly_message, use it
        if isinstance(error, HyggeError) and hasattr(error, "friendly_message"):
            friendly_msg = error.friendly_message or str(error)
            suggestion = getattr(error, "suggestion", None)
            return (friendly_msg, suggestion)

        # Format based on error type
        error_type = type(error).__name__

        # Connection errors
        if "Connection" in error_type or "connection" in str(error).lower():
            friendly_msg = "Couldn't connect to the data source or destination."
            suggestion = (
                "Check your connection settings, network connectivity, "
                "and credentials. Use 'hygge debug' to test connections."
            )
            return (friendly_msg, suggestion)

        # Configuration errors
        if "Config" in error_type or "config" in str(error).lower():
            friendly_msg = "There's an issue with your configuration."
            suggestion = (
                "Check your hygge.yml and flow.yml files for syntax errors "
                "or missing required fields. Use 'hygge debug' to validate."
            )
            return (friendly_msg, suggestion)

        # File/path errors
        if "FileNotFound" in error_type or "path" in str(error).lower():
            friendly_msg = "Couldn't find a file or directory."
            suggestion = (
                "Check that the path exists and you have permission to access it. "
                "Paths can be relative to your project directory or absolute."
            )
            return (friendly_msg, suggestion)

        # Permission errors
        if "Permission" in error_type or "permission" in str(error).lower():
            friendly_msg = "You don't have permission to access this resource."
            suggestion = (
                "Check file permissions or credentials. "
                "For cloud storage, verify your authentication is set up correctly."
            )
            return (friendly_msg, suggestion)

        # Generic error with context
        friendly_msg = str(error) if str(error) else f"An error occurred: {error_type}"
        suggestion = (
            "Check the error message above for details. "
            "Use 'hygge debug' to test your configuration, "
            "or run with --verbose for more technical details."
        )

        return (friendly_msg, suggestion)

    @staticmethod
    def format_with_stack_trace(error: Exception) -> str:
        """
        Format error with full stack trace for verbose mode.

        Args:
            error: The exception to format

        Returns:
            Formatted error with stack trace
        """
        import traceback

        error_type = type(error).__name__
        error_msg = str(error)

        # Build full traceback
        tb_lines = traceback.format_exception(type(error), error, error.__traceback__)

        # Format as readable output
        lines = [
            f"Error Type: {error_type}",
            f"Error Message: {error_msg}",
            "",
            "Full Traceback:",
            "─" * 60,
        ]
        lines.extend(tb_lines)
        lines.append("─" * 60)

        return "\n".join(lines)
