"""
Default settings that make hygge feel like home.
"""
from pydantic import BaseModel, Field


class TimeoutSettings(BaseModel):
    """Operation timeouts in seconds."""
    general: int = Field(default=300, description="General operation timeout (5 minutes)")
    batch: int = Field(default=600, description="Batch operation timeout (10 minutes)")
    long: int = Field(default=1800, description="Long operation timeout (30 minutes)")


class PathSettings(BaseModel):
    """Path patterns for data storage."""
    temp: str = Field(
        default="tmp/{name}/{filename}",
        description="Pattern for temporary storage paths"
    )
    final: str = Field(
        default="data/{name}/{filename}",
        description="Pattern for final storage paths"
    )


class BatchSettings(BaseModel):
    """Batch processing settings."""
    size: int = Field(
        default=10_000,
        description="Number of rows per batch",
        gt=0
    )
    row_multiplier: int = Field(
        default=300_000,
        description="Progress logging interval in rows",
        gt=0
    )


class Settings(BaseModel):
    """Global settings for hygge."""
    paths: PathSettings = PathSettings()
    batching: BatchSettings = BatchSettings()
    timeouts: TimeoutSettings = TimeoutSettings()


settings = Settings()
