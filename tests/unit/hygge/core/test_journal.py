"""
Tests for Journal functionality.

Following hygge's testing principles:
- Test behavior that matters to users
- Focus on data integrity and reliability
- Test watermark queries (primary use case)
- Verify run recording and aggregation
"""
import asyncio
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest
from pydantic import ValidationError

from hygge.core.journal import Journal, JournalConfig
from hygge.utility.exceptions import ConfigError
from hygge.utility.run_id import generate_run_id


@pytest.fixture
def temp_journal_dir(temp_dir):
    """Create temporary directory for journal files."""
    journal_dir = temp_dir / ".hygge_journal"
    journal_dir.mkdir(parents=True, exist_ok=True)
    return journal_dir


@pytest.fixture
def journal_config_explicit_path(temp_journal_dir):
    """Journal config with explicit path."""
    return JournalConfig(path=str(temp_journal_dir))


@pytest.fixture
def journal_config_store_location():
    """Journal config with store location inference."""
    return JournalConfig(location="store")


@pytest.fixture
def journal_config_home_location():
    """Journal config with home location inference."""
    return JournalConfig(location="home")


@pytest.fixture
def journal(temp_journal_dir):
    """Create a journal instance for testing."""
    config = JournalConfig(path=str(temp_journal_dir))
    return Journal(
        "test_journal",
        config,
        coordinator_name="test_coordinator",
        store_path=None,
        home_path=None,
    )


@pytest.fixture
def sample_entity_run_data():
    """Sample data for recording entity runs."""
    start_time = datetime.now(timezone.utc)
    finish_time = datetime.now(timezone.utc)
    return {
        "coordinator_run_id": generate_run_id(
            ["test_coordinator", start_time.isoformat()]
        ),
        "flow_run_id": generate_run_id(
            ["test_coordinator", "users_flow", start_time.isoformat()]
        ),
        "coordinator": "test_coordinator",
        "flow": "users_flow",
        "entity": "users",
        "start_time": start_time,
        "finish_time": finish_time,
        "status": "success",
        "run_type": "full_drop",
        "row_count": 1000,
        "duration": 5.0,
        "primary_key": "user_id",
        "watermark_column": "signup_date",
        "watermark_type": "datetime",
        "watermark": "2024-01-01T09:00:00Z",
        "message": None,
    }


class TestJournalConfig:
    """Test suite for JournalConfig."""

    def test_journal_config_explicit_path(self, temp_journal_dir):
        """Test journal config with explicit path."""
        config = JournalConfig(path=str(temp_journal_dir))
        assert config.path == str(temp_journal_dir)
        assert config.location == "store"  # Default

    def test_journal_config_store_location(self):
        """Test journal config with store location."""
        config = JournalConfig(location="store")
        assert config.location == "store"
        assert config.path is None

    def test_journal_config_home_location(self):
        """Test journal config with home location."""
        config = JournalConfig(location="home")
        assert config.location == "home"
        assert config.path is None

    def test_journal_config_default_location(self):
        """Test journal config uses default location."""
        config = JournalConfig()
        assert config.location == "store"  # Default
        assert config.path is None

    def test_journal_config_invalid_location(self):
        """Test journal config rejects invalid location."""
        with pytest.raises(ValidationError):
            JournalConfig(location="invalid")


class TestJournalInitialization:
    """Test suite for Journal initialization."""

    def test_journal_init_explicit_path(self, temp_journal_dir):
        """Test journal initialization with explicit path."""
        config = JournalConfig(path=str(temp_journal_dir))
        journal = Journal(
            "test_journal",
            config,
            coordinator_name="test_coordinator",
        )
        assert journal.name == "test_journal"
        assert journal.coordinator_name == "test_coordinator"
        assert journal.journal_path == temp_journal_dir / "journal.parquet"

    def test_journal_init_store_location(self, temp_dir):
        """Test journal initialization with store location inference."""
        store_path = str(temp_dir / "store")
        config = JournalConfig(location="store")
        journal = Journal(
            "test_journal",
            config,
            coordinator_name="test_coordinator",
            store_path=store_path,
        )
        expected_path = Path(store_path) / ".hygge_journal" / "journal.parquet"
        assert journal.journal_path == expected_path

    def test_journal_init_home_location(self, temp_dir):
        """Test journal initialization with home location inference."""
        home_path = str(temp_dir / "home")
        config = JournalConfig(location="home")
        journal = Journal(
            "test_journal",
            config,
            coordinator_name="test_coordinator",
            home_path=home_path,
        )
        expected_path = Path(home_path) / ".hygge_journal" / "journal.parquet"
        assert journal.journal_path == expected_path

    def test_journal_init_store_location_missing_path(self):
        """Test journal initialization fails if store path missing."""
        config = JournalConfig(location="store")
        with pytest.raises(ConfigError, match="store_path"):
            Journal("test_journal", config, coordinator_name="test_coordinator")

    def test_journal_init_home_location_missing_path(self):
        """Test journal initialization fails if home path missing."""
        config = JournalConfig(location="home")
        with pytest.raises(ConfigError, match="home_path"):
            Journal("test_journal", config, coordinator_name="test_coordinator")

    def test_journal_init_no_path_no_location(self):
        """Test journal initialization fails if no path or location."""
        config = JournalConfig(path=None, location=None)
        with pytest.raises(ConfigError, match="'path' or 'location'"):
            Journal("test_journal", config, coordinator_name="test_coordinator")

    def test_journal_creates_directory(self, temp_dir):
        """Test journal creates directory if it doesn't exist."""
        journal_dir = temp_dir / "new_journal"
        config = JournalConfig(path=str(journal_dir))
        journal = Journal(
            "test_journal",
            config,
            coordinator_name="test_coordinator",
        )
        assert journal_dir.exists()
        assert journal_dir.is_dir()
        assert journal.journal_path == journal_dir / "journal.parquet"


class TestRunIdGeneration:
    """Test suite for run ID generation."""

    def test_run_id_deterministic(self):
        """Test run IDs are deterministic."""
        components = ["coordinator", "flow", "entity", "2024-01-01T10:00:00Z"]
        id1 = generate_run_id(components)
        id2 = generate_run_id(components)
        assert id1 == id2

    def test_run_id_unique(self):
        """Test run IDs are unique for different inputs."""
        components1 = ["coordinator", "flow", "entity", "2024-01-01T10:00:00Z"]
        components2 = ["coordinator", "flow", "entity", "2024-01-01T10:00:01Z"]
        id1 = generate_run_id(components1)
        id2 = generate_run_id(components2)
        assert id1 != id2

    def test_run_id_length(self):
        """Test run IDs have correct length."""
        components = ["coordinator", "flow", "entity", "2024-01-01T10:00:00Z"]
        run_id = generate_run_id(components)
        assert len(run_id) == 32  # 32-character hex string

    def test_run_id_coordinator_format(self):
        """Test coordinator run ID format."""
        coordinator_name = "test_coordinator"
        start_time = "2024-01-01T10:00:00Z"
        run_id = generate_run_id([coordinator_name, start_time])
        assert len(run_id) == 32
        assert isinstance(run_id, str)

    def test_run_id_flow_format(self):
        """Test flow run ID format."""
        coordinator_name = "test_coordinator"
        flow_name = "users_flow"
        start_time = "2024-01-01T10:00:00Z"
        run_id = generate_run_id([coordinator_name, flow_name, start_time])
        assert len(run_id) == 32
        assert isinstance(run_id, str)


class TestJournalRecordEntityRun:
    """Test suite for recording entity runs."""

    @pytest.mark.asyncio
    async def test_record_entity_run_success(self, journal, sample_entity_run_data):
        """Test recording successful entity run."""
        entity_run_id = await journal.record_entity_run(**sample_entity_run_data)

        # Verify run ID returned
        assert entity_run_id is not None
        assert len(entity_run_id) == 32

        # Verify journal file exists
        assert journal.journal_path.exists()

        # Verify journal file can be read
        journal_df = pl.read_parquet(journal.journal_path)
        assert len(journal_df) == 1

        # Verify data recorded correctly
        row = journal_df.to_dicts()[0]
        assert row["coordinator"] == "test_coordinator"
        assert row["flow"] == "users_flow"
        assert row["entity"] == "users"
        assert row["status"] == "success"
        assert row["row_count"] == 1000
        assert row["watermark"] == "2024-01-01T09:00:00Z"

        # Ensure no temporary journal files remain
        temp_files = list(journal.journal_path.parent.glob("journal.parquet.tmp_*"))
        assert not temp_files

    @pytest.mark.asyncio
    async def test_record_entity_run_failure(self, journal):
        """Test recording failed entity run."""
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)
        entity_run_id = await journal.record_entity_run(
            coordinator_run_id=generate_run_id(
                ["test_coordinator", start_time.isoformat()]
            ),
            flow_run_id=generate_run_id(
                ["test_coordinator", "users_flow", start_time.isoformat()]
            ),
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time,
            finish_time=finish_time,
            status="fail",
            run_type="full_drop",
            row_count=None,
            duration=2.0,
            message="Connection timeout",
        )
        assert isinstance(entity_run_id, str)
        assert len(entity_run_id) == 32

        # Verify journal file exists
        assert journal.journal_path.exists()

        # Verify data recorded correctly
        journal_df = pl.read_parquet(journal.journal_path)
        row = journal_df.to_dicts()[0]
        assert row["status"] == "fail"
        assert row["message"] == "Connection timeout"
        assert row["row_count"] is None

    @pytest.mark.asyncio
    async def test_record_entity_run_skip(self, journal):
        """Test recording skipped entity run."""
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)
        await journal.record_entity_run(
            coordinator_run_id=generate_run_id(
                ["test_coordinator", start_time.isoformat()]
            ),
            flow_run_id=generate_run_id(
                ["test_coordinator", "users_flow", start_time.isoformat()]
            ),
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time,
            finish_time=finish_time,
            status="skip",
            run_type="incremental",
            row_count=0,
            duration=1.0,
            message="No new data found",
        )

        # Verify data recorded correctly
        journal_df = pl.read_parquet(journal.journal_path)
        row = journal_df.to_dicts()[0]
        assert row["status"] == "skip"
        assert row["message"] == "No new data found"
        assert row["row_count"] == 0

    @pytest.mark.asyncio
    async def test_record_entity_run_append(self, journal, sample_entity_run_data):
        """Test recording multiple entity runs appends to journal."""
        # Record first run
        await journal.record_entity_run(**sample_entity_run_data)

        # Record second run with different entity
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)
        second_run = sample_entity_run_data.copy()
        second_run["entity"] = "orders"
        second_run["flow_run_id"] = generate_run_id(
            ["test_coordinator", "users_flow", start_time.isoformat()]
        )
        second_run["start_time"] = start_time
        second_run["finish_time"] = finish_time
        await journal.record_entity_run(**second_run)

        # Verify both runs recorded
        journal_df = pl.read_parquet(journal.journal_path)
        assert len(journal_df) == 2

        # Verify both entities present
        entities = journal_df["entity"].to_list()
        assert "users" in entities
        assert "orders" in entities

    @pytest.mark.asyncio
    async def test_record_entity_run_concurrent_appends(self, journal):
        """Test concurrent writes preserve all entries."""

        async def record_entity(entity: str) -> str:
            start_time = datetime.now(timezone.utc)
            finish_time = datetime.now(timezone.utc)
            return await journal.record_entity_run(
                coordinator_run_id=generate_run_id(
                    ["test_coordinator", start_time.isoformat()]
                ),
                flow_run_id=generate_run_id(
                    ["test_coordinator", "users_flow", entity, start_time.isoformat()]
                ),
                coordinator="test_coordinator",
                flow="users_flow",
                entity=entity,
                start_time=start_time,
                finish_time=finish_time,
                status="success",
                run_type="incremental",
                row_count=10,
                duration=1.0,
            )

        await asyncio.gather(
            record_entity("users"),
            record_entity("orders"),
        )

        journal_df = pl.read_parquet(journal.journal_path)
        assert len(journal_df) == 2
        assert set(journal_df["entity"].to_list()) == {"users", "orders"}

    @pytest.mark.asyncio
    async def test_record_entity_run_no_watermark(self, journal):
        """Test recording entity run without watermark."""
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)
        await journal.record_entity_run(
            coordinator_run_id=generate_run_id(
                ["test_coordinator", start_time.isoformat()]
            ),
            flow_run_id=generate_run_id(
                ["test_coordinator", "users_flow", start_time.isoformat()]
            ),
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time,
            finish_time=finish_time,
            status="success",
            run_type="full_drop",
            row_count=1000,
            duration=5.0,
        )

        # Verify watermark fields are null
        journal_df = pl.read_parquet(journal.journal_path)
        row = journal_df.to_dicts()[0]
        assert row["primary_key"] is None
        assert row["watermark_column"] is None
        assert row["watermark"] is None


class TestJournalWatermark:
    """Test suite for watermark queries."""

    @pytest.mark.asyncio
    async def test_get_watermark_success(self, journal, sample_entity_run_data):
        """Test getting watermark for entity."""
        # Record run with watermark
        await journal.record_entity_run(**sample_entity_run_data)

        # Get watermark
        watermark = await journal.get_watermark(
            "users_flow",
            entity="users",
            primary_key="user_id",
            watermark_column="signup_date",
        )

        # Verify watermark returned
        assert watermark is not None
        assert watermark["watermark"] == "2024-01-01T09:00:00Z"
        assert watermark["watermark_type"] == "datetime"
        assert watermark["watermark_column"] == "signup_date"
        assert watermark["primary_key"] == "user_id"

    @pytest.mark.asyncio
    async def test_get_watermark_no_existing(self, journal):
        """Test getting watermark when none exists."""
        watermark = await journal.get_watermark("users_flow", entity="users")
        assert watermark is None

    @pytest.mark.asyncio
    async def test_get_watermark_most_recent(self, journal):
        """Test getting most recent watermark."""
        # Record first run
        start_time1 = datetime.now(timezone.utc)
        finish_time1 = datetime.now(timezone.utc)
        await journal.record_entity_run(
            coordinator_run_id=generate_run_id(
                ["test_coordinator", start_time1.isoformat()]
            ),
            flow_run_id=generate_run_id(
                ["test_coordinator", "users_flow", start_time1.isoformat()]
            ),
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time1,
            finish_time=finish_time1,
            status="success",
            run_type="full_drop",
            row_count=1000,
            duration=5.0,
            primary_key="user_id",
            watermark_column="signup_date",
            watermark_type="datetime",
            watermark="2024-01-01T09:00:00Z",
        )

        # Record second run with later watermark
        await asyncio.sleep(0.1)  # Ensure different timestamps
        start_time2 = datetime.now(timezone.utc)
        finish_time2 = datetime.now(timezone.utc)
        await journal.record_entity_run(
            coordinator_run_id=generate_run_id(
                ["test_coordinator", start_time2.isoformat()]
            ),
            flow_run_id=generate_run_id(
                ["test_coordinator", "users_flow", start_time2.isoformat()]
            ),
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time2,
            finish_time=finish_time2,
            status="success",
            run_type="incremental",
            row_count=500,
            duration=3.0,
            primary_key="user_id",
            watermark_column="signup_date",
            watermark_type="datetime",
            watermark="2024-01-02T09:00:00Z",
        )

        # Get watermark - should return most recent
        watermark = await journal.get_watermark("users_flow", entity="users")
        assert watermark is not None
        assert watermark["watermark"] == "2024-01-02T09:00:00Z"

    @pytest.mark.asyncio
    async def test_get_watermark_config_mismatch(self, journal):
        """Test watermark config mismatch detection."""
        # Record run with specific config
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)
        await journal.record_entity_run(
            coordinator_run_id=generate_run_id(
                ["test_coordinator", start_time.isoformat()]
            ),
            flow_run_id=generate_run_id(
                ["test_coordinator", "users_flow", start_time.isoformat()]
            ),
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time,
            finish_time=finish_time,
            status="success",
            run_type="full_drop",
            row_count=1000,
            duration=5.0,
            primary_key="user_id",
            watermark_column="signup_date",
            watermark_type="datetime",
            watermark="2024-01-01T09:00:00Z",
        )

        # Try to get watermark with different config
        with pytest.raises(ValueError, match="Watermark config mismatch"):
            await journal.get_watermark(
                "users_flow",
                entity="users",
                primary_key="user_id",
                watermark_column="updated_at",  # Different column
            )

    @pytest.mark.asyncio
    async def test_get_watermark_only_successful(self, journal):
        """Test watermark query only returns successful runs."""
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)

        # Record failed run with watermark
        await journal.record_entity_run(
            coordinator_run_id=generate_run_id(
                ["test_coordinator", start_time.isoformat()]
            ),
            flow_run_id=generate_run_id(
                ["test_coordinator", "users_flow", start_time.isoformat()]
            ),
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time,
            finish_time=finish_time,
            status="fail",
            run_type="full_drop",
            row_count=None,
            duration=2.0,
            primary_key="user_id",
            watermark_column="signup_date",
            watermark_type="datetime",
            watermark="2024-01-01T09:00:00Z",
            message="Error occurred",
        )

        # Get watermark - should return None (only successful runs)
        watermark = await journal.get_watermark("users_flow", entity="users")
        assert watermark is None


class TestJournalAggregations:
    """Test suite for journal aggregations."""

    @pytest.mark.asyncio
    async def test_get_flow_summary(self, journal):
        """Test getting flow summary aggregation."""
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)
        flow_run_id = generate_run_id(
            ["test_coordinator", "users_flow", start_time.isoformat()]
        )

        # Record multiple entity runs in same flow
        entities = ["users", "orders", "products"]
        for i, entity in enumerate(entities):
            await journal.record_entity_run(
                coordinator_run_id=generate_run_id(
                    ["test_coordinator", start_time.isoformat()]
                ),
                flow_run_id=flow_run_id,
                coordinator="test_coordinator",
                flow="users_flow",
                entity=entity,
                start_time=start_time,
                finish_time=finish_time,
                status="success" if i < 2 else "fail",  # First 2 success, last fails
                run_type="full_drop",
                row_count=1000,
                duration=5.0,
            )

        # Get flow summary
        summary = await journal.get_flow_summary(flow_run_id)

        # Verify summary
        assert summary["n_entities"] == 3
        assert summary["n_success"] == 2
        assert summary["n_fail"] == 1
        assert summary["n_skip"] == 0

    @pytest.mark.asyncio
    async def test_get_coordinator_summary(self, journal):
        """Test getting coordinator summary aggregation."""
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)
        coordinator_run_id = generate_run_id(
            ["test_coordinator", start_time.isoformat()]
        )

        # Record runs for multiple flows
        flows = ["users_flow", "orders_flow"]
        for flow in flows:
            flow_run_id = generate_run_id(
                ["test_coordinator", flow, start_time.isoformat()]
            )
            await journal.record_entity_run(
                coordinator_run_id=coordinator_run_id,
                flow_run_id=flow_run_id,
                coordinator="test_coordinator",
                flow=flow,
                entity="users",
                start_time=start_time,
                finish_time=finish_time,
                status="success",
                run_type="full_drop",
                row_count=1000,
                duration=5.0,
            )

        # Get coordinator summary
        summary = await journal.get_coordinator_summary(coordinator_run_id)

        # Verify summary
        assert summary["n_flows"] == 2
        # n_entities counts unique entities, not total (both flows have "users")
        assert summary["n_entities"] == 1

    @pytest.mark.asyncio
    async def test_get_flow_summary_empty(self, journal):
        """Test getting flow summary when no runs exist."""
        flow_run_id = generate_run_id(
            ["test_coordinator", "users_flow", "2024-01-01T10:00:00Z"]
        )
        summary = await journal.get_flow_summary(flow_run_id)

        assert summary["n_entities"] == 0
        assert summary["n_success"] == 0
        assert summary["n_fail"] == 0
        assert summary["n_skip"] == 0

    @pytest.mark.asyncio
    async def test_get_coordinator_summary_empty(self, journal):
        """Test getting coordinator summary when no runs exist."""
        coordinator_run_id = generate_run_id(
            ["test_coordinator", "2024-01-01T10:00:00Z"]
        )
        summary = await journal.get_coordinator_summary(coordinator_run_id)

        assert summary["n_flows"] == 0
        assert summary["n_entities"] == 0
