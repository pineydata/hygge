"""
Flow manages the movement of data from a single data set between Home to Store.

Implements a producer-consumer pattern to efficiently move data batches
from a source (Home) to a destination (Store), with proper error handling,
retries, and state management.

hygge is built on Polars + PyArrow for data movement.
Flows orchestrate the movement of Polars DataFrames from Home to Store.
"""
import asyncio
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Union

import polars as pl
from pydantic import BaseModel, Field, field_validator

from hygge.utility.exceptions import FlowError
from hygge.utility.logger import get_logger

from .home import Home, HomeConfig
from .journal import Journal, JournalConfig
from .store import Store, StoreConfig


class Flow:
    """
    Flow orchestrates data movement between a Home and Store.

    Responsibilities:
    - Manages producer-consumer pattern for data movement
    - Coordinates batch processing and state
    - Handles retries and error recovery
    - Tracks progress and performance

    Args:
        name (str): Name of this flow
        home (Home): Source to read data from
        store (Store): Destination to write data to
        options (Dict[str, Any], optional): Configuration options
            - queue_size (int): Size of batch queue (default: 10)
            - timeout (int): Operation timeout in seconds (default: 300)
    """

    def __init__(
        self,
        name: str,
        home: Home,
        store: Store,
        options: Optional[Dict[str, Any]] = None,
        journal: Optional[Journal] = None,
        coordinator_run_id: Optional[str] = None,
        flow_run_id: Optional[str] = None,
        coordinator_name: Optional[str] = None,
        base_flow_name: Optional[str] = None,
        entity_name: Optional[str] = None,
        run_type: Optional[str] = None,
        watermark_config: Optional[Dict[str, str]] = None,
    ):
        self.name = name
        self.home = home
        self.store = store
        self.options = options or {}

        # Default settings
        self.queue_size = self.options.get("queue_size", 10)
        self.timeout = self.options.get("timeout", 300)

        # Journal integration
        self.journal = journal
        self.coordinator_run_id = coordinator_run_id
        self.flow_run_id = flow_run_id
        self.coordinator_name = coordinator_name
        self.base_flow_name = base_flow_name or name
        self.entity_name = entity_name
        self.run_type = run_type or "full_drop"
        self.watermark_config = watermark_config

        # Allow stores to adjust their strategy (truncate vs append) per run
        if hasattr(self.store, "configure_for_run"):
            self.store.configure_for_run(self.run_type)

        self.initial_watermark_info: Optional[Dict[str, Any]] = None
        self.watermark_message: Optional[str] = None

        # State tracking
        self.total_rows = 0
        self.batches_processed = 0
        self.start_time = None
        self.end_time = None
        self.duration: float = 0.0
        self.entity_start_time: Optional[datetime] = None
        self._watermark_candidate: Optional[Any] = None
        self._watermark_type: Optional[str] = None
        self._watermark_primary_key_warned = False
        self._watermark_column_warned = False

        # Progress callback for coordinator-level tracking
        self.progress_callback = None

        # Set up flow-scoped logging
        self.logger = get_logger(f"hygge.flow.{name}")

        # Assign child loggers to home and store for clear log attribution
        self.home.logger = get_logger(f"hygge.flow.{name}.home")
        self.store.logger = get_logger(f"hygge.flow.{name}.store")

    def set_progress_callback(
        self, callback: Optional[Callable[[int], Awaitable[None]]]
    ) -> None:
        """Set callback for coordinator-level progress tracking."""
        self.progress_callback = callback

    async def start(self) -> None:
        """Start the flow from Home to Store."""
        # START line already logged by Coordinator (dbt-style)
        self.start_time = asyncio.get_event_loop().time()
        self.entity_start_time = datetime.now(timezone.utc)

        producer = None
        consumer = None

        try:
            queue = asyncio.Queue(maxsize=self.queue_size)
            producer_done = asyncio.Event()

            self._reset_watermark_tracker()
            await self._prepare_incremental_context()

            # Start producer and consumer tasks
            producer = asyncio.create_task(
                self._producer(queue, producer_done), name=f"{self.name}_producer"
            )
            consumer = asyncio.create_task(
                self._consumer(queue, producer_done), name=f"{self.name}_consumer"
            )

            # Wait for producer to finish
            await producer

            # Wait for consumer to finish - this will either complete or raise
            consumer_exception = None
            try:
                await consumer
            except Exception as e:
                consumer_exception = e

            # If consumer had an exception, don't wait for queue.join()
            # since the consumer failed and task_done() may not have been called
            if consumer_exception is None:
                await queue.join()

            # Ensure all data is written (only if no consumer error)
            if consumer_exception is None:
                await self.store.finish()
            else:
                raise consumer_exception

            self.end_time = asyncio.get_event_loop().time()
            self.duration = self.end_time - self.start_time if self.start_time else 0.0
            rate = self.total_rows / self.duration if self.duration > 0 else 0
            # Coordinator already logs OK line (dbt-style), so this is DEBUG
            self.logger.debug(
                f"Flow {self.name} completed: "
                f"{self.total_rows:,} rows in {self.duration:.1f}s ({rate:.0f} rows/s)"
            )

            # Record entity run in journal (if enabled)
            await self._record_entity_run(
                status="success", message=self.watermark_message
            )

        except Exception as e:
            # Capture duration even on failure
            if self.start_time:
                self.end_time = asyncio.get_event_loop().time()
                self.duration = self.end_time - self.start_time
            self.logger.error(f"Flow failed: {self.name}, error: {str(e)}")

            # Record entity run in journal (if enabled) with failure status
            await self._record_entity_run(status="fail", message=str(e))

            # Cancel tasks if they're still running
            if producer and not producer.done():
                producer.cancel()
            if consumer and not consumer.done():
                consumer.cancel()

            # Wait for cancellation to complete
            if producer:
                try:
                    await producer
                except asyncio.CancelledError:
                    pass
            if consumer:
                try:
                    await consumer
                except asyncio.CancelledError:
                    pass

            raise FlowError(f"Flow failed: {str(e)}")

    async def _producer(
        self, queue: asyncio.Queue, producer_done: asyncio.Event
    ) -> None:
        """Read batches from Home and put them in queue."""
        try:
            self.logger.debug(f"Starting producer for {self.name}")
            async for batch in self._iterate_home_batches():
                if batch is not None:
                    await queue.put(batch)
                    self.logger.debug(
                        f"Queued batch of {len(batch)} rows, "
                        f"queue size: {queue.qsize()}/{queue.maxsize}"
                    )

            # Signal that producer is done before sending end signal
            producer_done.set()
            # Signal end of data
            await queue.put(None)
            self.logger.debug("Producer completed, sent end signal")

        except Exception as e:
            self.logger.error(f"Producer error: {str(e)}")
            producer_done.set()  # Signal done even on error
            raise FlowError(f"Producer failed: {str(e)}")

    async def _iterate_home_batches(self) -> AsyncIterator[pl.DataFrame]:
        """Yield batches from the home, applying watermark filtering when available."""
        if self.initial_watermark_info and hasattr(self.home, "read_with_watermark"):
            async for batch in self.home.read_with_watermark(
                self.initial_watermark_info
            ):
                yield batch
        else:
            if self.initial_watermark_info and not hasattr(
                self.home, "read_with_watermark"
            ):
                self.logger.warning(
                    f"Home '{self.home.__class__.__name__}' does not support "
                    "watermark-based reads. Performing full load."
                )
            async for batch in self.home.read():
                yield batch

    def _reset_watermark_tracker(self) -> None:
        """Reset watermark aggregation state for a new run."""
        self._watermark_candidate = None
        self._watermark_type = None
        self._watermark_primary_key_warned = False
        self._watermark_column_warned = False

    def _update_watermark_tracker(self, batch: pl.DataFrame) -> None:
        """Update tracked watermark value based on a batch."""
        if not self.watermark_config:
            return

        primary_key = self.watermark_config.get("primary_key")
        watermark_column = self.watermark_config.get("watermark_column")

        if not primary_key or not watermark_column:
            return

        if primary_key not in batch.columns and not self._watermark_primary_key_warned:
            self.logger.warning(f"Primary key '{primary_key}' not found in data")
            self._watermark_primary_key_warned = True

        if watermark_column not in batch.columns:
            if not self._watermark_column_warned:
                self.logger.warning(
                    f"Watermark column '{watermark_column}' not found in data"
                )
                self._watermark_column_warned = True
            return

        column_series = batch[watermark_column]
        if column_series.is_null().all():
            return

        dtype = column_series.dtype
        candidate_type: Optional[str] = None
        candidate_value: Optional[Any] = None

        if dtype == pl.Datetime:
            candidate_type = "datetime"
            candidate_value = column_series.max()
        elif dtype in (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        ):
            candidate_type = "int"
            candidate_value = int(column_series.max())
        elif dtype == pl.Utf8:
            candidate_type = "string"
            candidate_value = str(column_series.max())
        else:
            if not self._watermark_column_warned:
                self.logger.warning(f"Unsupported watermark type: {dtype}")
                self._watermark_column_warned = True
            return

        if candidate_value is None:
            return

        if self._watermark_candidate is None:
            self._watermark_candidate = candidate_value
            self._watermark_type = candidate_type
            return

        if self._watermark_type != candidate_type:
            self.logger.warning(
                f"Inconsistent watermark types across batches: "
                f"{self._watermark_type} vs {candidate_type}"
            )
            return

        if candidate_value > self._watermark_candidate:
            self._watermark_candidate = candidate_value

    async def _prepare_incremental_context(self) -> None:
        """Resolve watermark context for incremental runs."""
        self.initial_watermark_info = None
        self.watermark_message = None

        if self.run_type != "incremental":
            return

        if not self.journal or not self.watermark_config:
            return

        entity_identifier = self._resolve_entity_name() or self.base_flow_name

        try:
            watermark_info = await self.journal.get_watermark(
                self.base_flow_name,
                entity=entity_identifier,
                primary_key=self.watermark_config.get("primary_key"),
                watermark_column=self.watermark_config.get("watermark_column"),
            )
            if watermark_info:
                self.initial_watermark_info = watermark_info
                watermark_val = watermark_info.get("watermark")
                watermark_type = watermark_info.get("watermark_type")
                self.logger.debug(
                    f"Using watermark {watermark_val} "
                    f"(type={watermark_type}) for flow {self.name}"
                )
        except ValueError as exc:
            warning_message = (
                f"Watermark config mismatch for {self.name}: {str(exc)}. "
                "Proceeding with full load and recording new watermark."
            )
            self.logger.warning(warning_message)
            self.watermark_message = warning_message
        except Exception as exc:
            self.logger.warning(
                f"Failed to retrieve watermark for {self.name}: {str(exc)}"
            )

    async def _consumer(
        self, queue: asyncio.Queue, producer_done: asyncio.Event
    ) -> None:
        """Process batches from queue and write to Store."""
        try:
            self.logger.debug(f"Starting consumer for {self.name}")
            while True:
                batch = await queue.get()

                if batch is None:  # End of data signal
                    self.logger.debug("Consumer received end signal")
                    queue.task_done()
                    break

                try:
                    # Write to store
                    await self.store.write(batch)

                    self._update_watermark_tracker(batch)

                    # Update metrics
                    batch_rows = len(batch)
                    self.total_rows += batch_rows
                    self.batches_processed += 1

                    # Notify coordinator of progress for milestone tracking
                    if self.progress_callback:
                        await self.progress_callback(batch_rows)

                    # Progress logging now handled by store with combined message

                except Exception as e:
                    self.logger.error(f"Failed to process batch: {str(e)}")
                    # Signal producer to stop by putting None in queue (non-blocking)
                    try:
                        queue.put_nowait(None)
                    except Exception:
                        pass
                    raise FlowError(f"Batch processing failed: {str(e)}")

                finally:
                    queue.task_done()

        except Exception as e:
            self.logger.error(f"Consumer error: {str(e)}")
            raise FlowError(f"Consumer failed: {str(e)}")

    async def _record_entity_run(
        self, status: str, message: Optional[str] = None
    ) -> None:
        """
        Record entity run in journal (if enabled).

        Args:
            status: Run status: "success", "fail", or "skip"
            message: Optional message (error message, skip reason, etc.)
        """
        if not self.journal:
            return

        if not self.coordinator_run_id or not self.flow_run_id:
            self.logger.warning(
                "Journal enabled but run IDs not provided - skipping journal record"
            )
            return

        try:
            # Extract entity name (if not provided, extract from flow name)
            entity = self._resolve_entity_name()

            # Determine final status
            if status == "success" and self.total_rows == 0:
                final_status = "skip"
                final_message = message or "No rows processed"
            else:
                final_status = status
                final_message = message

            # Extract watermark if configured and successful
            watermark_value = None
            watermark_type = None
            primary_key = None
            watermark_column = None

            if (
                final_status == "success"
                and self.watermark_config
                and self._watermark_candidate is not None
            ):
                primary_key = self.watermark_config.get("primary_key")
                watermark_column = self.watermark_config.get("watermark_column")

                if primary_key and watermark_column:
                    watermark_type = self._watermark_type
                    candidate = self._watermark_candidate

                    if watermark_type == "datetime":
                        watermark_value = candidate.isoformat()
                    elif watermark_type == "int":
                        watermark_value = str(candidate)
                    elif watermark_type == "string":
                        watermark_value = candidate
                    else:
                        self.logger.warning(
                            "Unable to serialize watermark value "
                            f"of type {watermark_type}"
                        )
                        watermark_value = None

            # Record in journal
            finish_time = datetime.now(timezone.utc)
            await self.journal.record_entity_run(
                coordinator_run_id=self.coordinator_run_id,
                flow_run_id=self.flow_run_id,
                coordinator=self.coordinator_name or "unknown",
                flow=self.base_flow_name,
                entity=entity or self.base_flow_name,
                start_time=self.entity_start_time or datetime.now(timezone.utc),
                finish_time=finish_time,
                status=final_status,
                run_type=self.run_type,
                row_count=self.total_rows if self.total_rows > 0 else None,
                duration=self.duration,
                primary_key=primary_key,
                watermark_column=watermark_column,
                watermark_type=watermark_type,
                watermark=watermark_value,
                message=final_message,
            )

        except Exception as e:
            # Journal failures should not break flows
            self.logger.warning(f"Failed to record entity run in journal: {str(e)}")

    def _resolve_entity_name(self) -> Optional[str]:
        """Resolve entity name for journal lookups."""
        if self.entity_name:
            return self.entity_name
        if self.name != self.base_flow_name and self.name.startswith(
            f"{self.base_flow_name}_"
        ):
            return self.name[len(f"{self.base_flow_name}_") :]
        return None


class FlowConfig(BaseModel):
    """
    Configuration for a data flow.

    Supports both simple and advanced configurations:

    Simple (Rails spirit - convention over configuration):
    ```yaml
    flows:
      users_to_lake:
        home: data/users.parquet
        store: data/lake/users
    ```

    Advanced (full control):
    ```yaml
    flows:
      users_to_lake:
        home:
          type: sql
          table: users
          connection: ${DATABASE_URL}
        store:
          type: parquet
          path: data/lake/users
          options:
            compression: snappy
    ```
    """

    # Clean, simple configuration - only home/store
    home: Union[str, Dict[str, Any]] = Field(..., description="Home configuration")
    store: Union[str, Dict[str, Any]] = Field(..., description="Store configuration")
    queue_size: int = Field(
        default=10, ge=1, le=100, description="Size of internal queue"
    )
    timeout: int = Field(default=300, ge=1, description="Operation timeout in seconds")
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional flow options"
    )
    entities: Optional[Union[List[str], List[Dict[str, Any]]]] = Field(
        default=None, description="Entity names or definitions for this flow"
    )
    # Flow-level strategy: full_drop for full reloads
    full_drop: Optional[bool] = Field(
        default=None,
        description=(
            "Compatibility flag for legacy configs. When set, overrides "
            "the default run_type: true → run_type 'full_drop', "
            "false → run_type 'incremental'."
        ),
    )
    # Journal configuration (optional)
    journal: Optional[Union[Dict[str, Any], JournalConfig]] = Field(
        default=None,
        description="Journal configuration for tracking flow execution metadata",
    )
    # Watermark configuration (flow-level, applies to all entities unless overridden)
    watermark: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Watermark configuration for incremental loads. "
            "Applies to all entities unless overridden at entity level. "
            "Requires 'primary_key' and 'watermark_column'."
        ),
    )
    # Run type (flow-level default, can be overridden at entity level)
    run_type: Optional[str] = Field(
        default="full_drop",
        description=(
            "Run type for this flow: 'full_drop' (default) or 'incremental'. "
            "Can be overridden at entity level."
        ),
    )

    @field_validator("home", mode="before")
    @classmethod
    def validate_home(cls, v):
        """Validate home configuration structure."""
        if isinstance(v, str):
            # For strings, validate by trying to create HomeConfig
            try:
                HomeConfig.create(v)
            except Exception as e:
                # Re-raise the original ValidationError
                raise e
        elif isinstance(v, dict):
            if not v:
                raise ValueError("Home configuration cannot be empty")
            # Validate the structure by trying to create HomeConfig
            try:
                HomeConfig.create(v)
            except Exception as e:
                # Re-raise the original ValidationError
                raise e
        return v

    @field_validator("store", mode="before")
    @classmethod
    def validate_store(cls, v):
        """Validate store configuration structure."""
        if isinstance(v, str):
            # For strings, validate by trying to create StoreConfig
            try:
                StoreConfig.create(v)
            except Exception as e:
                # Re-raise the original ValidationError
                raise e
        elif isinstance(v, dict):
            if not v:
                raise ValueError("Store configuration cannot be empty")
            # Validate the structure by trying to create StoreConfig
            try:
                StoreConfig.create(v)
            except Exception as e:
                # Re-raise the original ValidationError
                raise e
        return v

    @field_validator("journal", mode="before")
    @classmethod
    def validate_journal(cls, v):
        """Ensure journal configuration is parsed into JournalConfig."""
        if v is None:
            return None
        if isinstance(v, JournalConfig):
            return v
        if isinstance(v, dict):
            return JournalConfig(**v)
        raise ValueError(
            "Journal configuration must be a dict or JournalConfig instance"
        )

    @property
    def home_instance(self) -> Home:
        """Get home instance - converts raw config to Home instance."""
        if isinstance(self.home, str):
            # Simple string configuration
            config = HomeConfig.create(self.home)
            return Home.create("flow_home", config)
        elif isinstance(self.home, dict):
            # Dictionary configuration - create Home instance
            config = HomeConfig.create(self.home)
            return Home.create("flow_home", config)
        else:
            # Already a Home instance
            return self.home

    @property
    def store_instance(self) -> Store:
        """Get store instance - converts raw config to Store instance."""
        if isinstance(self.store, str):
            # Simple string configuration
            config = StoreConfig.create(self.store)
            return Store.create("", config)
        elif isinstance(self.store, dict):
            # Dictionary configuration - create Store instance
            config = StoreConfig.create(self.store)
            return Store.create("", config)
        else:
            # Already a Store instance
            return self.store

    @property
    def home_config(self) -> HomeConfig:
        """Get home config - converts raw config to HomeConfig."""
        home_instance = self.home_instance
        return home_instance.config

    @property
    def store_config(self) -> StoreConfig:
        """Get store config - converts raw config to StoreConfig."""
        store_instance = self.store_instance
        return store_instance.config
