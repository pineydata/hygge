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
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

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

        # State tracking
        self.total_rows = 0
        self.batches_processed = 0
        self.start_time = None
        self.end_time = None
        self.duration: float = 0.0
        self.entity_start_time: Optional[datetime] = None
        self.written_data: Optional[pl.DataFrame] = None  # Accumulate for watermark

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
            await self._record_entity_run(status="success")

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
            async for batch in self.home.read():
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

                    # Accumulate data for watermark extraction
                    if self.watermark_config and self.written_data is None:
                        self.written_data = batch
                    elif self.watermark_config and self.written_data is not None:
                        self.written_data = pl.concat([self.written_data, batch])

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
            entity = self.entity_name
            if not entity and self.name != self.base_flow_name:
                # Entity flow: extract entity name from flow name
                # Flow name format: {base_flow_name}_{entity_name}
                if self.name.startswith(f"{self.base_flow_name}_"):
                    entity = self.name[len(f"{self.base_flow_name}_") :]
                else:
                    entity = None

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
                and self.written_data is not None
                and len(self.written_data) > 0
            ):
                primary_key = self.watermark_config.get("primary_key")
                watermark_column = self.watermark_config.get("watermark_column")

                if primary_key and watermark_column:
                    try:
                        # Validate primary key exists
                        # (for reference, not used in watermark value)
                        if primary_key not in self.written_data.columns:
                            self.logger.warning(
                                f"Primary key '{primary_key}' not found in data"
                            )

                        # Extract max watermark value
                        if watermark_column in self.written_data.columns:
                            max_watermark = self.written_data[watermark_column].max()

                            # Infer watermark type from DataFrame column
                            watermark_dtype = self.written_data[watermark_column].dtype
                            if watermark_dtype == pl.Datetime:
                                watermark_type = "datetime"
                                watermark_value = max_watermark.isoformat()
                            elif watermark_dtype in [pl.Int64, pl.Int32, pl.UInt64]:
                                watermark_type = "int"
                                watermark_value = str(max_watermark)
                            elif watermark_dtype == pl.Utf8:
                                watermark_type = "string"
                                watermark_value = str(max_watermark)
                            else:
                                self.logger.warning(
                                    f"Unsupported watermark type: {watermark_dtype}"
                                )
                        else:
                            self.logger.warning(
                                f"Watermark column '{watermark_column}' "
                                f"not found in data"
                            )
                    except Exception as e:
                        self.logger.warning(f"Failed to extract watermark: {str(e)}")

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
            "Flow-level strategy: true for full reload (drops/recreates tables), "
            "false for incremental updates. "
            "If not set, uses store-level configuration."
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
