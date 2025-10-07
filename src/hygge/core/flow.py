"""
Flow manages the movement of data from a single data set between Home to Store.

Implements a producer-consumer pattern to efficiently move data batches
from a source (Home) to a destination (Store), with proper error handling,
retries, and state management.

hygge is built on Polars + PyArrow for data movement.
Flows orchestrate the movement of Polars DataFrames from Home to Store.
"""
import asyncio
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator

from hygge.utility.exceptions import FlowError
from hygge.utility.logger import get_logger

from .home import Home, HomeConfig
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
    ):
        self.name = name
        self.home = home
        self.store = store
        self.options = options or {}

        # Default settings
        self.queue_size = self.options.get("queue_size", 10)
        self.timeout = self.options.get("timeout", 300)

        # State tracking
        self.total_rows = 0
        self.batches_processed = 0
        self.start_time = None

        self.logger = get_logger(f"hygge.flow.{name}")

    async def start(self) -> None:
        """Start the flow from Home to Store."""
        self.logger.info(f"Starting flow: {self.name}")
        self.start_time = asyncio.get_event_loop().time()

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

            duration = asyncio.get_event_loop().time() - self.start_time
            rate = self.total_rows / duration if duration > 0 else 0
            self.logger.success(
                f"Flow {self.name} completed: "
                f"{self.total_rows:,} rows in {duration:.1f}s ({rate:.0f} rows/s)"
            )

        except Exception as e:
            self.logger.error(f"Flow failed: {self.name}, error: {str(e)}")

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

                    # Update metrics
                    self.total_rows += len(batch)
                    self.batches_processed += 1

                    # Log progress periodically
                    if self.batches_processed % 10 == 0:
                        duration = asyncio.get_event_loop().time() - self.start_time
                        rate = self.total_rows / duration if duration > 0 else 0
                        self.logger.info(
                            f"Processed {self.total_rows:,} rows "
                            f"in {duration:.1f}s ({rate:.0f} rows/s)"
                        )

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
    home: Union[str, Dict, Any] = Field(..., description="Home configuration")
    store: Union[str, Dict, Any] = Field(..., description="Store configuration")
    queue_size: int = Field(
        default=10, ge=1, le=100, description="Size of internal queue"
    )
    timeout: int = Field(default=300, ge=1, description="Operation timeout in seconds")
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Additional flow options"
    )
    entities: Optional[List[Dict[str, Any]]] = Field(
        default=None, description="Entity definitions for this flow"
    )

    @field_validator("home", mode="before")
    @classmethod
    def parse_home(cls, v):
        """Parse home configuration using registry pattern."""
        # Registry pattern creates the right HomeConfig
        config = HomeConfig.create(v)
        # Registry pattern creates the right Home instance
        return Home.create("flow_home", config)

    @field_validator("store", mode="before")
    @classmethod
    def parse_store(cls, v):
        """Parse store configuration using registry pattern."""
        # Registry pattern creates the right StoreConfig
        config = StoreConfig.create(v)
        # Registry pattern creates the right Store instance
        return Store.create("", config)

    @property
    def home_instance(self) -> Home:
        """Get home instance - always returns Home after validation."""
        return self.home

    @property
    def store_instance(self) -> Store:
        """Get store instance - always returns Store after validation."""
        return self.store
