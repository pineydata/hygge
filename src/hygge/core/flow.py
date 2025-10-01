"""
Flow manages the movement of data from Home to Store.

Implements a producer-consumer pattern to efficiently move data batches
from a source (Home) to a destination (Store), with proper error handling,
retries, and state management.
"""
import asyncio
from typing import Any, Dict, Optional

from hygge.utility.exceptions import FlowError
from hygge.utility.logger import get_logger
from .home import Home
from .store import Store


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
        home (Home, optional): Source to read data from (if not using home_class)
        store (Store, optional): Destination to write data to (if not using store_class)
        home_class (type, optional): Class to instantiate for Home
        home_config (Dict[str, Any], optional): Configuration for Home instantiation
        store_class (type, optional): Class to instantiate for Store
        store_config (Dict[str, Any], optional): Configuration for Store instantiation
        options (Dict[str, Any], optional): Configuration options
            - queue_size (int): Size of batch queue (default: 10)
            - timeout (int): Operation timeout in seconds (default: 300)
    """

    def __init__(
        self,
        name: str,
        home: Optional[Home] = None,
        store: Optional[Store] = None,
        home_class: Optional[type] = None,
        home_config: Optional[Dict[str, Any]] = None,
        store_class: Optional[type] = None,
        store_config: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None
    ):
        self.name = name
        self.options = options or {}

        # Instantiate Home and Store if classes and configs are provided
        if home_class and home_config:
            self.home = home_class(**home_config)
        elif home:
            self.home = home
        else:
            raise ValueError("Either 'home' or both 'home_class' and 'home_config' must be provided")

        if store_class and store_config:
            self.store = store_class(**store_config)
        elif store:
            self.store = store
        else:
            raise ValueError("Either 'store' or both 'store_class' and 'store_config' must be provided")

        # Settings
        self.queue_size = self.options.get('queue_size', 10)
        self.timeout = self.options.get('timeout', 300)

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
                self._producer(queue, producer_done),
                name=f"{self.name}_producer"
            )
            consumer = asyncio.create_task(
                self._consumer(queue, producer_done),
                name=f"{self.name}_consumer"
            )

            # Wait for producer to finish
            await producer

            # Wait for consumer to process all data
            await queue.join()

            # Get consumer result
            await consumer

            # Ensure all data is written
            await self.store.finish()

            duration = asyncio.get_event_loop().time() - self.start_time
            self.logger.info(
                f"Flow completed: {self.name}, "
                f"processed {self.total_rows:,} rows in {duration:.1f}s"
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

    async def _producer(self, queue: asyncio.Queue, producer_done: asyncio.Event) -> None:
        """Read batches from Home and put them in queue."""
        try:
            self.logger.debug(f"Starting producer for {self.name}")
            async for batch in self.home.read():  # Use Home's read() method
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

    async def _consumer(self, queue: asyncio.Queue, producer_done: asyncio.Event) -> None:
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
                    # Write to store, indicating if this might be the last batch
                    # Check if producer is done and queue is empty
                    is_last_batch = queue.empty() and producer_done.is_set()
                    self.logger.debug(f"Processing batch: {len(batch)} rows, is_last_batch={is_last_batch}, queue_empty={queue.empty()}, producer_done={producer_done.is_set()}")
                    staged_path = await self.store.write(
                        batch,
                        is_last_batch=is_last_batch
                    )

                    if staged_path:
                        self.logger.debug(f"Batch staged at: {staged_path}")

                    # Update metrics
                    self.total_rows += len(batch)
                    self.batches_processed += 1

                    # Log progress periodically
                    if self.batches_processed % 10 == 0:
                        duration = asyncio.get_event_loop().time() - self.start_time
                        rate = self.total_rows / duration if duration > 0 else 0
                        self.logger.info(
                            f"Progress: {self.total_rows:,} rows, "
                            f"{self.batches_processed} batches, "
                            f"{rate:.0f} rows/s"
                        )

                except Exception as e:
                    self.logger.error(f"Failed to process batch: {str(e)}")
                    # Signal producer to stop by putting None in queue
                    try:
                        await queue.put(None)
                    except:
                        pass
                    raise FlowError(f"Batch processing failed: {str(e)}")

                finally:
                    queue.task_done()

        except Exception as e:
            self.logger.error(f"Consumer error: {str(e)}")
            raise FlowError(f"Consumer failed: {str(e)}")
