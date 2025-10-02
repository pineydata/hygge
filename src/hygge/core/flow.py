"""
Flow manages the movement of data from Home to Store.

Implements a producer-consumer pattern to efficiently move data batches
from a source (Home) to a destination (Store), with proper error handling,
retries, and state management.
"""
import asyncio
from typing import Any, Dict, Optional, Union

from hygge.utility.exceptions import FlowError
from hygge.utility.logger import get_logger

from .configs import FlowDefaults
from .home import Home
from .homes import ParquetHome, SQLHome
from .homes.configs import ParquetHomeConfig, SQLHomeConfig
from .store import Store
from .stores import ParquetStore
from .stores.configs import ParquetStoreConfig


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
        home_config: Optional[
            Union[Dict[str, Any], ParquetHomeConfig, SQLHomeConfig]
        ] = None,
        store_config: Optional[
            Union[Dict[str, Any], ParquetStoreConfig]
        ] = None,
        options: Optional[Dict[str, Any]] = None
    ):
        self.name = name
        self.options = options or {}

        # Home and Store type mappings
        self.HOME_TYPES = {
            'sql': SQLHome,
            'parquet': ParquetHome
        }

        self.STORE_TYPES = {
            'parquet': ParquetStore
        }

        # Instantiate Home
        if home:
            self.home = home
        elif home_config:
            self.home = self._create_home(home_config)
        else:
            raise ValueError("Either 'home' or 'home_config' must be provided")

        # Instantiate Store
        if store:
            self.store = store
        elif store_config:
            self.store = self._create_store(store_config)
        else:
            raise ValueError("Either 'store' or 'store_config' must be provided")

        # Settings with defaults using FlowDefaults
        defaults = FlowDefaults()
        self.queue_size = self.options.get('queue_size', defaults.queue_size)
        self.timeout = self.options.get('timeout', defaults.timeout)

        # State tracking
        self.total_rows = 0
        self.batches_processed = 0
        self.start_time = None

        self.logger = get_logger(f"hygge.flow.{name}")

    def _create_home(
        self, config: Union[Dict[str, Any], ParquetHomeConfig, SQLHomeConfig]
    ) -> Home:
        """Create a home instance from configuration."""
        # Handle Pydantic config objects
        if isinstance(config, (ParquetHomeConfig, SQLHomeConfig)):
            home_class = self.HOME_TYPES.get(config.type)
            if not home_class:
                raise ValueError(f"Unknown home type: {config.type}")

            return home_class(name=self.name, config=config)

        # Handle dictionary configs (legacy support)
        home_type = config.get('type')
        if not home_type:
            raise ValueError("Home configuration missing 'type'")

        home_class = self.HOME_TYPES.get(home_type)
        if not home_class:
            raise ValueError(f"Unknown home type: {home_type}")

        # Extract name and options
        name = config.get('name', self.name)
        options = config.get('options', {})

        # Create home based on type using new config system
        if home_type == 'sql':
            if 'connection' not in config:
                raise ValueError("SQL home missing 'connection'")
            if 'query' not in config:
                raise ValueError("SQL home missing 'query'")

            sql_config = SQLHomeConfig(
                connection=config['connection'],
                query=config['query'],
                options=options
            )
            return home_class(name=name, config=sql_config)

        elif home_type == 'parquet':
            if 'path' not in config:
                raise ValueError("Parquet home missing 'path'")

            parquet_config = ParquetHomeConfig(
                path=config['path'],
                options=options
            )
            return home_class(name=name, config=parquet_config)
        else:
            raise ValueError(f"Unsupported home type: {home_type}")

    def _create_store(self, config: Union[Dict[str, Any], ParquetStoreConfig]) -> Store:
        """Create a store instance from configuration."""
        # Handle Pydantic config objects
        if isinstance(config, ParquetStoreConfig):
            store_class = self.STORE_TYPES.get(config.type)
            if not store_class:
                raise ValueError(f"Unknown store type: {config.type}")

            return store_class(name=self.name, config=config, flow_name=self.name)

        # Handle dictionary configs (legacy support)
        store_type = config.get('type')
        if not store_type:
            raise ValueError("Store configuration missing 'type'")

        store_class = self.STORE_TYPES.get(store_type)
        if not store_class:
            raise ValueError(f"Unknown store type: {store_type}")

        # Extract name and options
        name = config.get('name', self.name)
        options = config.get('options', {})

        # Create store based on type using new config system
        if store_type == 'parquet':
            if 'path' not in config:
                raise ValueError("Parquet store missing 'path'")

            parquet_config = ParquetStoreConfig(
                path=config['path'],
                options=options
            )
            return store_class(name=name, config=parquet_config, flow_name=self.name)
        else:
            raise ValueError(f"Unsupported store type: {store_type}")

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
                            self.logger.EXTRACT_TEMPLATE.format(
                                self.total_rows, duration, rate
                            )
                        )

                except Exception as e:
                    self.logger.error(f"Failed to process batch: {str(e)}")
                    # Signal producer to stop by putting None in queue
                    try:
                        await queue.put(None)
                    except Exception:
                        pass
                    raise FlowError(f"Batch processing failed: {str(e)}")

                finally:
                    queue.task_done()

        except Exception as e:
            self.logger.error(f"Consumer error: {str(e)}")
            raise FlowError(f"Consumer failed: {str(e)}")
