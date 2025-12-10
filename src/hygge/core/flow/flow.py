"""
Core Flow class for comfortable, reliable data movement.

Flow orchestrates the movement of data from a Home to a Store, making
data feel at home wherever it lives. It uses a producer-consumer pattern
to move data smoothly in batches, with automatic retries for transient
errors and clear progress tracking.

Following hygge's philosophy, Flow prioritizes:
- **Comfort**: Data moves naturally without friction
- **Reliability**: Automatic retries, graceful error handling, state management
- **Flow over force**: Smooth batch processing that adapts to your data
"""
import asyncio
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Optional

import polars as pl

from hygge.messages import get_logger
from hygge.utility.exceptions import (
    FlowConnectionError,
    FlowError,
    FlowExecutionError,
    HomeConnectionError,
    HomeError,
    JournalWriteError,
    StoreConnectionError,
    StoreError,
)
from hygge.utility.retry import with_retry

from ..home import Home
from ..journal import Journal


class Flow:
    """
    Orchestrates comfortable, reliable data movement from Home to Store.

    Flow makes data movement feel natural and smooth. It reads data from
    your Home in batches, writes it to your Store efficiently, and handles
    all the complexity of retries, error recovery, and progress tracking.

    Following hygge's philosophy:
    - **Comfort**: Data moves smoothly without you worrying about the details
    - **Reliability**: Automatic retries for transient errors, graceful failure handling
    - **Natural flow**: Producer-consumer pattern that adapts to your data volume

    Flow manages the producer-consumer pattern internally, coordinating batch
    processing, handling retries automatically, and tracking progress so you
    can see how things are going. It's designed to work reliably in production
    while feeling simple and comfortable to use.

    Args:
        name: Name of this flow (for logging and identification)
        home: Source to read data from (any Home implementation)
        store: Destination to write data to (any Store implementation)
        options: Optional configuration options:
            - queue_size: Size of batch queue (default: 10)
            - timeout: Operation timeout in seconds (default: 300)
    """

    def __init__(
        self,
        name: str,
        home: Home,
        store: Any,  # Store type
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
        self.base_flow_name = base_flow_name

        # Entity name is always set by FlowFactory (either from Entity or flow_name)
        # Fail fast if not provided - FlowFactory is the canonical way to create flows
        if entity_name is None:
            raise FlowError(
                f"Flow '{name}' created without entity_name. "
                "Flows must be created via FlowFactory, which always sets entity_name. "
                "Use FlowFactory.from_config() or FlowFactory.from_entity() instead."
            )
        self.entity_name = entity_name

        self.run_type = run_type or "full_drop"
        self.watermark_config = watermark_config

        # Allow stores to adjust their strategy (truncate vs append) per run
        # Default implementation is no-op, so always safe to call
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
        """
        Start the flow from Home to Store with automatic retry for transient errors.

        Retries the entire flow (producer + consumer) on transient connection errors,
        cleaning up staging/tmp directories before each retry to ensure a clean state.
        """
        # Apply retry decorator with instance methods
        retry_decorated = with_retry(
            retries=3,
            delay=2,
            exceptions=(FlowError,),
            timeout=3600,  # 1 hour for entire flow
            logger_name="hygge.flow",
            retry_if_func=self._should_retry_flow_error,
            before_sleep_func=self._cleanup_before_retry,
        )(self._execute_flow)

        await retry_decorated()

    async def _execute_flow(self) -> None:
        """Execute a single attempt of the flow (producer + consumer)."""
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

        except FlowConnectionError:
            # Transient connection error - will be retried, preserve exception
            raise
        except FlowError:
            # Other flow errors - don't retry, preserve exception
            raise
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

            # CRITICAL: Use 'from e' to preserve exception context
            raise FlowExecutionError(
                f"Flow failed: {self.name}, error: {str(e)}"
            ) from e

    def _should_retry_flow_error(self, exception: Exception) -> bool:
        """
        Determine if a FlowError should be retried based on exception type.

        Args:
            exception: The exception that was raised

        Returns:
            True if the error is transient and should be retried
        """
        # Retry on transient connection errors
        if isinstance(
            exception, (FlowConnectionError, HomeConnectionError, StoreConnectionError)
        ):
            return True

        # Don't retry on other errors
        return False

    async def _cleanup_before_retry(self, retry_state) -> None:
        """
        Clean up staging/tmp and reset flow state before retrying.

        Args:
            retry_state: Tenacity retry state object
        """
        attempt_number = retry_state.attempt_number
        self.logger.warning(
            f"Preparing for retry {attempt_number} of flow {self.name}: "
            "cleaning up staging and resetting state"
        )

        # Clean up staging/tmp before retrying to ensure clean state
        try:
            await self.store.cleanup_staging()
        except Exception as cleanup_error:
            self.logger.warning(
                f"Failed to cleanup staging before retry: {str(cleanup_error)}"
            )

        # Reset retry-sensitive state in store (e.g., sequence counters)
        try:
            await self.store.reset_retry_sensitive_state()
        except Exception as reset_error:
            self.logger.warning(
                f"Failed to reset retry-sensitive state before retry: "
                f"{str(reset_error)}"
            )

        # Reset flow state for retry
        self.total_rows = 0
        self.rows_written = 0
        self.batches_processed = 0

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

        except HomeConnectionError:
            # Connection errors from home - preserve and re-raise
            raise
        except HomeError:
            # Other home errors - preserve and re-raise
            raise
        except Exception as e:
            self.logger.error(f"Producer error: {str(e)}")
            producer_done.set()  # Signal done even on error
            # CRITICAL: Use 'from e' to preserve exception context
            raise FlowError(f"Producer failed: {str(e)}") from e

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

        entity_identifier = self.entity_name

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

                except StoreConnectionError:
                    # Connection errors from store - preserve and re-raise
                    raise
                except StoreError:
                    # Other store errors - preserve and re-raise
                    raise
                except Exception as e:
                    self.logger.error(f"Failed to process batch: {str(e)}")
                    # Signal producer to stop by putting None in queue (non-blocking)
                    try:
                        queue.put_nowait(None)
                    except Exception:
                        pass
                    # CRITICAL: Use 'from e' to preserve exception context
                    raise FlowError(f"Batch processing failed: {str(e)}") from e

                finally:
                    queue.task_done()

        except StoreConnectionError:
            # Connection errors from store - preserve and re-raise
            raise
        except StoreError:
            # Other store errors - preserve and re-raise
            raise
        except Exception as e:
            self.logger.error(f"Consumer error: {str(e)}")
            # CRITICAL: Use 'from e' to preserve exception context
            raise FlowError(f"Consumer failed: {str(e)}") from e

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
            # Entity name is always set (from Entity or flow_name for non-entity flows)
            # Runtime validation ensures it's never None
            if self.entity_name is None:
                raise FlowError(
                    f"Flow '{self.name}' has None entity_name. "
                    "This should not happen when using FlowFactory. "
                    "entity_name must be set when creating Flow."
                )
            entity = self.entity_name

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
                entity=entity,
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

        except JournalWriteError as e:
            # Log error clearly but don't break flow
            self.logger.error(
                f"Failed to record entity run in journal: {str(e)}. "
                "This is non-blocking, but journal tracking may be incomplete."
            )
        except Exception as e:
            # Unexpected errors - log and wrap
            self.logger.error(
                f"Unexpected error recording entity run in journal: {str(e)}"
            )
            # Don't raise - journal failures should not break flows
            # But log as error so it's visible
