"""
Database execution engines for different execution strategies.

Provides flexible execution strategies for different database types:
- ThreadPoolEngine: For synchronous drivers (PYODBC) needing true parallelism
- SimpleEngine: For simple operations using asyncio.to_thread()

hygge philosophy: Simple, focused abstraction with smart defaults.
"""
import asyncio
import concurrent.futures
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Callable, Dict, Optional, TypeVar

from hygge.utility.logger import get_logger

T = TypeVar("T")

logger = get_logger("hygge.connections.execution")


class ExecutionEngine(ABC):
    """
    Base class for database execution strategies.

    Different databases need different execution strategies:
    - ThreadPoolEngine: For synchronous drivers (PYODBC) that need true parallelism
    - SimpleEngine: For single operations using asyncio.to_thread()
    """

    @abstractmethod
    async def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Execute a synchronous operation.

        Args:
            func: Synchronous function to execute
            *args, **kwargs: Arguments to pass to function

        Returns:
            Result of function execution
        """
        pass

    @abstractmethod
    async def execute_streaming(
        self, extract_func: Callable[..., Any], *args
    ) -> AsyncIterator:
        """
        Execute a streaming operation (generator).

        Yields results as they're produced, enabling true streaming
        without buffering everything in memory.

        Args:
            extract_func: Synchronous generator function (yields batches)
            *args: Arguments to pass to extract_func

        Yields:
            Results as they're produced (streaming, not buffered)
        """
        pass


class ThreadPoolEngine(ExecutionEngine):
    """
    Execution engine using a shared thread pool.

    Used for synchronous database drivers (PYODBC) that need true parallelism.
    Multiple extractions can run simultaneously in separate threads.

    Supports streaming: batches are yielded as they're extracted.
    """

    _executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
    _pool_size: int = 8

    @classmethod
    def initialize(cls, pool_size: int = 8) -> None:
        """
        Initialize shared thread pool.

        Args:
            pool_size: Number of worker threads in the pool
        """
        if cls._executor is None:
            cls._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=pool_size, thread_name_prefix="hygge-db"
            )
            cls._pool_size = pool_size
            logger.debug(f"Initialized ThreadPoolEngine with {pool_size} workers")
        else:
            logger.warning("ThreadPoolEngine already initialized")

    @classmethod
    async def execute(cls, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Execute single synchronous operation in thread pool.

        Args:
            func: Synchronous function to execute
            *args, **kwargs: Arguments to pass to function

        Returns:
            Result of function execution
        """
        if cls._executor is None:
            cls.initialize()

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(cls._executor, lambda: func(*args, **kwargs))

    @classmethod
    async def execute_streaming(
        cls, extract_func: Callable[..., Any], *args
    ) -> AsyncIterator:
        """
        Execute streaming extraction with true streaming.

        Runs extraction in a thread, yields batches as they're produced.
        Uses an asyncio.Queue to bridge thread and async generator.
        This enables true streaming: batches are consumed as they're extracted,
        not after extraction completes.

        Args:
            extract_func: Synchronous generator function (yields batches)
            *args: Arguments to pass to extract_func

        Yields:
            Batches as they're extracted (streaming, not buffered)

        Example:
            ```python
            def extract_batches(query):
                for batch in pl.read_database(query, ...):
                    yield batch

            async for batch in engine.execute_streaming(extract_batches, query):
                # Batch is yielded as soon as it's extracted
                yield batch
            ```
        """
        if cls._executor is None:
            cls.initialize()

        queue = asyncio.Queue(maxsize=5)  # Small buffer for smooth flow
        loop = asyncio.get_event_loop()
        exception_holder = {"exception": None}

        def extract_to_queue():
            """Extract batches in thread, put in queue."""
            try:
                for batch in extract_func(*args):
                    # Put batch in queue from thread
                    # Use run_coroutine_threadsafe to schedule async put
                    # .result() blocks thread until put completes (handles backpressure)
                    future = asyncio.run_coroutine_threadsafe(queue.put(batch), loop)
                    future.result()  # Wait for put, handles queue full case

                # Signal end of extraction
                asyncio.run_coroutine_threadsafe(queue.put(None), loop).result()
            except Exception as e:
                exception_holder["exception"] = e
                # Signal end even on error
                try:
                    asyncio.run_coroutine_threadsafe(queue.put(None), loop).result()
                except Exception:
                    pass  # Queue might be full, that's ok

        # Start extraction in thread pool
        extraction_task = cls._executor.submit(extract_to_queue)

        # Yield batches from queue as they arrive (true streaming)
        try:
            while True:
                batch = await queue.get()

                if batch is None:  # End signal
                    # Check for exceptions from extraction thread
                    if exception_holder["exception"]:
                        raise exception_holder["exception"]
                    break

                yield batch
        finally:
            # Ensure extraction thread completes (might already be done)
            try:
                extraction_task.result(timeout=1.0)  # Re-raise any exceptions
            except concurrent.futures.TimeoutError:
                # Thread still running, that's ok - it will complete when queue drains
                pass

    @classmethod
    def shutdown(cls) -> None:
        """Shutdown thread pool."""
        if cls._executor:
            logger.debug("Shutting down ThreadPoolEngine")
            cls._executor.shutdown(wait=True)
            cls._executor = None
            logger.debug("ThreadPoolEngine shutdown complete")

    @classmethod
    def is_initialized(cls) -> bool:
        """Check if thread pool is initialized."""
        return cls._executor is not None


class SimpleEngine(ExecutionEngine):
    """
    Simple execution engine using asyncio.to_thread().

    For operations that don't need thread pooling - just wrap in asyncio.to_thread().
    Used for simple, one-off database operations.
    """

    @staticmethod
    async def execute(func: Callable[..., T], *args, **kwargs) -> T:
        """
        Execute using asyncio.to_thread().

        Args:
            func: Synchronous function to execute
            *args, **kwargs: Arguments to pass to function

        Returns:
            Result of function execution
        """
        return await asyncio.to_thread(func, *args, **kwargs)

    @staticmethod
    async def execute_streaming(
        extract_func: Callable[..., Any], *args
    ) -> AsyncIterator:
        """
        Execute streaming extraction using asyncio.to_thread().

        Note: This buffers results (limitation of asyncio.to_thread with generators).
        For true streaming with parallelism, use ThreadPoolEngine.

        Args:
            extract_func: Synchronous generator function
            *args: Arguments to pass to extract_func

        Yields:
            Results (buffered, not true streaming)
        """

        # asyncio.to_thread doesn't handle generators well, so we need to
        # collect and yield - but this is fine for simple cases
        def collect_results():
            return list(extract_func(*args))

        results = await asyncio.to_thread(collect_results)
        for result in results:
            yield result


# Registry for execution engines
_execution_engines: Dict[str, ExecutionEngine] = {}


def register_engine(name: str, engine: ExecutionEngine) -> None:
    """
    Register an execution engine.

    Args:
        name: Name to register the engine under
        engine: Execution engine instance
    """
    _execution_engines[name] = engine


def get_engine(name: str) -> ExecutionEngine:
    """
    Get execution engine by name.

    Args:
        name: Name of the engine

    Returns:
        Execution engine instance

    Raises:
        ValueError: If engine name is not registered
    """
    if name not in _execution_engines:
        raise ValueError(f"Unknown execution engine: {name}")
    return _execution_engines[name]


# Register default engines
register_engine("thread_pool", ThreadPoolEngine())
register_engine("simple", SimpleEngine())
