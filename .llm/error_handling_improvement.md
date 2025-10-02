# Error Handling Improvement Plan

## Current State Analysis

### What's Working
- ✅ Custom exception hierarchy: `HomeError`, `StoreError`, `FlowError`
- ✅ Retry decorator with exponential backoff implemented
- ✅ Basic exception propagation

### What's Broken
- ❌ Inconsistent error handling across components
- ❌ Some places use generic `Exception` instead of custom exceptions
- ❌ No standardized error messages
- ❌ No error context or debugging information
- ❌ No graceful degradation strategies

## hygge Error Handling Philosophy

Following hygge's "Reliability Over Speed" principle:

1. **Graceful Failure** - Handle errors without crashing
2. **Clear Communication** - Error messages should be helpful and actionable
3. **Context Preservation** - Maintain error context for debugging
4. **Recovery Strategies** - Provide ways to recover from failures
5. **User Comfort** - Errors should not surprise users

## Current Error Handling Issues

### 1. Inconsistent Exception Usage

**Problem**: Some code uses generic `Exception`, others use custom exceptions.

**Current (Inconsistent):**
```python
# In coordinator.py
except Exception as e:
    raise ConfigError(f"Failed to setup coordinator: {str(e)}")

# In flow.py
except Exception as e:
    raise FlowError(f"Flow {self.name} failed: {e}")
```

**Solution**: Standardize on custom exceptions throughout.

### 2. Missing Error Context

**Problem**: Errors don't provide enough context for debugging.

**Current (No Context):**
```python
except Exception as e:
    raise HomeError(f"Failed to read data: {e}")
```

**Solution**: Include operation context, file paths, and relevant parameters.

### 3. No Graceful Degradation

**Problem**: Failures cause complete flow termination.

**Current (All-or-Nothing):**
```python
# If one batch fails, entire flow fails
async for batch in self.home.read_batches():
    await self.store.write(batch)  # If this fails, flow stops
```

**Solution**: Implement partial failure recovery and retry strategies.

## Recommended Error Handling Architecture

### 1. Enhanced Exception Hierarchy

```python
# src/hygge/utility/exceptions.py

class HyggeError(Exception):
    """Base exception for all hygge errors."""

    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.context = context or {}
        self.timestamp = datetime.now()

    def __str__(self) -> str:
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{super().__str__()} (Context: {context_str})"
        return super().__str__()

class ConfigError(HyggeError):
    """Configuration-related errors."""
    pass

class HomeError(HyggeError):
    """Home (data source) related errors."""
    pass

class StoreError(HyggeError):
    """Store (data destination) related errors."""
    pass

class FlowError(HyggeError):
    """Flow execution related errors."""
    pass

class CoordinatorError(HyggeError):
    """Coordinator orchestration related errors."""
    pass
```

### 2. Error Context Builder

```python
# src/hygge/utility/error_context.py

class ErrorContext:
    """Builds error context for better debugging."""

    def __init__(self):
        self.context = {}

    def add_operation(self, operation: str) -> 'ErrorContext':
        self.context['operation'] = operation
        return self

    def add_file_path(self, path: str) -> 'ErrorContext':
        self.context['file_path'] = path
        return self

    def add_batch_info(self, batch_size: int, batch_number: int) -> 'ErrorContext':
        self.context['batch_size'] = batch_size
        self.context['batch_number'] = batch_number
        return self

    def add_flow_info(self, flow_name: str) -> 'ErrorContext':
        self.context['flow_name'] = flow_name
        return self

    def add_home_info(self, home_type: str, home_name: str) -> 'ErrorContext':
        self.context['home_type'] = home_type
        self.context['home_name'] = home_name
        return self

    def add_store_info(self, store_type: str, store_name: str) -> 'ErrorContext':
        self.context['store_type'] = store_type
        self.context['store_name'] = store_name
        return self

    def build(self) -> Dict[str, Any]:
        return self.context.copy()
```

### 3. Standardized Error Handling Decorator

```python
# src/hygge/utility/error_handler.py

def handle_errors(
    error_type: Type[HyggeError],
    operation: str,
    context_builder: Optional[Callable] = None
):
    """Decorator for standardized error handling."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except HyggeError:
                # Re-raise hygge errors as-is
                raise
            except Exception as e:
                # Build context for non-hygge errors
                context = {}
                if context_builder:
                    context = context_builder(*args, **kwargs)

                # Add operation context
                context['operation'] = operation
                context['original_error'] = str(e)
                context['error_type'] = type(e).__name__

                raise error_type(f"Failed to {operation}: {e}", context)

        return wrapper
    return decorator
```

### 4. Graceful Failure Strategies

```python
# src/hygge/utility/failure_strategies.py

class FailureStrategy:
    """Base class for failure handling strategies."""

    async def handle_failure(self, error: Exception, context: Dict[str, Any]) -> bool:
        """Handle a failure and return whether to continue."""
        raise NotImplementedError

class RetryStrategy(FailureStrategy):
    """Retry failed operations with exponential backoff."""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay

    async def handle_failure(self, error: Exception, context: Dict[str, Any]) -> bool:
        retry_count = context.get('retry_count', 0)

        if retry_count < self.max_retries:
            delay = self.base_delay * (2 ** retry_count)
            await asyncio.sleep(delay)
            context['retry_count'] = retry_count + 1
            return True  # Continue with retry

        return False  # Give up

class SkipStrategy(FailureStrategy):
    """Skip failed operations and continue."""

    async def handle_failure(self, error: Exception, context: Dict[str, Any]) -> bool:
        # Log the failure but continue
        logger.warning(f"Skipping failed operation: {error}")
        return True  # Continue

class StopStrategy(FailureStrategy):
    """Stop on first failure."""

    async def handle_failure(self, error: Exception, context: Dict[str, Any]) -> bool:
        return False  # Stop
```

### 5. Enhanced Flow Error Handling

```python
# src/hygge/core/flow.py

class Flow:
    def __init__(self, name: str, failure_strategy: FailureStrategy = None):
        self.name = name
        self.failure_strategy = failure_strategy or RetryStrategy()
        self.failed_batches = []
        self.successful_batches = 0

    @handle_errors(FlowError, "execute flow", lambda self, *args, **kwargs: {
        'flow_name': self.name
    })
    async def start(self):
        """Start the flow with enhanced error handling."""
        try:
            async for batch in self.home.read_batches():
                try:
                    await self.store.write(batch)
                    self.successful_batches += 1
                except Exception as e:
                    context = ErrorContext()\
                        .add_flow_info(self.name)\
                        .add_batch_info(len(batch), self.successful_batches + 1)\
                        .build()

                    should_continue = await self.failure_strategy.handle_failure(e, context)

                    if not should_continue:
                        raise FlowError(f"Flow {self.name} failed after retries", context)

                    # Log failed batch but continue
                    self.failed_batches.append({
                        'batch_number': self.successful_batches + 1,
                        'error': str(e),
                        'timestamp': datetime.now()
                    })

        except Exception as e:
            context = ErrorContext()\
                .add_flow_info(self.name)\
                .add_operation("flow_execution")\
                .build()

            raise FlowError(f"Flow {self.name} failed: {e}", context)
```

### 6. Enhanced Coordinator Error Handling

```python
# src/hygge/core/coordinator.py

class Coordinator:
    @handle_errors(CoordinatorError, "setup coordinator", lambda self, *args, **kwargs: {
        'config_path': str(self.config_path)
    })
    async def setup(self):
        """Setup coordinator with enhanced error handling."""
        try:
            with open(self.config_path) as f:
                config = yaml.safe_load(f)

            errors = validate_config(config)
            if errors:
                context = ErrorContext()\
                    .add_operation("config_validation")\
                    .add_file_path(str(self.config_path))\
                    .build()

                raise ConfigError(
                    f"Configuration validation failed: {', '.join(errors)}",
                    context
                )

            await self._setup_flows(config.get('flows', {}))

        except FileNotFoundError:
            context = ErrorContext()\
                .add_operation("config_loading")\
                .add_file_path(str(self.config_path))\
                .build()

            raise ConfigError(f"Configuration file not found: {self.config_path}", context)

        except yaml.YAMLError as e:
            context = ErrorContext()\
                .add_operation("yaml_parsing")\
                .add_file_path(str(self.config_path))\
                .build()

            raise ConfigError(f"Invalid YAML syntax: {e}", context)
```

## Implementation Plan

### Phase 1: Foundation (Week 1)
1. **Enhanced exception hierarchy** - Add context support
2. **Error context builder** - Standardized context creation
3. **Error handling decorator** - Consistent error wrapping

### Phase 2: Component Integration (Week 2)
1. **Flow error handling** - Enhanced flow error management
2. **Coordinator error handling** - Better orchestration errors
3. **Home/Store error handling** - Consistent source/destination errors

### Phase 3: Failure Strategies (Week 3)
1. **Retry strategies** - Exponential backoff implementation
2. **Graceful degradation** - Partial failure recovery
3. **Error reporting** - Better error messages and context

### Phase 4: Testing and Validation (Week 4)
1. **Error scenario tests** - Test all failure modes
2. **Recovery tests** - Test retry and recovery strategies
3. **Performance impact** - Ensure error handling doesn't slow down flows

## Success Metrics

### Error Handling Quality
- **Error context**: 100% of errors include relevant context
- **Error messages**: 100% of errors are actionable and clear
- **Recovery rate**: 90%+ of transient failures recover automatically
- **Graceful degradation**: 100% of partial failures don't crash entire flows

### User Experience
- **Error clarity**: Users can understand and fix errors
- **Recovery guidance**: Clear instructions for error recovery
- **Debugging support**: Sufficient context for troubleshooting
- **Operational comfort**: Errors don't surprise users

## Configuration Options

### Error Handling Configuration
```yaml
# In flow configuration
flows:
  users_to_parquet:
    home: data/users.parquet
    store: data/lake/users
    error_handling:
      strategy: retry  # retry, skip, stop
      max_retries: 3
      retry_delay: 1.0
      continue_on_error: true
      log_failures: true
```

### Coordinator Error Handling
```yaml
# In coordinator configuration
coordinator:
  error_handling:
    continue_on_flow_error: true
    max_concurrent_failures: 5
    failure_notification: true
```

This error handling improvement plan ensures hygge follows its "Reliability Over Speed" principle while maintaining the comfort and predictability that makes it hygge.
