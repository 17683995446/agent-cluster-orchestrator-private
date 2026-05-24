"""
🔧 兼容性层 - 保持向后兼容性
确保新旧模块结构都能正常工作
"""
from __future__ import annotations
import sys
from typing import Any

# 兼容性警告
COMPATIBILITY_MODE = True

# ------------------------------------------------------------------------------
# 异常模块兼容性
# ------------------------------------------------------------------------------
try:
    # 新模块结构
    from app.core.exceptions import (
        AgentClusterException,
        AgentNotFoundError,
        TaskNotFoundError,
        ValidationError,
        LLMProviderError,
        RateLimitExceededError,
        TaskTimeoutError,
        ResourceNotFoundError,
        ResourceExhaustedError,
        OperationNotAllowedError,
        agent_not_found_error_response,
        task_not_found_error_response,
        validation_error_response,
        exception_to_http_response,
    )
except ImportError:
    # 旧模块结构回退
    try:
        from app.core.custom_exceptions_handler import (
            AgentClusterException,
            AgentNotFoundError,
            TaskNotFoundError,
            ValidationError,
            LLMProviderError,
            RateLimitExceededError,
            TaskTimeoutError,
            agent_not_found_error,
            task_not_found_error,
            validation_error_response,
        )
        # 别名映射
        agent_not_found_error_response = agent_not_found_error
        task_not_found_error_response = task_not_found_error
        
    except ImportError:
        # 最后的回退
        class AgentClusterException(Exception):
            def __init__(self, message: str, code: str = "INTERNAL_ERROR"):
                self.message = message
                self.code = code
                super().__init__(message)
        
        AgentNotFoundError = AgentClusterException
        TaskNotFoundError = AgentClusterException
        ValidationError = AgentClusterException


# ------------------------------------------------------------------------------
# 日志模块兼容性
# ------------------------------------------------------------------------------
def get_logger(name: str | None = None):
    """获取兼容的日志记录器"""
    try:
        from app.core.logging import get_application_logger
        return get_application_logger()
    except ImportError:
        try:
            from app.core.structured_logging_config import get_structured_logger
            return get_structured_logger(name) if name else structlog.get_logger()
        except ImportError:
            import logging
            return logging.getLogger(name or "app")


# 尝试导入structlog
try:
    import structlog
except ImportError:
    structlog = None


# ------------------------------------------------------------------------------
# 保护机制兼容性
# ------------------------------------------------------------------------------
try:
    from app.core.protection import (
        # 熔断器
        CircuitBreaker,
        CircuitBreakerConfig,
        CircuitState,
        CircuitBreakerOpenError,
        CircuitBreakerError,
        CircuitBreakerManager,
        # 连接池
        ConnectionPool,
        ConnectionWrapper,
        PoolConfig,
        PoolExhaustedError,
        # 超时保护
        TimeoutMonitor,
        TimeoutConfig,
        TimeoutError,
        execute_with_timeout,
        # 自适应队列
        AdaptiveQueue,
        QueueConfig,
        QueueFullError,
        PriorityItem,
    )
except ImportError:
    # 回退到旧的保护机制模块
    try:
        from app.core.protection_mechanisms import (
            CircuitBreaker,
            CircuitBreakerConfig,
            CircuitState,
            CircuitBreakerOpenError,
            CircuitBreakerError,
            CircuitBreakerManager,
            ConnectionPool,
            ConnectionWrapper,
            PoolConfig,
            PoolExhaustedError,
            TimeoutMonitor,
            TimeoutConfig,
            TimeoutError,
            execute_with_timeout,
            AdaptiveQueue,
            QueueConfig,
            QueueFullError,
            PriorityItem,
        )
    except ImportError:
        # 如果都不可用，提供基本的占位符
        pass


# ------------------------------------------------------------------------------
# ==========================================
# 事件系统兼容性
# ==========================================
try:
    from app.core.events import (
        Event,
        EventType,
        EventBus,
        EventEmitter,
        initialize_event_system,
        start_event_system,
        stop_event_system,
        get_event_bus,
        get_event_emitter,
    )
except ImportError:
    # 回退到旧的event_bus模块或提供简单的兼容接口
    try:
        from app.core.event_bus import (
            Event,
            EventType,
            EventBus,
            EventEmitter,
            initialize_event_system,
            start_event_system,
            stop_event_system,
            get_event_bus,
            get_event_emitter,
        )
    except ImportError:
        # 最小兼容性接口
        print("Warning: Event system not found, using minimal interface")
        
        class MinimalEvent:
            def __init__(self, event_type, source, **kwargs):
                self.event_type = event_type
                self.source = source
                self.data = kwargs
        
        Event = MinimalEvent
        EventType = str
        EventBus = None
        EventEmitter = None
        
        # 无操作函数
        def initialize_event_system(): pass
        def start_event_system(): pass
        def stop_event_system(): pass
        def get_event_bus(): return None
        def get_event_emitter(source): return None


# ------------------------------------------------------------------------------
# 模块重新导出
# ------------------------------------------------------------------------------
__all__ = [
    # 异常
    "AgentClusterException",
    "AgentNotFoundError",
    "TaskNotFoundError",
    "ValidationError",
    "LLMProviderError",
    "RateLimitExceededError",
    "TaskTimeoutError",
    "ResourceNotFoundError",
    "ResourceExhaustedError",
    "OperationNotAllowedError",
    # 响应转换器
    "agent_not_found_error_response",
    "task_not_found_error_response",
    "validation_error_response",
    "exception_to_http_response",
    # 日志
    "get_logger",
    # 熔断器
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "CircuitBreakerOpenError",
    "CircuitBreakerError",
    "CircuitBreakerManager",
    # 连接池
    "ConnectionPool",
    "ConnectionWrapper",
    "PoolConfig",
    "PoolExhaustedError",
    # 超时保护
    "TimeoutMonitor",
    "TimeoutConfig",
    "TimeoutError",
    "execute_with_timeout",
    # 自适应队列
    "AdaptiveQueue",
    "QueueConfig",
    "QueueFullError",
    "PriorityItem",
    # 事件系统
    "Event",
    "EventType",
    "EventPriority",
    "EventBus",
    "EventEmitter",
    "StreamProcessor",
    "EventPersister",
    "initialize_event_system",
    "start_event_system",
    "stop_event_system",
    "get_event_bus",
    "get_event_emitter",
]


def log_compatibility_info():
    """记录兼容性信息"""
    import warnings
    if COMPATIBILITY_MODE:
        warnings.warn(
            "Running in compatibility mode. Consider updating imports to new module structure.",
            DeprecationWarning,
            stacklevel=2
        )


# 初始化时记录兼容性信息
if COMPATIBILITY_MODE:
    try:
        log_compatibility_info()
    except Exception:
        pass
