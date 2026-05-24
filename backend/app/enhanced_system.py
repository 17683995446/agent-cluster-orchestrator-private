"""
🚀 系统增强和优化模块
提供额外的功能、性能改进和更好的错误处理
"""
from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from enum import Enum, auto
import structlog

logger = structlog.get_logger(__name__)


class CircuitState(Enum):
    """熔断器状态"""
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


@dataclass
class EnhancedCircuitBreakerConfig:
    """增强的熔断器配置"""
    failure_threshold: int = 5
    success_threshold: int = 3
    timeout_duration: float = 60.0
    half_open_max_calls: int = 3
    failure_rate_threshold: float = 0.5
    min_call_count: int = 10
    slow_call_threshold: float = 2.0  # 慢调用阈值（秒）
    slow_call_rate_threshold: float = 0.8  # 慢调用率阈值


class EnhancedCircuitBreaker:
    """增强的熔断器 - 支持慢调用检测和更多统计"""
    
    def __init__(
        self,
        name: str,
        config: Optional[EnhancedCircuitBreakerConfig] = None,
        on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None
    ):
        self.name = name
        self.config = config or EnhancedCircuitBreakerConfig()
        self.on_state_change = on_state_change
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.slow_count = 0
        self.total_calls = 0
        self.last_failure_time: Optional[float] = None
        self.half_open_calls = 0
        
        logger.info(
            "enhanced_circuit_breaker_initialized",
            name=name,
            config=self.config
        )
    
    async def execute(
        self,
        func: Callable[..., Any],
        *args,
        **kwargs
    ) -> Any:
        """执行函数，带熔断器保护"""
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._transition_to(CircuitState.HALF_OPEN)
            else:
                raise Exception(f"Circuit breaker '{self.name}' is OPEN")
        
        start_time = time.time()
        is_slow = False
        
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            # 检查是否是慢调用
            duration = time.time() - start_time
            is_slow = duration > self.config.slow_call_threshold
            
            if is_slow:
                self.slow_count += 1
                logger.warning(
                    "slow_call_detected",
                    circuit_breaker=self.name,
                    duration=duration
                )
            
            self._on_success()
            return result
            
        except Exception as e:
            self._on_failure()
            raise
    
    def _on_success(self):
        """成功调用处理"""
        self.total_calls += 1
        
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self._transition_to(CircuitState.CLOSED)
        elif self.state == CircuitState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)
            self.success_count += 1
    
    def _on_failure(self):
        """失败调用处理"""
        self.total_calls += 1
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitState.CLOSED:
            # 检查失败率或慢调用率
            if self.total_calls >= self.config.min_call_count:
                failure_rate = self.failure_count / self.total_calls
                slow_rate = self.slow_count / self.total_calls if self.total_calls > 0 else 0
                
                if (failure_rate >= self.config.failure_rate_threshold or
                    slow_rate >= self.config.slow_call_rate_threshold or
                    self.failure_count >= self.config.failure_threshold):
                    self._transition_to(CircuitState.OPEN)
    
    def _should_attempt_reset(self) -> bool:
        """检查是否应该尝试重置熔断器"""
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.config.timeout_duration
    
    def _transition_to(self, new_state: CircuitState):
        """转换熔断器状态"""
        old_state = self.state
        self.state = new_state
        
        if new_state == CircuitState.CLOSED:
            self.failure_count = 0
            self.success_count = 0
            self.half_open_calls = 0
        elif new_state == CircuitState.HALF_OPEN:
            self.half_open_calls = 0
        elif new_state == CircuitState.OPEN:
            self.last_failure_time = time.time()
        
        logger.info(
            "circuit_breaker_state_changed",
            circuit_breaker=self.name,
            old_state=old_state.name,
            new_state=new_state.name
        )
        
        if self.on_state_change:
            try:
                self.on_state_change(old_state, new_state)
            except Exception as e:
                logger.error("state_change_callback_failed", error=str(e))
    
    def get_stats(self) -> Dict[str, Any]:
        """获取熔断器统计信息"""
        return {
            "name": self.name,
            "state": self.state.name,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "slow_count": self.slow_count,
            "total_calls": self.total_calls,
            "failure_rate": self.failure_count / self.total_calls if self.total_calls > 0 else 0,
            "slow_rate": self.slow_count / self.total_calls if self.total_calls > 0 else 0
        }


@dataclass
class RetryPolicy:
    """重试策略配置"""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    exponential_base: float = 2.0
    retryable_exceptions: Tuple[Exception, ...] = (Exception,)


class EnhancedRetryManager:
    """增强的重试管理器"""
    
    def __init__(self, policy: Optional[RetryPolicy] = None):
        self.policy = policy or RetryPolicy()
        self.stats: Dict[str, int] = {"total_retires": 0, "successful_retries": 0, "failed_retries": 0}
    
    async def execute_with_retry(
        self,
        func: Callable,
        *args,
        policy: Optional[RetryPolicy] = None,
        **kwargs
    ):
        """带重试的执行"""
        policy = policy or self.policy
        last_exception = None
        
        for attempt in range(1, policy.max_attempts + 1):
            try:
                result = (
                    await func(*args, **kwargs)
                    if asyncio.iscoroutinefunction(func)
                    else func(*args, **kwargs)
                )
                
                if attempt > 1:
                    self.stats["successful_retries"] += 1
                return result
                
            except policy.retryable_exceptions as e:
                last_exception = e
                self.stats["total_retires"] += 1
                
                if attempt >= policy.max_attempts:
                    self.stats["failed_retries"] += 1
                    logger.error(
                        "retry_failed_after_max_attempts",
                        max_attempts=policy.max_attempts,
                        error=str(e)
                    )
                    raise
                
                # 计算延迟
                delay = min(
                    policy.initial_delay * (policy.backoff_multiplier ** (attempt - 1)),
                    policy.max_delay
                )
                
                logger.warning(
                    "retrying_operation",
                    attempt=attempt,
                    max_attempts=policy.max_attempts,
                    delay=delay,
                    error=str(e)
                )
                
                await asyncio.sleep(delay)
        
        raise last_exception
    
    def get_stats(self) -> Dict[str, int]:
        """获取重试统计"""
        return self.stats.copy()


# 全局重试管理器实例
_retry_manager: Optional[EnhancedRetryManager] = None

def get_retry_manager() -> EnhancedRetryManager:
    """获取全局重试管理器"""
    global _retry_manager
    if _retry_manager is None:
        _retry_manager = EnhancedRetryManager()
    return _retry_manager
