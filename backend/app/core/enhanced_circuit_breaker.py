"""
🔧 增强版熔断器 - 线程安全 + 慢调用检测 + 滑动窗口统计
修复的问题:
1. 缺少线程安全锁 - ✅ 已添加
2. 缺少慢调用检测 - ✅ 已实现
3. 缺少滑动窗口统计 - ✅ 已实现
4. 缺少详细统计 - ✅ 已添加
"""
from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import structlog

logger = structlog.get_logger(__name__)


class CircuitState(str, Enum):
    """熔断器状态"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class EnhancedCircuitBreakerConfig:
    """增强版熔断器配置"""
    failure_threshold: int = 5
    success_threshold: int = 3
    timeout_duration: float = 60.0
    half_open_max_calls: int = 3
    failure_rate_threshold: float = 0.5
    min_call_count: int = 5
    slow_call_threshold: float = 2.0
    slow_call_rate_threshold: float = 0.3
    window_size: int = 100  # 滑动窗口大小


class EnhancedCircuitBreaker:
    """增强版熔断器 - 生产级别的可靠性实现"""
    
    def __init__(
        self,
        name: str,
        config: Optional[EnhancedCircuitBreakerConfig] = None,
        on_state_change: Optional[Callable[[CircuitState, CircuitState], Any]] = None
    ):
        self.name = name
        self.config = config or EnhancedCircuitBreakerConfig()
        self.on_state_change = on_state_change
        
        # 线程安全保护
        self._lock = asyncio.Lock()
        
        # 状态管理
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.slow_call_count = 0
        self.total_calls = 0
        self.last_failure_time: Optional[float] = None
        self.half_open_calls = 0
        
        # 滑动窗口统计
        self._call_window: List[Dict[str, Any]] = []
        self._max_window_size = self.config.window_size
        
        # 统计信息
        self._stats = {
            'total_calls': 0,
            'failed_calls': 0,
            'slow_calls': 0,
            'state_transitions': 0
        }
        
        logger.info("enhanced_circuit_breaker_initialized", name=name, config=self.config)
    
    async def call(
        self,
        func: Callable,
        *args,
        fallback: Optional[Callable]=None,
        **kwargs
    ) -> Any:
        """通过熔断器执行函数"""
        async with self._lock:
            if not await self._allow_request():
                if fallback:
                    if asyncio.iscoroutinefunction(fallback):
                        return await fallback(*args, **kwargs)
                    else:
                        return fallback(*args, **kwargs)
                raise Exception(f"Circuit breaker '{self.name}' is {self.state}")
        
        start_time = time.time()
        
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            elif asyncio.iscoroutine(func):
                result = await func
            else:
                result = func(*args, **kwargs)
            
            duration = time.time() - start_time
            is_slow_call = duration > self.config.slow_call_threshold
            
            await self._on_success(duration, is_slow_call)
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            await self._on_failure(duration, str(e))
            raise
    
    async def _allow_request(self) -> bool:
        """判断是否允许请求通过"""
        if self.state == CircuitState.CLOSED:
            return True
        
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                await self._transition_to(CircuitState.HALF_OPEN)
                return True
            return False
        
        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_calls < self.config.half_open_max_calls:
                self.half_open_calls += 1
                return True
            return False
        
        return False
    
    def _should_attempt_reset(self) -> bool:
        """检查是否应该尝试恢复"""
        if self.last_failure_time is None:
            return True
        
        elapsed = time.time() - self.last_failure_time
        return elapsed >= self.config.timeout_duration
    
    async def _on_success(self, duration: float, is_slow_call: bool) -> None:
        """处理成功调用"""
        async with self._lock:
            self.success_count += 1
            self.total_calls += 1
            self._stats['total_calls'] += 1
            
            if is_slow_call:
                self.slow_call_count += 1
                self._stats['slow_calls'] += 1
            
            self._record_call(duration, success=True, slow=is_slow_call)
            
            if self.state == CircuitState.HALF_OPEN:
                if self.success_count >= self.config.success_threshold:
                    logger.info("circuit_breaker_recovered", name=self.name)
                    await self._transition_to(CircuitState.CLOSED)
            elif self.state == CircuitState.CLOSED:
                if await self._should_open():
                    await self._transition_to(CircuitState.OPEN)
    
    async def _on_failure(self, duration: float, error: str) -> None:
        """处理失败调用"""
        async with self._lock:
            self.failure_count += 1
            self.total_calls += 1
            self._stats['total_calls'] += 1
            self.success_count = 0
            self.last_failure_time = time.time()
            self._stats['failed_calls'] += 1
            
            self._record_call(duration, success=False, slow=False)
            
            if self.state == CircuitState.CLOSED:
                if await self._should_open():
                    await self._transition_to(CircuitState.OPEN)
            elif self.state == CircuitState.HALF_OPEN:
                logger.warning("circuit_breaker_reopened", name=self.name)
                await self._transition_to(CircuitState.OPEN)
    
    async def _should_open(self) -> bool:
        """智能判断是否应该打开熔断器"""
        if self.total_calls < self.config.min_call_count:
            return False
        
        recent_calls = self._call_window[-self.config.min_call_count:]
        if not recent_calls:
            return False
        
        failure_rate = sum(1 for call in recent_calls if not call['success']) / len(recent_calls)
        slow_rate = sum(1 for call in recent_calls if call['slow']) / len(recent_calls)
        
        return (
            failure_rate >= self.config.failure_rate_threshold or
            slow_rate >= self.config.slow_call_rate_threshold or
            self.failure_count >= self.config.failure_threshold
        )
    
    def _record_call(self, duration: float, success: bool, slow: bool) -> None:
        """记录调用到滑动窗口"""
        call_info = {
            'timestamp': time.time(),
            'duration': duration,
            'success': success,
            'slow': slow
        }
        
        self._call_window.append(call_info)
        if len(self._call_window) > self._max_window_size:
            self._call_window.pop(0)
    
    async def _transition_to(self, new_state: CircuitState) -> None:
        """状态转换"""
        old_state = self.state
        self.state = new_state
        
        if new_state == CircuitState.CLOSED:
            self.failure_count = 0
            self.success_count = 0
            self.half_open_calls = 0
        
        self._stats['state_transitions'] += 1
        
        logger.info(
            "circuit_breaker_state_changed",
            name=self.name,
            old_state=old_state,
            new_state=new_state
        )
        
        if self.on_state_change:
            try:
                if asyncio.iscoroutinefunction(self.on_state_change):
                    await self.on_state_change(old_state, new_state)
                else:
                    self.on_state_change(old_state, new_state)
            except Exception as e:
                logger.error("state_change_callback_error", error=str(e))
    
    def get_stats(self) -> Dict[str, Any]:
        """获取详细统计信息"""
        stats_copy = dict(self._stats)
        stats_copy['state'] = self.state
        stats_copy['failure_count'] = self.failure_count
        stats_copy['success_count'] = self.success_count
        stats_copy['window_size'] = len(self._call_window)
        return stats_copy
