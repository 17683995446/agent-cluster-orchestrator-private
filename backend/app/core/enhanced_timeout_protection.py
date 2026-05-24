"""
🔧 增强版超时保护 - 线程安全 + 更完整功能
修复的问题:
1. TimeoutMonitor 缺少线程安全保护 - ✅ 已添加
2. execute_with_timeout 不够灵活 - ✅ 增强
3. 缺少超时统计 - ✅ 完整统计
"""
from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class EnhancedTimeoutConfig:
    """增强版超时配置"""
    default_timeout: float = 5.0
    max_timeout: float = 30.0
    enable_timeout_protection: bool = True
    enable_retry: bool = True
    max_retries: int = 3
    retry_delay: float = 1.0
    max_history_size: int = 1000


class TimeoutError(Exception):
    """超时异常"""
    pass


@dataclass
class TimeoutEvent:
    """超时事件记录"""
    task_id: str
    timeout: float
    duration: float
    success: bool
    retries: int
    timestamp: str
    error: Optional[str] = None


class EnhancedTimeoutMonitor:
    """
    增强版超时监控器 - 线程安全
    
    特性:
    - 线程安全的所有操作
    - 完整的历史记录
    - 详细的统计信息
    - 支持重试逻辑
    """
    
    def __init__(self, config: Optional[EnhancedTimeoutConfig] = None):
        self.config = config or EnhancedTimeoutConfig()
        
        # 线程安全
        self._lock = asyncio.Lock()
        
        # 历史记录
        self.timeouts: List[TimeoutEvent] = []
        self.successes: List[TimeoutEvent] = []
        
        # 回调
        self.callbacks: List[Callable] = []
        
        # 统计
        self._stats = {
            "total_executions": 0,
            "total_timeouts": 0,
            "total_successes": 0,
            "total_retries": 0,
            "avg_duration": 0.0,
            "success_rate": 100.0
        }
        
        logger.info("enhanced_timeout_monitor_initialized")
    
    async def register_callback(
        self,
        callback: Callable[[str, float], None]
    ) -> None:
        """线程安全的回调注册"""
        async with self._lock:
            self.callbacks.append(callback)
    
    async def on_timeout(
        self,
        task_id: str,
        timeout: float,
        duration: float,
        error: Optional[str] = None
    ) -> None:
        """记录超时事件 - 线程安全"""
        event = TimeoutEvent(
            task_id=task_id,
            timeout=timeout,
            duration=duration,
            success=False,
            retries=0,
            timestamp=datetime.now(timezone.utc).isoformat(),
            error=error
        )
        
        async with self._lock:
            self.timeouts.append(event)
            
            if len(self.timeouts) > self.config.max_history_size:
                self.timeouts = self.timeouts[-self.config.max_history_size:]
            
            self._stats["total_executions"] += 1
            self._stats["total_timeouts"] += 1
            self._update_stats()
            
            # 触发回调
            for callback in self.callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(task_id, timeout)
                    else:
                        callback(task_id, timeout)
                except Exception as e:
                    logger.error("timeout_callback_error", error=str(e))
    
    async def on_success(
        self,
        task_id: str,
        timeout: float,
        duration: float,
        retries: int = 0
    ) -> None:
        """记录成功事件 - 线程安全"""
        event = TimeoutEvent(
            task_id=task_id,
            timeout=timeout,
            duration=duration,
            success=True,
            retries=retries,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
        
        async with self._lock:
            self.successes.append(event)
            
            if len(self.successes) > self.config.max_history_size:
                self.successes = self.successes[-self.config.max_history_size:]
            
            self._stats["total_executions"] += 1
            self._stats["total_successes"] += 1
            if retries > 0:
                self._stats["total_retries"] += retries
            self._update_stats()
    
    def _update_stats(self) -> None:
        """内部更新统计"""
        total = self._stats["total_executions"]
        success = self._stats["total_successes"]
        
        if total > 0:
            self._stats["success_rate"] = (success / total * 100)
        
        all_events = self.timeouts + self.successes
        if all_events:
            avg_duration = sum(e.duration for e in all_events) / len(all_events)
            self._stats["avg_duration"] = avg_duration
    
    async def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        async with self._lock:
            return dict(self._stats)
    
    async def get_recent_events(self, limit: int = 10) -> List[TimeoutEvent]:
        """获取最近事件"""
        async with self._lock:
            all_events = self.timeouts + self.successes
            all_events.sort(key=lambda e: e.timestamp, reverse=True)
            return all_events[:limit]


async def enhanced_execute_with_timeout(
    coro_or_func,
    timeout: float,
    task_name: Optional[str] = None,
    monitor: Optional[EnhancedTimeoutMonitor] = None,
    enable_retry: bool = False,
    max_retries: int = 3,
    retry_delay: float = 1.0
) -> Any:
    """
    增强版超时执行
    
    特性:
    - 支持同步/异步函数
    - 可选的重试逻辑
    - 可选的监控集成
    - 更详细的日志
    """
    task_id = task_name or f"task_{int(time.time())}"
    start_time = time.time()
    retries = 0
    
    async def execute_once():
        if asyncio.iscoroutine(coro_or_func):
            return await coro_or_func
        elif asyncio.iscoroutinefunction(coro_or_func):
            return await coro_or_func()
        else:
            # 同步函数在线程池中执行
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, coro_or_func)
    
    while True:
        try:
            result = await asyncio.wait_for(
                execute_once(),
                timeout=timeout
            )
            
            duration = time.time() - start_time
            if monitor:
                await monitor.on_success(task_id, timeout, duration, retries)
            
            return result
            
        except asyncio.TimeoutError:
            duration = time.time() - start_time
            
            if retries < max_retries and enable_retry:
                retries += 1
                logger.warning(
                    "task_timeout_retrying",
                    task_id=task_id,
                    timeout=timeout,
                    attempt=retries,
                    max_attempts=max_retries
                )
                
                await asyncio.sleep(retry_delay)
                continue
            
            logger.warning(
                "task_timeout",
                task_id=task_id,
                timeout=timeout,
                duration=duration
            )
            
            if monitor:
                await monitor.on_timeout(
                    task_id,
                    timeout,
                    duration,
                    "execution_timeout"
                )
            
            raise TimeoutError(
                f"Task '{task_id}' exceeded timeout of {timeout}s "
                f"after {retries} retries"
            )
            
        except Exception as e:
            duration = time.time() - start_time
            
            if retries < max_retries and enable_retry:
                retries += 1
                logger.warning(
                    "task_error_retrying",
                    task_id=task_id,
                    error=str(e),
                    attempt=retries,
                    max_attempts=max_retries
                )
                
                await asyncio.sleep(retry_delay)
                continue
            
            if monitor:
                await monitor.on_timeout(
                    task_id,
                    timeout,
                    duration,
                    str(e)
                )
            
            raise
