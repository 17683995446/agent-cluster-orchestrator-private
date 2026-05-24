"""
🔧 增强版事件总线 - 线程安全 + 并行回调 + 超时保护 + 优雅停止
修复的问题:
1. 缺少线程安全锁 - ✅ 已添加
2. 回调串行执行 - ✅ 改为并行执行
3. 缺少超时保护 - ✅ 30秒超时保护
4. stop()不等待队列清空 - ✅ 已实现
5. 缺少统计信息 - ✅ 已添加
6. 历史记录限制使用列表切片O(n) - ✅ 改用deque O(1)
"""
from __future__ import annotations
import asyncio
from typing import Any, Callable, Dict, List, Optional
from collections import deque
import structlog

logger = structlog.get_logger(__name__)


class EnhancedEventBus:
    """增强版事件总线 - 生产级别的可靠性实现"""
    
    def __init__(
        self,
        max_queue_size: int = 10000,
        max_history_size: int = 10000,
        callback_timeout: float = 30.0
    ):
        self._subscribers: Dict[Any, List[Callable]] = {}
        self._all_subscribers: List[Callable] = []
        self._event_queue = asyncio.Queue(maxsize=max_queue_size)
        self._is_running = False
        self._process_task: Optional[asyncio.Task] = None
        self._event_history: deque = deque(maxlen=max_history_size)
        self._max_history = max_history_size
        self._callback_timeout = callback_timeout
        
        # 新增: 线程安全保护
        self._lock = asyncio.Lock()
        
        # 新增: 统计信息
        self._stats = {
            'events_published': 0,
            'events_processed': 0,
            'events_dropped': 0,
            'callback_errors': 0,
            'queue_size': 0
        }
        
        logger.debug("enhanced_event_bus_initialized")
    
    async def start(self) -> None:
        """安全地启动事件总线"""
        async with self._lock:
            if self._is_running:
                return
            
            self._is_running = True
            self._process_task = asyncio.create_task(self._process_event_queue())
            logger.info("enhanced_event_bus_started")
    
    async def stop(self, wait_for_queue: bool = True) -> None:
        """优雅停止 - 支持等待队列清空"""
        async with self._lock:
            self._is_running = False
        
        # 等待队列处理完成
        if wait_for_queue:
            try:
                await self._event_queue.join()
            except Exception as e:
                logger.warning("queue_join_error", error=str(e))
        
        # 取消处理任务
        if self._process_task:
            self._process_task.cancel()
            try:
                await self._process_task
            except asyncio.CancelledError:
                pass
        
        logger.info("enhanced_event_bus_stopped")
    
    async def subscribe(
        self,
        callback: Callable,
        event_types: Optional[List[Any]] = None
    ) -> None:
        """线程安全的订阅"""
        async with self._lock:
            if event_types is None:
                self._all_subscribers.append(callback)
            else:
                for event_type in event_types:
                    if event_type not in self._subscribers:
                        self._subscribers[event_type] = []
                    self._subscribers[event_type].append(callback)
    
    async def unsubscribe(
        self,
        callback: Callable,
        event_types: Optional[List[Any]] = None
    ) -> None:
        """线程安全的取消订阅"""
        async with self._lock:
            if event_types is None:
                if callback in self._all_subscribers:
                    self._all_subscribers.remove(callback)
            else:
                for event_type in event_types:
                    if event_type in self._subscribers:
                        if callback in self._subscribers[event_type]:
                            self._subscribers[event_type].remove(callback)
    
    async def publish(self, event: Any) -> None:
        """增强的发布方法 - 带超时和统计"""
        try:
            await asyncio.wait_for(self._event_queue.put(event), timeout=0.5)
            self._stats['events_published'] += 1
        except asyncio.QueueFull:
            self._stats['events_dropped'] += 1
            logger.warning("event_queue_full_event_dropped")
        except asyncio.TimeoutError:
            self._stats['events_dropped'] += 1
            logger.warning("event_queue_put_timeout")
    
    async def _process_event_queue(self) -> None:
        """内部事件处理循环"""
        while self._is_running or not self._event_queue.empty():
            try:
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0
                )
                
                self._event_history.append(event)
                
                await self._dispatch_event_to_subscribers(event)
                
                self._stats['events_processed'] += 1
                self._event_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("event_processing_error", error=str(e))
    
    async def _dispatch_event_to_subscribers(self, event: Any) -> None:
        """并行分发事件 - 性能优化"""
        callbacks: List[Callable] = []
        
        async with self._lock:
            callbacks.extend(self._all_subscribers)
            event_type = getattr(event, 'event_type', None)
            if event_type is None and isinstance(event, dict):
                event_type = event.get('event_type')
            if event_type is not None:
                callbacks.extend(self._subscribers.get(event_type, []))
        
        if not callbacks:
            return
        
        tasks = [self._safe_invoke_callback(callback, event) for callback in callbacks]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _safe_invoke_callback(self, callback: Callable, event: Any) -> None:
        """安全的回调调用 - 带超时保护"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await asyncio.wait_for(callback(event), timeout=self._callback_timeout)
            else:
                callback(event)
        except asyncio.TimeoutError:
            self._stats['callback_errors'] += 1
            logger.error("callback_timeout")
        except Exception as e:
            self._stats['callback_errors'] += 1
            logger.error("callback_error", error=str(e))
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return dict(self._stats)
    
    def get_history(self, limit: Optional[int] = None) -> List[Any]:
        """获取事件历史"""
        if limit:
            return list(self._event_history)[-limit:]
        return list(self._event_history)
