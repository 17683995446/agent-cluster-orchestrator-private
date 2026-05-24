"""
🔧 增强版自适应队列 - 性能优化 + 智能调整 + 更完整功能
修复的问题:
1. 每次入队都 sort()，O(n log n) - ✅ 使用 heapq，O(log n)
2. 缺少等待超时机制 - ✅ 实现
3. 缺少容量调整冷却期 - ✅ 实现防止抖动
4. dequeue 从 index 0 pop 效率低 - ✅ 堆结构优化
"""
from __future__ import annotations
import asyncio
import time
import heapq
from dataclasses import dataclass, field
from typing import Any, List, Optional, Dict
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class EnhancedQueueConfig:
    """增强版队列配置"""
    initial_capacity: int = 100
    max_capacity: int = 1000
    growth_factor: float = 1.5
    shrink_factor: float = 0.5
    growth_cooldown: float = 1.0  # 扩容冷却时间
    shrink_cooldown: float = 5.0  # 缩容冷却时间
    default_wait_timeout: float = 30.0  # 默认等待超时


@dataclass(order=True)
class PriorityItem:
    """优先级项目 - 使用 heapq 优化"""
    priority: int
    created_at: float = field(compare=False)
    item: Any = field(compare=False)
    
    def __init__(self, priority: int, item: Any):
        # 负数优先级，高优先级在前
        self.priority = -priority
        self.created_at = time.time()
        self.item = item


class EnhancedAdaptiveQueue:
    """
    增强版自适应队列 - 生产级实现
    
    优化:
    - 使用 heapq 替代 list.sort()，性能 O(log n) vs O(n log n)
    - 容量调整冷却期，防止频繁抖动
    - 支持等待超时的 get/put
    - 完整的统计信息
    """
    
    def __init__(self, config: Optional[EnhancedQueueConfig] = None):
        self.config = config or EnhancedQueueConfig()
        self.capacity = self.config.initial_capacity
        
        # 使用 heapq 替代 list.sort()
        self.heap: List[PriorityItem] = []
        
        # 统计信息
        self.stats = {
            "enqueued": 0,
            "dequeued": 0,
            "rejected": 0,
            "growth_count": 0,
            "shrink_count": 0,
            "wait_timeouts": 0
        }
        
        # 线程安全
        self._lock = asyncio.Lock()
        
        # 非空条件变量
        self._not_empty = asyncio.Condition(self._lock)
        
        # 容量调整冷却
        self._last_growth: Optional[float] = None
        self._last_shrink: Optional[float] = None
        
        logger.info("enhanced_adaptive_queue_initialized", config=self.config)
    
    async def enqueue(
        self,
        item: Any,
        priority: int = 0,
        wait_timeout: Optional[float] = None
    ) -> bool:
        """
        入队 - 支持等待超时
        
        Args:
            item: 入队项目
            priority: 优先级（数字越大，优先级越高）
            wait_timeout: 等待超时时间，None 不等待
            
        Returns:
            是否成功入队
        """
        timeout = wait_timeout if wait_timeout is not None else self.config.default_wait_timeout
        
        async with self._not_empty:
            if len(self.heap) >= self.capacity:
                # 先检查是否可以拒绝低优先级项目（这个是更优先的策略）
                if self._can_reject_low_priority(priority):
                    self._reject_lowest_priority()
                elif self._can_grow():
                    self._grow()
                else:
                    # 不能拒绝也不能扩容，等待或返回
                    if timeout == 0:
                        self.stats["rejected"] += 1
                        return False
                    
                    wait_until = time.time() + timeout
                    while len(self.heap) >= self.capacity:
                        remaining = wait_until - time.time()
                        if remaining <= 0:
                            self.stats["wait_timeouts"] += 1
                            self.stats["rejected"] += 1
                            return False
                        
                        try:
                            await asyncio.wait_for(
                                self._not_empty.wait(),
                                timeout=remaining
                            )
                        except asyncio.TimeoutError:
                            pass
            
            # 使用 heapq 推送，O(log n)
            priority_item = PriorityItem(priority, item)
            heapq.heappush(self.heap, priority_item)
            self.stats["enqueued"] += 1
            
            # 通知等待者
            self._not_empty.notify()
            
            return True
    
    async def dequeue(
        self,
        wait_timeout: Optional[float] = None
    ) -> Optional[Any]:
        """
        出队 - 支持等待超时
        
        Args:
            wait_timeout: 等待超时时间，None 无限等待
            
        Returns:
            最高优先级的项目，或 None（超时/空队列）
        """
        timeout = wait_timeout
        
        async with self._not_empty:
            if not self.heap:
                if timeout == 0:
                    return None
                
                try:
                    await asyncio.wait_for(
                        self._not_empty.wait(),
                        timeout=timeout
                    )
                except asyncio.TimeoutError:
                    self.stats["wait_timeouts"] += 1
                    return None
                
                if not self.heap:
                    return None
            
            # 使用 heapq 弹出，O(log n)
            priority_item = heapq.heappop(self.heap)
            self.stats["dequeued"] += 1
            
            # 检查缩容
            if self._can_shrink():
                self._shrink()
            
            # 通知可能的等待入队者
            self._not_empty.notify()
            
            return priority_item.item
    
    def _can_grow(self) -> bool:
        """判断是否可以扩容"""
        if self.capacity >= self.config.max_capacity:
            return False
        
        if self._last_growth is not None:
            elapsed = time.time() - self._last_growth
            if elapsed < self.config.growth_cooldown:
                return False
        
        return True
    
    def _can_shrink(self) -> bool:
        """判断是否可以缩容"""
        if self.capacity <= self.config.initial_capacity:
            return False
        
        if len(self.heap) > self.capacity * self.config.shrink_factor:
            return False
        
        if self._last_shrink is not None:
            elapsed = time.time() - self._last_shrink
            if elapsed < self.config.shrink_cooldown:
                return False
        
        return True
    
    def _can_reject_low_priority(self, new_priority: int) -> bool:
        """判断是否可以拒绝低优先级项目"""
        if not self.heap:
            return False
        
        # 找到真正的最低优先级（所有项目中priority最小的）
        lowest_priority = float('inf')
        for item in self.heap:
            priority = -item.priority
            if priority < lowest_priority:
                lowest_priority = priority
        
        # 当新项目优先级 >= 最低优先级时，可以拒绝最低优先级项目
        return new_priority >= lowest_priority
    
    def _grow(self) -> None:
        """执行扩容"""
        old_capacity = self.capacity
        self.capacity = min(
            int(self.capacity * self.config.growth_factor),
            self.config.max_capacity
        )
        self._last_growth = time.time()
        self.stats["growth_count"] += 1
        
        logger.info(
            "enhanced_queue_grew",
            old_capacity=old_capacity,
            new_capacity=self.capacity
        )
    
    def _shrink(self) -> None:
        """执行缩容"""
        old_capacity = self.capacity
        self.capacity = max(
            int(self.capacity * self.config.shrink_factor),
            self.config.initial_capacity
        )
        self._last_shrink = time.time()
        self.stats["shrink_count"] += 1
        
        logger.info(
            "enhanced_queue_shrank",
            old_capacity=old_capacity,
            new_capacity=self.capacity
        )
    
    def _reject_lowest_priority(self) -> None:
        """拒绝最低优先级项目 - 从堆中移除最低优先级项目"""
        if self.heap:
            # 找到最低优先级项目（所有项目中priority最大的负数）
            # 由于heapq是小顶堆，heap[0]是优先级最高的，我们需要找到优先级最低的
            min_priority_idx = 0
            for i in range(1, len(self.heap)):
                if self.heap[i].priority > self.heap[min_priority_idx].priority:
                    min_priority_idx = i
            
            removed_item = self.heap.pop(min_priority_idx)
            # 重新堆化
            heapq.heapify(self.heap)
            
            self.stats["rejected"] += 1
            logger.debug("rejecting_lowest_priority_item", priority=-removed_item.priority)
            
            # 通知等待入队者现在有空位了！
            self._not_empty.notify()
    
    def size(self) -> int:
        """获取当前队列大小"""
        return len(self.heap)
    
    def is_empty(self) -> bool:
        """判断是否为空"""
        return len(self.heap) == 0
    
    def get_stats(self) -> Dict[str, Any]:
        """获取详细统计信息"""
        stats_copy = dict(self.stats)
        stats_copy["current_size"] = len(self.heap)
        stats_copy["capacity"] = self.capacity
        stats_copy["utilization"] = (len(self.heap) / self.capacity * 100) if self.capacity > 0 else 0
        return stats_copy
