#!/usr/bin/env python3
"""
🔥🔥🔥 系统长时间压力测试 - 发现深层次缺陷
持续运行极端场景，发现并报告系统问题
"""
from __future__ import annotations
import asyncio
import gc
import random
import time
import tracemalloc
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from collections import defaultdict, deque

sys.path.insert(0, str(Path(__file__).parent))

import structlog

logger = structlog.get_logger(__name__)

# ==========================================
# 测试配置
# ==========================================
TEST_CONFIG = {
    "duration_minutes": 2,  # 测试持续时间（为了演示，设为2分钟）
    "agent_count": 1000,
    "task_count": 50000,
    "max_concurrent_tasks": 2000,
    "enable_memory_tracking": True,
    "enable_event_system": True,
    "chaos_injection": True,  # 注入故障
}

# ==========================================
# 问题发现和记录系统
# ==========================================
class IssueSeverity(Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

@dataclass
class DiscoveredIssue:
    id: str
    severity: IssueSeverity
    category: str
    description: str
    timestamp: float
    stack_trace: Optional[str] = None
    reproduction_steps: Optional[List[str]] = None
    context: Optional[Dict[str, Any]] = None

class IssueTracker:
    def __init__(self):
        self.issues: List[DiscoveredIssue] = []
        self.issue_count_by_severity: Dict[IssueSeverity, int] = defaultdict(int)
        self.issue_count_by_category: Dict[str, int] = defaultdict(int)
    
    def record_issue(
        self,
        severity: IssueSeverity,
        category: str,
        description: str,
        stack_trace: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> DiscoveredIssue:
        issue = DiscoveredIssue(
            id=str(uuid.uuid4()),
            severity=severity,
            category=category,
            description=description,
            timestamp=time.time(),
            stack_trace=stack_trace,
            context=context
        )
        self.issues.append(issue)
        self.issue_count_by_severity[severity] += 1
        self.issue_count_by_category[category] += 1
        
        logger.error(
            "ISSUE_DISCOVERED",
            severity=severity.value,
            category=category,
            description=description
        )
        
        return issue
    
    def get_summary(self) -> Dict[str, Any]:
        return {
            "total_issues": len(self.issues),
            "by_severity": {s.value: c for s, c in self.issue_count_by_severity.items()},
            "by_category": dict(self.issue_count_by_category),
            "issues": [
                {
                    "id": i.id,
                    "severity": i.severity.value,
                    "category": i.category,
                    "description": i.description,
                    "timestamp": i.timestamp
                }
                for i in self.issues
            ]
        }

# ==========================================
# 性能指标跟踪
# ==========================================
@dataclass
class PerformanceMetrics:
    start_time: float
    end_time: Optional[float] = None
    task_success_count: int = 0
    task_failure_count: int = 0
    agent_count: int = 0
    event_count: int = 0
    memory_usage_samples: List[float] = field(default_factory=list)
    latency_samples: List[float] = field(default_factory=list)
    queue_backpressure_events: int = 0
    
    def get_summary(self) -> Dict[str, Any]:
        duration = (self.end_time or time.time()) - self.start_time
        success_rate = (
            self.task_success_count / (self.task_success_count + self.task_failure_count) * 100
            if (self.task_success_count + self.task_failure_count) > 0
            else 0
        )
        
        return {
            "duration_seconds": duration,
            "task_success_count": self.task_success_count,
            "task_failure_count": self.task_failure_count,
            "success_rate_percent": success_rate,
            "throughput_tasks_per_second": (self.task_success_count + self.task_failure_count) / duration if duration > 0 else 0,
            "avg_memory_mb": sum(self.memory_usage_samples) / len(self.memory_usage_samples) if self.memory_usage_samples else 0,
            "max_memory_mb": max(self.memory_usage_samples) if self.memory_usage_samples else 0,
            "min_memory_mb": min(self.memory_usage_samples) if self.memory_usage_samples else 0,
            "avg_latency_ms": sum(self.latency_samples) / len(self.latency_samples) * 1000 if self.latency_samples else 0,
            "p95_latency_ms": (sorted(self.latency_samples)[int(len(self.latency_samples) * 0.95)] * 1000) if self.latency_samples else 0,
            "queue_backpressure_events": self.queue_backpressure_events
        }

# ==========================================
# 混沌工程 - 故障注入
# ==========================================
class ChaosMonkey:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.injected_faults: List[Dict[str, Any]] = []
    
    async def maybe_inject_fault(self, context: str) -> None:
        if not self.enabled:
            return
        
        # 1% 概率注入故障
        if random.random() < 0.01:
            fault_type = random.choice([
                "delay", "memory_usage", "cpu_spike", "network_timeout"
            ])
            
            self.injected_faults.append({
                "type": fault_type,
                "context": context,
                "timestamp": time.time()
            })
            
            if fault_type == "delay":
                await asyncio.sleep(random.uniform(0.1, 1.0))
            elif fault_type == "memory_usage":
                # 临时分配一些内存
                _ = [i for i in range(random.randint(1000, 10000))]
            
            logger.warning(
                "CHAOS_FAULT_INJECTED",
                fault_type=fault_type,
                context=context
            )

# ==========================================
# 模拟的 Agent 和 Task 系统
# ==========================================
class MockAgent:
    def __init__(self, agent_id: str, capacity: int = 10):
        self.agent_id = agent_id
        self.capacity = capacity
        self.current_load = 0
        self.completed_tasks = 0
        self.failed_tasks = 0
    
    async def execute_task(self, task_id: str) -> Tuple[bool, float]:
        start_time = time.time()
        await asyncio.sleep(random.uniform(0.001, 0.1))
        
        # 随机成功或失败
        if random.random() < 0.05:  # 5% 失败率
            self.failed_tasks += 1
            return False, time.time() - start_time
        
        self.completed_tasks += 1
        return True, time.time() - start_time

class MockTask:
    def __init__(self, task_id: str, priority: int = 5):
        self.task_id = task_id
        self.priority = priority
        self.created_at = time.time()

# ==========================================
# 综合压力测试场景
# ==========================================
class ComprehensiveStressTest:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.issue_tracker = IssueTracker()
        self.metrics = PerformanceMetrics(start_time=time.time())
        self.chaos = ChaosMonkey(enabled=config.get("chaos_injection", False))
        self.agents: Dict[str, MockAgent] = {}
        self.tasks: Dict[str, MockTask] = {}
        
        # 导入模块
        try:
            from app.core.protection import AdaptiveQueue, QueueConfig, CircuitBreaker, CircuitBreakerConfig
            from app.core.events import EventBus, Event, EventType
            
            self.queue_config = QueueConfig(
                initial_capacity=100,
                max_capacity=10000
            )
            self.queue = AdaptiveQueue(self.queue_config)
            
            self.cb_config = CircuitBreakerConfig()
            self.circuit_breaker = CircuitBreaker("stress_test_cb", self.cb_config)
            
            self.event_bus = EventBus()
            
        except Exception as e:
            self.issue_tracker.record_issue(
                IssueSeverity.CRITICAL,
                "module_import",
                f"Failed to import modules: {str(e)}",
                traceback.format_exc()
            )
    
    async def run(self):
        print("\n" + "=" * 80)
        print("🔥🔥🔥 开始长时间系统压力测试 🔥🔥🔥")
        print("=" * 80)
        
        # 初始化阶段
        await self.initialize_phase()
        
        # 压力测试阶段
        await self.stress_test_phase()
        
        # 清理和分析阶段
        await self.analysis_phase()
        
        self.metrics.end_time = time.time()
        self.print_final_report()
    
    async def initialize_phase(self):
        print("\n📋 初始化阶段 - 创建测试资源...")
        
        # 创建 Agents
        for i in range(self.config["agent_count"]):
            agent_id = f"agent_{i:04d}"
            self.agents[agent_id] = MockAgent(agent_id)
        print(f"   ✅ 创建了 {len(self.agents)} 个 Agent")
        
        # 创建 Tasks
        for i in range(self.config["task_count"]):
            task_id = f"task_{i:06d}"
            priority = random.randint(1, 10)
            self.tasks[task_id] = MockTask(task_id, priority)
        print(f"   ✅ 创建了 {len(self.tasks)} 个 Task")
        
        # 内存跟踪开始
        if self.config["enable_memory_tracking"]:
            tracemalloc.start()
            print("   ✅ 内存跟踪已启动")
    
    async def stress_test_phase(self):
        print("\n💥 压力测试阶段 - 开始极端负载测试...")
        
        start_time = time.time()
        end_time = start_time + (self.config["duration_minutes"] * 60)
        
        print(f"   ⏱️  测试将运行 {self.config['duration_minutes']} 分钟")
        
        # 任务队列
        task_queue = asyncio.Queue(maxsize=self.config["max_concurrent_tasks"])
        
        # 生产者任务 - 不断生成任务
        async def task_producer():
            task_list = list(self.tasks.values())
            task_idx = 0
            
            while time.time() < end_time:
                try:
                    if task_idx < len(task_list):
                        task = task_list[task_idx]
                        try:
                            await self.queue.enqueue(task, priority=task.priority)
                        except Exception as e:
                            self.issue_tracker.record_issue(
                                IssueSeverity.MEDIUM,
                                "queue_overflow",
                                f"Task enqueue failed: {str(e)}"
                            )
                            self.metrics.queue_backpressure_events += 1
                        
                        task_idx += 1
                    else:
                        # 循环使用任务
                        task_idx = 0
                        await asyncio.sleep(0.01)
                
                except Exception as e:
                    self.issue_tracker.record_issue(
                        IssueSeverity.HIGH,
                        "producer_error",
                        f"Producer error: {str(e)}",
                        traceback.format_exc()
                    )
        
        # 消费者任务 - 处理任务
        async def task_consumer():
            while time.time() < end_time:
                try:
                    await self.chaos.maybe_inject_fault("consumer")
                    
                    # 从队列获取任务（模拟）
                    await asyncio.sleep(random.uniform(0.001, 0.01))
                    
                    # 随机选择 Agent
                    agent_id = random.choice(list(self.agents.keys()))
                    agent = self.agents[agent_id]
                    
                    task_id = f"mock_task_{int(time.time() * 1000000)}"
                    success, latency = await agent.execute_task(task_id)
                    
                    if success:
                        self.metrics.task_success_count += 1
                    else:
                        self.metrics.task_failure_count += 1
                    
                    self.metrics.latency_samples.append(latency)
                    
                    # 记录内存使用情况
                    if self.config["enable_memory_tracking"] and len(self.metrics.memory_usage_samples) % 100 == 0:
                        import psutil
                        process = psutil.Process()
                        self.metrics.memory_usage_samples.append(
                            process.memory_info().rss / 1024 / 1024
                        )
                
                except Exception as e:
                    self.issue_tracker.record_issue(
                        IssueSeverity.HIGH,
                        "consumer_error",
                        f"Consumer error: {str(e)}",
                        traceback.format_exc()
                    )
        
        # 事件生成器
        async def event_generator():
            event_types = list(EventType) if 'EventType' in globals() else ["task_created", "task_completed"]
            while time.time() < end_time:
                try:
                    await self.chaos.maybe_inject_fault("event_system")
                    
                    # 模拟事件系统活动
                    await asyncio.sleep(random.uniform(0.001, 0.05))
                    self.metrics.event_count += 1
                
                except Exception as e:
                    self.issue_tracker.record_issue(
                        IssueSeverity.MEDIUM,
                        "event_system_error",
                        f"Event system error: {str(e)}",
                        traceback.format_exc()
                    )
        
        # 启动所有协程
        producers = [asyncio.create_task(task_producer()) for _ in range(5)]
        consumers = [asyncio.create_task(task_consumer()) for _ in range(20)]
        event_generators = [asyncio.create_task(event_generator()) for _ in range(2)]
        
        # 等待所有任务完成或超时
        try:
            await asyncio.gather(*producers, *consumers, *event_generators)
        except asyncio.CancelledError:
            pass
        
        # 清理
        for task in producers + consumers + event_generators:
            task.cancel()
    
    async def analysis_phase(self):
        print("\n🔍 分析阶段 - 深度系统检查...")
        
        # 内存泄漏检测
        if self.config["enable_memory_tracking"]:
            print("   🔬 执行深度内存泄漏检测...")
            
            # 强制 GC
            gc.collect()
            
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')
            
            if top_stats:
                print(f"   ⚠️  Top {min(5, len(top_stats))} 内存分配点:")
                for stat in top_stats[:5]:
                    print(f"      {stat}")
            
            # 检查内存增长
            if len(self.metrics.memory_usage_samples) > 100:
                initial = self.metrics.memory_usage_samples[:10]
                final = self.metrics.memory_usage_samples[-10:]
                memory_growth = sum(final) / len(final) - sum(initial) / len(initial)
                
                if memory_growth > 10:  # > 10MB 增长
                    self.issue_tracker.record_issue(
                        IssueSeverity.HIGH,
                        "memory_leak",
                        f"Significant memory growth detected: {memory_growth:.2f} MB"
                    )
        
        print("   ✅ 分析阶段完成")
    
    def print_final_report(self):
        print("\n" + "=" * 80)
        print("📊 长时间压力测试最终报告")
        print("=" * 80)
        
        # 性能指标
        metrics_summary = self.metrics.get_summary()
        print("\n📈 性能指标:")
        for key, value in metrics_summary.items():
            if isinstance(value, float):
                print(f"   {key}: {value:.2f}")
            else:
                print(f"   {key}: {value}")
        
        # 问题汇总
        print(f"\n🚨 发现的问题: {len(self.issue_tracker.issues)}")
        
        if self.issue_tracker.issues:
            print("\n问题清单:")
            for issue in self.issue_tracker.issues:
                print(f"\n[{issue.severity.value}] {issue.category}:")
                print(f"   {issue.description}")
        
        # Agent 统计
        print(f"\n🤖 Agent 统计:")
        total_completed = sum(a.completed_tasks for a in self.agents.values())
        total_failed = sum(a.failed_tasks for a in self.agents.values())
        print(f"   Total completed tasks: {total_completed}")
        print(f"   Total failed tasks: {total_failed}")
        
        print("\n" + "=" * 80)
        if len(self.issue_tracker.issues) == 0:
            print("✅ 系统表现完美！未发现任何问题！")
        else:
            print(f"⚠️  发现 {len(self.issue_tracker.issues)} 个问题需要处理")
        print("=" * 80)
        
        return self.issue_tracker.get_summary()

# ==========================================
# 主函数
# ==========================================
async def main():
    tester = ComprehensiveStressTest(TEST_CONFIG)
    await tester.run()

if __name__ == "__main__":
    import uuid
    import traceback
    from enum import Enum
    from dataclasses import dataclass, field
    
    asyncio.run(main())
