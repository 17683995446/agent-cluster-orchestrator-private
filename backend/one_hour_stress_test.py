"""
🚀 1小时深度压力测试 - 长时间真实运行测试
目标: 发现系统在长时间运行中的潜在问题
"""
import asyncio
import time
import random
import sys
import psutil
import os
from typing import List, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime
import structlog

sys.path.insert(0, '/workspace/backend')

logger = structlog.get_logger()


@dataclass
class LongTermBug:
    bug_id: str
    severity: str
    component: str
    description: str
    timestamp: str
    metrics: Dict[str, Any]
    fix_suggestion: str


@dataclass
class SystemMetrics:
    timestamp: float
    cpu_percent: float
    memory_mb: float
    event_bus_stats: Dict = field(default_factory=dict)
    circuit_breaker_stats: Dict = field(default_factory=dict)
    queue_stats: Dict = field(default_factory=dict)


class OneHourStressTest:
    """1小时深度压力测试"""

    def __init__(self):
        self.test_duration = 3600  # 1小时
        self.bugs_found: List[LongTermBug] = []
        self.metrics_history: List[SystemMetrics] = []
        self.start_time = None
        self.check_interval = 10  # 每10秒检查一次

    async def run(self):
        """运行1小时压力测试"""
        self.start_time = time.time()
        
        print("\n" + "="*80)
        print("🚀 1小时深度压力测试开始")
        print(f"   开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   测试时长: {self.test_duration}秒 (1小时)")
        print("="*80 + "\n")

        # 创建测试任务
        test_tasks = [
            asyncio.create_task(self._continuous_event_bus_test()),
            asyncio.create_task(self._continuous_circuit_breaker_test()),
            asyncio.create_task(self._continuous_queue_test()),
            asyncio.create_task(self._resource_monitor_test()),
            asyncio.create_task(self._fault_injection_test()),
            asyncio.create_task(self._memory_leak_detection()),
            asyncio.create_task(self._concurrency_stress_test()),
        ]

        # 运行所有测试
        await asyncio.gather(*test_tasks)

        duration = time.time() - self.start_time
        self._generate_report(duration)

    async def _continuous_event_bus_test(self):
        """持续EventBus测试"""
        from app.core.enhanced_event_bus import EnhancedEventBus

        bus = EnhancedEventBus(max_history_size=1000, max_queue_size=5000)
        await bus.start()

        print(f"[EventBus] 开始持续测试...")
        
        event_count = {"published": 0, "processed": 0, "dropped": 0}
        callback_errors = {"count": 0}
        large_event_count = {"count": 0}

        async def counting_callback(event):
            event_count["processed"] += 1
            if random.random() < 0.001:
                callback_errors["count"] += 1
                raise ValueError("Random callback error")

        await bus.subscribe(counting_callback)

        start = time.time()
        iteration = 0
        
        while time.time() - start < self.test_duration:
            iteration += 1
            
            # 正常事件
            batch_size = random.randint(50, 200)
            for i in range(batch_size):
                await bus.publish({
                    "event_id": i,
                    "iteration": iteration,
                    "timestamp": time.time()
                })
                event_count["published"] += 1

            # 大事件（测试内存）
            if iteration % 10 == 0:
                await bus.publish({
                    "event_id": f"large_{iteration}",
                    "data": "x" * 10000  # 10KB
                })
                large_event_count["count"] += 1

            # 小憩一下
            await asyncio.sleep(random.uniform(0.05, 0.2))

            # 定期报告
            if iteration % 100 == 0:
                stats = bus.get_stats()
                elapsed = time.time() - start
                print(f"[EventBus] 迭代 {iteration}, "
                      f"已运行 {elapsed/60:.1f}分钟, "
                      f"发布: {event_count['published']}, "
                      f"处理: {event_count['processed']}, "
                      f"丢包: {stats.get('events_dropped', 0)}")

        await asyncio.sleep(2)
        await bus.stop()

        final_stats = bus.get_stats()
        history_size = len(bus._event_history)
        
        print(f"[EventBus] 完成:")
        print(f"  - 总发布: {event_count['published']}")
        print(f"  - 总处理: {event_count['processed']}")
        print(f"  - 丢包: {final_stats.get('events_dropped', 0)}")
        print(f"  - 历史大小: {history_size}")
        print(f"  - 回调错误: {callback_errors['count']}")

        # 检测问题
        if history_size > 1100:
            self.bugs_found.append(LongTermBug(
                bug_id="LONGTERM_EB_001",
                severity="HIGH",
                component="EnhancedEventBus",
                description=f"历史记录超过限制: {history_size} > 1000",
                timestamp=datetime.now().isoformat(),
                metrics={"history_size": history_size, "limit": 1000},
                fix_suggestion="检查deque maxlen配置"
            ))

        if event_count["published"] != event_count["processed"] + final_stats.get('events_dropped', 0):
            self.bugs_found.append(LongTermBug(
                bug_id="LONGTERM_EB_002",
                severity="MEDIUM",
                component="EnhancedEventBus",
                description="事件计数不一致",
                timestamp=datetime.now().isoformat(),
                metrics={
                    "published": event_count["published"],
                    "processed": event_count["processed"],
                    "dropped": final_stats.get('events_dropped', 0)
                },
                fix_suggestion="检查事件处理逻辑"
            ))

    async def _continuous_circuit_breaker_test(self):
        """持续熔断器测试"""
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker,
            EnhancedCircuitBreakerConfig
        )

        print(f"[CircuitBreaker] 开始持续测试...")
        
        # 创建多个熔断器
        breakers = {
            f"stable_{i}": EnhancedCircuitBreaker(
                f"stable_{i}",
                EnhancedCircuitBreakerConfig(failure_threshold=5, timeout_duration=30)
            )
            for i in range(10)
        }
        
        flaky_breakers = {
            f"flaky_{i}": EnhancedCircuitBreaker(
                f"flaky_{i}",
                EnhancedCircuitBreakerConfig(failure_threshold=3, timeout_duration=5)
            )
            for i in range(5)
        }

        call_stats = {"success": 0, "failed": 0, "rejected": 0}
        start = time.time()
        iteration = 0

        while time.time() - start < self.test_duration:
            iteration += 1
            
            # 稳定熔断器 - 大部分成功
            for name, breaker in breakers.items():
                try:
                    if random.random() < 0.95:
                        await breaker.call(lambda: "success")
                        call_stats["success"] += 1
                    else:
                        await breaker.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
                        call_stats["failed"] += 1
                except:
                    call_stats["failed"] += 1

            # 不稳定熔断器 - 频繁失败
            for name, breaker in flaky_breakers.items():
                try:
                    if random.random() < 0.4:
                        await breaker.call(lambda: "success")
                        call_stats["success"] += 1
                    else:
                        await breaker.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
                        call_stats["failed"] += 1
                except:
                    call_stats["failed"] += 1

            await asyncio.sleep(random.uniform(0.01, 0.05))

            # 定期报告
            if iteration % 500 == 0:
                elapsed = time.time() - start
                open_count = sum(1 for b in list(breakers.values()) + list(flaky_breakers.values()) 
                               if b.state.value == "open")
                print(f"[CircuitBreaker] 迭代 {iteration}, "
                      f"已运行 {elapsed/60:.1f}分钟, "
                      f"成功: {call_stats['success']}, "
                      f"失败: {call_stats['failed']}, "
                      f"打开: {open_count}/15")

        print(f"[CircuitBreaker] 完成:")
        print(f"  - 成功调用: {call_stats['success']}")
        print(f"  - 失败调用: {call_stats['failed']}")
        print(f"  - 总调用: {call_stats['success'] + call_stats['failed']}")

    async def _continuous_queue_test(self):
        """持续队列测试"""
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

        print(f"[Queue] 开始持续测试...")
        
        queue = EnhancedAdaptiveQueue()
        
        stats = {"enqueued": 0, "dequeued": 0, "rejected": 0}
        start = time.time()
        iteration = 0

        async def producer():
            nonlocal iteration
            while time.time() - start < self.test_duration:
                iteration += 1
                
                # 批量入队
                batch = random.randint(5, 30)
                for i in range(batch):
                    priority = random.randint(1, 10)
                    result = await queue.enqueue(f"item_{iteration}_{i}", priority=priority)
                    if result:
                        stats["enqueued"] += 1
                    else:
                        stats["rejected"] += 1
                
                await asyncio.sleep(random.uniform(0.01, 0.1))

        async def consumer():
            while time.time() - start < self.test_duration:
                item = await queue.dequeue(wait_timeout=1.0)
                if item:
                    stats["dequeued"] += 1
                await asyncio.sleep(random.uniform(0.005, 0.05))

        # 多个生产者消费者
        producers = [asyncio.create_task(producer()) for _ in range(3)]
        consumers = [asyncio.create_task(consumer()) for _ in range(5)]

        await asyncio.gather(*producers, *consumers)

        queue_stats = queue.get_stats()
        print(f"[Queue] 完成:")
        print(f"  - 入队: {stats['enqueued']}")
        print(f"  - 出队: {stats['dequeued']}")
        print(f"  - 拒绝: {stats['rejected']}")
        print(f"  - 剩余: {queue.size()}")
        print(f"  - 容量: {queue_stats.get('capacity', 'N/A')}")

        # 检测问题
        if stats["rejected"] > stats["enqueued"] * 0.2:
            self.bugs_found.append(LongTermBug(
                bug_id="LONGTERM_Q_001",
                severity="MEDIUM",
                component="EnhancedAdaptiveQueue",
                description=f"拒绝率过高: {stats['rejected']}/{stats['enqueued']}",
                timestamp=datetime.now().isoformat(),
                metrics=stats,
                fix_suggestion="增加max_capacity或调整growth_cooldown"
            ))

    async def _resource_monitor_test(self):
        """资源监控测试"""
        print(f"[ResourceMonitor] 开始监控...")
        
        process = psutil.Process(os.getpid())
        start = time.time()
        
        cpu_samples = []
        memory_samples = []
        
        while time.time() - start < self.test_duration:
            try:
                cpu_percent = process.cpu_percent()
                memory_mb = process.memory_info().rss / 1024 / 1024
                
                cpu_samples.append(cpu_percent)
                memory_samples.append(memory_mb)
                
                if len(cpu_samples) > 100:
                    cpu_samples = cpu_samples[-100:]
                if len(memory_samples) > 100:
                    memory_samples = memory_samples[-100:]
                
                # 定期报告
                elapsed = time.time() - start
                if len(cpu_samples) % 50 == 0:
                    avg_cpu = sum(cpu_samples[-50:]) / min(len(cpu_samples), 50)
                    avg_mem = sum(memory_samples[-50:]) / min(len(memory_samples), 50)
                    print(f"[ResourceMonitor] 已运行 {elapsed/60:.1f}分钟, "
                          f"CPU: {avg_cpu:.1f}%, "
                          f"内存: {avg_mem:.1f}MB")
                
                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                print(f"[ResourceMonitor] 错误: {e}")
                await asyncio.sleep(self.check_interval)

        # 资源泄漏检测
        if len(memory_samples) > 10:
            first_10_avg = sum(memory_samples[:10]) / 10
            last_10_avg = sum(memory_samples[-10:]) / 10
            memory_growth = last_10_avg - first_10_avg
            
            if memory_growth > 100:  # 增长超过100MB
                self.bugs_found.append(LongTermBug(
                    bug_id="LONGTERM_MEM_001",
                    severity="HIGH",
                    component="System",
                    description=f"疑似内存泄漏: 增长 {memory_growth:.1f}MB",
                    timestamp=datetime.now().isoformat(),
                    metrics={
                        "first_avg": first_10_avg,
                        "last_avg": last_10_avg,
                        "growth": memory_growth
                    },
                    fix_suggestion="检查对象引用和缓存清理"
                ))
        
        print(f"[ResourceMonitor] 完成:")
        print(f"  - 平均CPU: {sum(cpu_samples)/len(cpu_samples):.1f}%")
        print(f"  - 峰值内存: {max(memory_samples):.1f}MB")
        print(f"  - 平均内存: {sum(memory_samples)/len(memory_samples):.1f}MB")

    async def _fault_injection_test(self):
        """故障注入测试"""
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker,
            EnhancedCircuitBreakerConfig
        )
        from app.core.enhanced_event_bus import EnhancedEventBus

        print(f"[FaultInjection] 开始故障注入测试...")
        
        breaker = EnhancedCircuitBreaker(
            "fault_test",
            EnhancedCircuitBreakerConfig(failure_threshold=5, timeout_duration=10)
        )
        
        bus = EnhancedEventBus()
        await bus.start()
        
        recovery_stats = {"success": 0, "failure": 0}
        start = time.time()
        test_round = 0

        while time.time() - start < self.test_duration:
            test_round += 1
            
            # 故障注入 - 连续失败打开熔断器
            print(f"[FaultInjection] 测试轮次 {test_round}, 注入故障...")
            
            for _ in range(10):
                try:
                    await breaker.call(lambda: (_ for _ in ()).throw(ValueError("injected fault")))
                except:
                    pass
            
            await asyncio.sleep(1)
            
            # 验证熔断器已打开
            if breaker.state.value == "open":
                print(f"[FaultInjection] 熔断器已打开，等待恢复...")
                
                # 等待恢复
                await asyncio.sleep(15)
                
                # 尝试恢复
                try:
                    await breaker.call(lambda: "recovery")
                    recovery_stats["success"] += 1
                    print(f"[FaultInjection] 恢复成功!")
                except:
                    recovery_stats["failure"] += 1
                    print(f"[FaultInjection] 恢复失败!")
            
            # 每10分钟进行一次完整测试
            if test_round % 60 == 0:
                print(f"[FaultInjection] 完成 {test_round} 轮测试")
            
            await asyncio.sleep(random.uniform(10, 30))

        await bus.stop()
        
        print(f"[FaultInjection] 完成:")
        print(f"  - 成功恢复: {recovery_stats['success']}")
        print(f"  - 恢复失败: {recovery_stats['failure']}")
        
        if recovery_stats['failure'] > recovery_stats['success'] * 0.2:
            self.bugs_found.append(LongTermBug(
                bug_id="LONGTERM_FAULT_001",
                severity="MEDIUM",
                component="CircuitBreaker",
                description=f"故障恢复失败率过高: {recovery_stats['failure']}/{recovery_stats['success']+recovery_stats['failure']}",
                timestamp=datetime.now().isoformat(),
                metrics=recovery_stats,
                fix_suggestion="检查熔断器timeout_duration配置"
            ))

    async def _memory_leak_detection(self):
        """内存泄漏检测"""
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_timeout_protection import EnhancedTimeoutMonitor

        print(f"[MemoryLeakDetection] 开始内存泄漏检测...")
        
        bus = EnhancedEventBus(max_history_size=1000)
        monitor = EnhancedTimeoutMonitor()
        await bus.start()
        
        initial_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        
        memory_snapshots = []
        start = time.time()
        
        while time.time() - start < self.test_duration:
            # 持续产生事件
            for i in range(100):
                await bus.publish({"event_id": i, "data": "x" * 1000})
            
            # 记录内存快照
            current_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            memory_snapshots.append({
                "time": time.time() - start,
                "memory": current_memory,
                "history_size": len(bus._event_history)
            })
            
            await asyncio.sleep(60)  # 每分钟检查一次
            
            elapsed = time.time() - start
            print(f"[MemoryLeakDetection] 已运行 {elapsed/60:.1f}分钟, "
                  f"内存: {current_memory:.1f}MB, "
                  f"历史: {len(bus._event_history)}")

        await bus.stop()
        
        # 分析内存趋势
        if len(memory_snapshots) > 10:
            first_avg = sum(s["memory"] for s in memory_snapshots[:5]) / 5
            last_avg = sum(s["memory"] for s in memory_snapshots[-5:]) / 5
            growth = last_avg - first_avg
            
            print(f"[MemoryLeakDetection] 完成:")
            print(f"  - 初始内存: {initial_memory:.1f}MB")
            print(f"  - 峰值内存: {max(s['memory'] for s in memory_snapshots):.1f}MB")
            print(f"  - 最终内存: {last_avg:.1f}MB")
            print(f"  - 内存增长: {growth:.1f}MB")
            
            # 检测持续增长
            if growth > 50:  # 增长超过50MB
                self.bugs_found.append(LongTermBug(
                    bug_id="LONGTERM_LEAK_001",
                    severity="HIGH",
                    component="EnhancedEventBus",
                    description=f"内存持续增长: {growth:.1f}MB",
                    timestamp=datetime.now().isoformat(),
                    metrics={
                        "first_avg": first_avg,
                        "last_avg": last_avg,
                        "growth": growth,
                        "snapshots": len(memory_snapshots)
                    },
                    fix_suggestion="检查事件历史和对象引用"
                ))

    async def _concurrency_stress_test(self):
        """并发压力测试"""
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker,
            EnhancedCircuitBreakerConfig
        )

        print(f"[ConcurrencyStress] 开始并发压力测试...")
        
        queue = EnhancedAdaptiveQueue()
        breakers = [
            EnhancedCircuitBreaker(f"conc_{i}", EnhancedCircuitBreakerConfig())
            for i in range(20)
        ]
        
        errors = []
        lock_errors = {"count": 0}
        start = time.time()
        operation_count = 0

        async def worker(worker_id: int):
            nonlocal operation_count
            while time.time() - start < self.test_duration:
                try:
                    # 队列操作
                    if random.random() < 0.5:
                        result = await queue.enqueue(f"w{worker_id}_{operation_count}", priority=random.randint(1, 10))
                    else:
                        item = await queue.dequeue(wait_timeout=0.1)
                    
                    # 熔断器操作
                    breaker = random.choice(breakers)
                    if random.random() < 0.9:
                        await breaker.call(lambda: "success")
                    else:
                        try:
                            await breaker.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
                        except:
                            pass
                    
                    operation_count += 1
                    await asyncio.sleep(random.uniform(0.001, 0.01))
                    
                except Exception as e:
                    errors.append(str(e))
                    if "lock" in str(e).lower():
                        lock_errors["count"] += 1

        # 启动50个并发worker
        workers = [asyncio.create_task(worker(i)) for i in range(50)]
        
        # 定期报告
        while time.time() - start < self.test_duration:
            await asyncio.sleep(300)  # 每5分钟报告一次
            elapsed = time.time() - start
            print(f"[ConcurrencyStress] 已运行 {elapsed/60:.1f}分钟, "
                  f"操作: {operation_count}, "
                  f"错误: {len(errors)}, "
                  f"TPS: {operation_count/elapsed:.1f}")

        await asyncio.gather(*workers, return_exceptions=True)
        
        print(f"[ConcurrencyStress] 完成:")
        print(f"  - 总操作: {operation_count}")
        print(f"  - 总错误: {len(errors)}")
        print(f"  - Lock错误: {lock_errors['count']}")
        print(f"  - 平均TPS: {operation_count/self.test_duration:.1f}")
        
        if lock_errors["count"] > 0:
            self.bugs_found.append(LongTermBug(
                bug_id="LONGTERM_CONC_001",
                severity="HIGH",
                component="System",
                description=f"发现 {lock_errors['count']} 个Lock相关错误",
                timestamp=datetime.now().isoformat(),
                metrics={
                    "total_errors": len(errors),
                    "lock_errors": lock_errors["count"]
                },
                fix_suggestion="检查Lock使用和死锁可能性"
            ))

    def _generate_report(self, duration: float):
        """生成测试报告"""
        print("\n" + "="*80)
        print("🚀 1小时深度压力测试报告")
        print("="*80)
        
        print(f"\n⏱️  测试时长: {duration:.2f}秒 ({duration/60:.1f}分钟)")
        print(f"📅 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.bugs_found:
            print(f"\n⚠️  发现 {len(self.bugs_found)} 个问题:")
            for i, bug in enumerate(self.bugs_found, 1):
                print(f"\n  [{bug.severity}] {bug.bug_id}")
                print(f"    组件: {bug.component}")
                print(f"    描述: {bug.description}")
                print(f"    时间: {bug.timestamp}")
                print(f"    指标: {bug.metrics}")
                print(f"    修复: {bug.fix_suggestion}")
        else:
            print(f"\n✅ 未发现问题 - 系统运行稳定!")
        
        print("\n" + "="*80)


async def main():
    tester = OneHourStressTest()
    await tester.run()


if __name__ == "__main__":
    print("⚠️  警告: 此测试将运行1小时!")
    print("建议使用 tmux 或 screen 运行")
    response = input("是否继续? (yes/no): ")
    
    if response.lower() == "yes":
        asyncio.run(main())
    else:
        print("测试已取消")
