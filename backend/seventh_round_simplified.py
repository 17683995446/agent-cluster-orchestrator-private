"""
🚀 第七轮深度测试 - 简化版（无额外依赖）
目标: 发现系统在真实运行场景中的问题
"""
import asyncio
import time
import random
import sys
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, '/workspace/backend')


@dataclass
class TestResult:
    """测试结果数据"""
    test_name: str
    start_time: float
    end_time: float
    success: bool
    metrics: Dict[str, Any] = field(default_factory=dict)
    issues_found: List[str] = field(default_factory=list)


class SimplifiedProductionTester:
    """简化的生产环境测试器"""

    def __init__(self):
        self.results: List[TestResult] = []
        self.issues_found: List[str] = []

    async def run(self):
        """运行所有测试"""
        print("\n" + "=" * 80)
        print("🚀 第七轮深度测试 - 真实运行场景")
        print(f"   开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80 + "\n")

        await self.test_event_bus_extreme_load()
        await self.test_circuit_breaker_state_stability()
        await self.test_queue_starvation()
        await self.test_timeout_propagation()
        await self.test_fault_recovery()
        await self.test_deadlock_scenarios()

        self.print_summary()

    async def test_event_bus_extreme_load(self):
        """测试1: EventBus极端负载"""
        test_name = "event_bus_extreme_load"
        print(f"\n--- 测试1: {test_name} ---")
        start = time.time()
        success = True
        metrics = {}

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus

            bus = EnhancedEventBus(max_history_size=1000)
            await bus.start()

            processed_count = 0
            callback_errors = 0

            async def callback(event):
                nonlocal processed_count, callback_errors
                try:
                    if random.random() < 0.01:
                        raise Exception("random callback error")
                    await asyncio.sleep(random.uniform(0.001, 0.01))
                    processed_count += 1
                except Exception:
                    callback_errors += 1

            await bus.subscribe(callback)

            # 阶段1: 小批量稳定发送
            print("阶段1: 稳定小批量")
            for i in range(30):
                for j in range(20):
                    await bus.publish({"id": f"{i}-{j}"})
                await asyncio.sleep(0.1)

            # 阶段2: 突发大流量
            print("阶段2: 突发大流量")
            burst_count = 0
            for i in range(20):
                burst_size = random.randint(50, 200)
                for j in range(burst_size):
                    await bus.publish({"id": f"burst-{i}-{j}"})
                burst_count += burst_size
                await asyncio.sleep(0.2)

            # 阶段3: 混合优先级大事件
            print("阶段3: 混合负载")
            for i in range(50):
                for j in range(30):
                    await bus.publish({
                        "id": f"mixed-{i}-{j}",
                        "data": "x" * 100  # 较大数据
                    })
                await asyncio.sleep(0.05)

            await asyncio.sleep(3)
            await bus.stop()

            stats = bus.get_stats()
            metrics = {
                "published": stats.get('events_published', 0),
                "processed": processed_count,
                "callback_errors": stats.get('callback_errors', 0),
                "dropped": stats.get('events_dropped', 0),
                "history_size": len(bus._event_history)
            }

            if len(bus._event_history) > 1000:
                self.issues_found.append(f"历史记录超限: {len(bus._event_history)} > 1000")
                success = False

            if metrics["dropped"] > metrics["published"] * 0.1:
                self.issues_found.append(f"掉包率过高: {metrics['dropped']/metrics['published']*100:.1f}%")
                success = False

        except Exception as e:
            self.issues_found.append(f"{test_name}崩溃: {e}")
            success = False

        self.results.append(TestResult(
            test_name=test_name,
            start_time=start,
            end_time=time.time(),
            success=success,
            metrics=metrics
        ))

        print(f"  {test_name}: {'✅ 成功' if success else '❌ 失败'}")

    async def test_circuit_breaker_state_stability(self):
        """测试2: 熔断器状态稳定性"""
        test_name = "circuit_breaker_state_stability"
        print(f"\n--- 测试2: {test_name} ---")
        start = time.time()
        success = True
        metrics = {}

        try:
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )

            cb = EnhancedCircuitBreaker(
                "stability_test",
                EnhancedCircuitBreakerConfig(
                    failure_threshold=5,
                    success_threshold=3,
                    timeout_duration=1.0
                )
            )

            state_changes = []
            open_count = 0

            async def track_changes(old, new):
                state_changes.append((time.time(), old, new))
                if new.value == "open":
                    nonlocal open_count
                    open_count += 1

            cb.on_state_change = track_changes

            total_calls = 0
            errors = 0

            async def call_with_errors(rate):
                nonlocal total_calls, errors
                for _ in range(50):
                    try:
                        if random.random() < rate:
                            await cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
                        else:
                            await cb.call(lambda: asyncio.sleep(0.001))
                        total_calls += 1
                    except Exception:
                        errors += 1
                    await asyncio.sleep(0.005)

            # 错误率变化场景
            print("阶段1: 0%错误")
            await call_with_errors(0)
            await asyncio.sleep(0.5)

            print("阶段2: 30%错误")
            await call_with_errors(0.3)
            await asyncio.sleep(0.5)

            print("阶段3: 100%错误 -> 打开")
            await call_with_errors(1.0)
            await asyncio.sleep(2.0)

            print("阶段4: 0%错误 -> 恢复")
            await call_with_errors(0)
            await asyncio.sleep(0.5)

            metrics = {
                "total_calls": total_calls,
                "errors": errors,
                "state_changes": len(state_changes),
                "open_count": open_count,
                "final_state": cb.state.value
            }

            if metrics["final_state"] == "open" and errors < total_calls * 0.1:
                self.issues_found.append(f"熔断器错误恢复失败")
                success = False

        except Exception as e:
            self.issues_found.append(f"{test_name}崩溃: {e}")
            success = False

        self.results.append(TestResult(
            test_name=test_name,
            start_time=start,
            end_time=time.time(),
            success=success,
            metrics=metrics
        ))

        print(f"  {test_name}: {'✅ 成功' if success else '❌ 失败'}")

    async def test_queue_starvation(self):
        """测试3: 队列饥饿问题"""
        test_name = "queue_starvation"
        print(f"\n--- 测试3: {test_name} ---")
        start = time.time()
        success = True
        metrics = {}

        try:
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            queue = EnhancedAdaptiveQueue()

            high_processed = 0
            medium_processed = 0
            low_processed = 0

            async def high_producer():
                for i in range(100):
                    await queue.enqueue(f"high-{i}", priority=10)

            async def medium_producer():
                for i in range(100):
                    await queue.enqueue(f"medium-{i}", priority=5)

            async def low_producer():
                for i in range(100):
                    await queue.enqueue(f"low-{i}", priority=1)

            async def consumer():
                nonlocal high_processed, medium_processed, low_processed
                for _ in range(300):
                    item = await queue.dequeue(wait_timeout=0.5)
                    if item:
                        if item.startswith("high"):
                            high_processed += 1
                        elif item.startswith("medium"):
                            medium_processed += 1
                        elif item.startswith("low"):
                            low_processed += 1
                    await asyncio.sleep(0.005)

            await asyncio.gather(
                high_producer(),
                medium_producer(),
                low_producer(),
                consumer()
            )

            metrics = {
                "high_processed": high_processed,
                "medium_processed": medium_processed,
                "low_processed": low_processed,
                "remaining": queue.size()
            }

            if low_processed == 0 and high_processed > 0:
                self.issues_found.append(f"低优先级任务完全饥饿！")
                success = False
            elif low_processed < 30 and high_processed > 90:
                self.issues_found.append(f"低优先级处理过低: {low_processed}/100")
                success = False

        except Exception as e:
            self.issues_found.append(f"{test_name}崩溃: {e}")
            success = False

        self.results.append(TestResult(
            test_name=test_name,
            start_time=start,
            end_time=time.time(),
            success=success,
            metrics=metrics
        ))

        print(f"  {test_name}: {'✅ 成功' if success else '❌ 失败'}")

    async def test_timeout_propagation(self):
        """测试4: 超时传播和嵌套超时"""
        test_name = "timeout_propagation"
        print(f"\n--- 测试4: {test_name} ---")
        start = time.time()
        success = True
        metrics = {}

        try:
            from app.core.enhanced_timeout_protection import (
                EnhancedTimeoutMonitor,
                enhanced_execute_with_timeout
            )

            monitor = EnhancedTimeoutMonitor()

            timeout_count = 0
            completed_count = 0

            async def deep_call(depth: int):
                if depth > 0:
                    try:
                        await enhanced_execute_with_timeout(
                            deep_call(depth - 1),
                            timeout=0.5,
                            monitor=monitor
                        )
                    except asyncio.TimeoutError:
                        raise
                    await asyncio.sleep(0.01)
                else:
                    await asyncio.sleep(1.0)  # 故意超时

            for i in range(20):
                try:
                    await enhanced_execute_with_timeout(
                        deep_call(3),
                        timeout=2.0,
                        monitor=monitor
                    )
                    completed_count += 1
                except asyncio.TimeoutError:
                    timeout_count += 1

            stats = await monitor.get_stats()

            metrics = {
                "timeouts": timeout_count,
                "completed": completed_count,
                "monitor_stats": stats
            }

            if timeout_count < 10:
                self.issues_found.append(f"超时触发过少: {timeout_count}/20")
                success = False

        except Exception as e:
            self.issues_found.append(f"{test_name}崩溃: {e}")
            success = False

        self.results.append(TestResult(
            test_name=test_name,
            start_time=start,
            end_time=time.time(),
            success=success,
            metrics=metrics
        ))

        print(f"  {test_name}: {'✅ 成功' if success else '❌ 失败'}")

    async def test_fault_recovery(self):
        """测试5: 故障后恢复"""
        test_name = "fault_recovery"
        print(f"\n--- 测试5: {test_name} ---")
        start = time.time()
        success = True
        metrics = {}

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )

            results = []

            cb = EnhancedCircuitBreaker(
                "recovery_test",
                EnhancedCircuitBreakerConfig(
                    failure_threshold=3,
                    success_threshold=2,
                    timeout_duration=1.5
                )
            )

            # 1. 触发熔断
            print("步骤1: 触发熔断")
            for _ in range(10):
                try:
                    await cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
                except Exception:
                    pass

            # 2. 等待恢复
            print("步骤2: 等待超时")
            await asyncio.sleep(2.0)

            # 3. 测试恢复
            print("步骤3: 测试恢复")
            recovered = False
            for attempt in range(20):
                try:
                    await cb.call(lambda: asyncio.sleep(0.01))
                    if cb.state.value == "closed":
                        recovered = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.1)

            metrics = {
                "recovered": recovered,
                "final_state": cb.state.value
            }

            if not recovered:
                self.issues_found.append("熔断器无法从打开状态恢复")
                success = False

        except Exception as e:
            self.issues_found.append(f"{test_name}崩溃: {e}")
            success = False

        self.results.append(TestResult(
            test_name=test_name,
            start_time=start,
            end_time=time.time(),
            success=success,
            metrics=metrics
        ))

        print(f"  {test_name}: {'✅ 成功' if success else '❌ 失败'}")

    async def test_deadlock_scenarios(self):
        """测试6: 潜在死锁场景"""
        test_name = "deadlock_scenarios"
        print(f"\n--- 测试6: {test_name} ---")
        start = time.time()
        success = True
        metrics = {}

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            completed = 0
            errors = 0

            async def mixed_operation(idx: int):
                nonlocal completed, errors
                try:
                    # 混合使用所有组件
                    bus = EnhancedEventBus()
                    await bus.start()
                    queue = EnhancedAdaptiveQueue()
                    cb = EnhancedCircuitBreaker(f"deadlock_test_{idx}", EnhancedCircuitBreakerConfig())

                    await queue.enqueue(f"msg-{idx}")
                    item = await queue.dequeue(wait_timeout=0.1)

                    await bus.publish({"item": item})
                    await bus.stop()
                    completed += 1
                except Exception:
                    errors += 1

            tasks = [asyncio.create_task(mixed_operation(i)) for i in range(20)]

            # 使用超时检测潜在死锁
            done, pending = await asyncio.wait(tasks, timeout=30.0)

            if pending:
                for p in pending:
                    p.cancel()
                self.issues_found.append(f"检测到潜在死锁: {len(pending)}个任务未完成")
                success = False

            metrics = {
                "completed": completed,
                "errors": errors,
                "pending_cancelled": len(pending)
            }

        except Exception as e:
            self.issues_found.append(f"{test_name}崩溃: {e}")
            success = False

        self.results.append(TestResult(
            test_name=test_name,
            start_time=start,
            end_time=time.time(),
            success=success,
            metrics=metrics
        ))

        print(f"  {test_name}: {'✅ 成功' if success else '❌ 失败'}")

    def print_summary(self):
        """打印总结"""
        print("\n" + "=" * 80)
        print("📊 第七轮深度测试总结")
        print("=" * 80)

        total = len(self.results)
        passed = sum(1 for r in self.results if r.success)

        print(f"\n总测试: {total}, 通过: {passed}, 失败: {total - passed}")

        if self.issues_found:
            print(f"\n⚠️ 发现问题 ({len(self.issues_found)}):")
            for i, issue in enumerate(self.issues_found, 1):
                print(f"  {i}. {issue}")
        else:
            print(f"\n✅ 未发现问题！")

        print(f"\n详细结果:")
        for result in self.results:
            status = "✅" if result.success else "❌"
            duration = result.end_time - result.start_time
            print(f"  {status} {result.test_name} ({duration:.1f}秒)")
            if result.metrics:
                print(f"    指标: {result.metrics}")

        print("\n" + "=" * 80)


async def main():
    tester = SimplifiedProductionTester()
    await tester.run()


if __name__ == "__main__":
    asyncio.run(main())
