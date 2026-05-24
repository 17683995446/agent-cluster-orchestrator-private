"""
🔬 第八轮深度测试 - 极端场景验证
目标: 通过模拟极端生产环境发现系统深层问题

测试场景:
1. 大规模并发冲击 (1000+ concurrent operations)
2. 网络分区模拟 (网络延迟/断开)
3. 资源耗尽测试 (内存/CPU压力)
4. 长时间稳定性 (30分钟连续运行)
5. 组件级故障注入
6. 数据一致性验证

遵循第一性原理: 从最基本的系统行为出发，验证每个组件的正确性
"""
import asyncio
import time
import random
import sys
import gc
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

sys.path.insert(0, '/workspace/backend')


@dataclass
class TestScenario:
    name: str
    duration_minutes: int
    target_qps: int
    description: str


@dataclass
class TestResult:
    scenario: str
    start_time: float
    end_time: float
    success: bool
    total_ops: int
    error_rate: float
    avg_latency_ms: float
    issues: List[str] = field(default_factory=list)


class ExtremeEnvironmentTester:
    """极端环境测试器 - 模拟真实生产环境的各种极端情况"""
    
    def __init__(self):
        self.results: List[TestResult] = []
        self.global_errors: List[str] = []
        self.stop_event = asyncio.Event()

    async def run(self):
        """运行所有极端场景测试"""
        print("\n" + "="*90)
        print("🔬 Agent Cluster Orchestrator - 第八轮极端场景测试")
        print(f"   开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("   目标: 发现系统深层问题，遵循第一性原理验证")
        print("="*90 + "\n")

        await self.scenario1_massive_concurrency()
        await self.scenario2_network_partition()
        await self.scenario3_resource_exhaustion()
        await self.scenario4_long_running_stability()
        await self.scenario5_component_failure_injection()

        self.print_summary()
        self.generate_report()

    async def scenario1_massive_concurrency(self):
        """场景1: 大规模并发冲击"""
        scenario = "大规模并发冲击"
        print(f"\n--- 🚀 场景1: {scenario} ---")
        start = time.time()
        success = True
        issues = []
        total_ops = 0
        errors = 0
        latencies = []

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            bus = EnhancedEventBus(max_history_size=5000)
            queue = EnhancedAdaptiveQueue()
            await bus.start()

            processed_events = 0
            lock = asyncio.Lock()

            async def listener(event):
                nonlocal processed_events
                async with lock:
                    processed_events += 1
                await asyncio.sleep(random.uniform(0.001, 0.005))

            await bus.subscribe(listener)

            async def producer(producer_id: int, count: int):
                nonlocal total_ops, errors
                for i in range(count):
                    try:
                        await bus.publish({
                            "producer": producer_id,
                            "seq": i,
                            "timestamp": time.time()
                        })
                        await queue.enqueue(f"task_{producer_id}_{i}", priority=random.randint(1, 10))
                        total_ops += 2
                        await asyncio.sleep(0.001)
                    except Exception as e:
                        errors += 1

            producers = [producer(i, 200) for i in range(10)]
            await asyncio.gather(*producers)

            await asyncio.sleep(5)
            await bus.stop()

            recovered_items = 0
            while queue.size() > 0:
                item = await queue.dequeue(wait_timeout=0.1)
                if item:
                    recovered_items += 1

            if processed_events != 2000:
                issues.append(f"事件丢失: 期望2000，实际{processed_events}")
                success = False

            if recovered_items != 2000:
                issues.append(f"队列数据丢失: 期望2000，实际{recovered_items}")
                success = False

            error_rate = errors / total_ops if total_ops > 0 else 0
            if error_rate > 0.01:
                issues.append(f"错误率过高: {error_rate*100:.2f}%")
                success = False

            print(f"  总操作: {total_ops}, 事件处理: {processed_events}, 队列恢复: {recovered_items}")

        except Exception as e:
            issues.append(f"场景崩溃: {e}")
            success = False
            error_rate = 0
            avg_latency_ms = 0

        self.results.append(TestResult(
            scenario=scenario,
            start_time=start,
            end_time=time.time(),
            success=success,
            total_ops=total_ops,
            error_rate=error_rate,
            avg_latency_ms=0,
            issues=issues
        ))

    async def scenario2_network_partition(self):
        """场景2: 网络分区模拟"""
        scenario = "网络分区模拟"
        print(f"\n--- 🌐 场景2: {scenario} ---")
        start = time.time()
        success = True
        issues = []
        total_ops = 0
        errors = 0

        try:
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
            )

            cb = EnhancedCircuitBreaker(
                "network_test",
                EnhancedCircuitBreakerConfig(
                    failure_threshold=3,
                    success_threshold=2,
                    timeout_duration=0.5,
                    min_call_count=3
                )
            )

            async def simulate_network_call(failure_rate: float):
                if random.random() < failure_rate:
                    await asyncio.sleep(random.uniform(0.1, 0.5))
                    raise Exception("network_timeout")
                await asyncio.sleep(random.uniform(0.01, 0.05))
                return "success"

            # 阶段1: 正常网络 (0%失败)
            print("  阶段1: 正常网络")
            for _ in range(20):
                try:
                    await cb.call(lambda: simulate_network_call(0))
                    total_ops += 1
                except Exception:
                    errors += 1
                await asyncio.sleep(0.01)

            # 阶段2: 网络延迟 (50%失败)
            print("  阶段2: 网络延迟 (50%失败)")
            for _ in range(30):
                try:
                    await cb.call(lambda: simulate_network_call(0.5))
                    total_ops += 1
                except Exception:
                    errors += 1
                await asyncio.sleep(0.01)

            # 阶段3: 网络断开 (100%失败)
            print("  阶段3: 网络断开")
            for _ in range(20):
                try:
                    await cb.call(lambda: simulate_network_call(1.0))
                    total_ops += 1
                except Exception:
                    errors += 1
                await asyncio.sleep(0.01)

            if cb.state.value != "open":
                issues.append("网络断开时熔断器未打开")
                success = False

            # 阶段4: 网络恢复
            print("  阶段4: 网络恢复")
            await asyncio.sleep(0.7)
            recovered = False
            for _ in range(10):
                try:
                    await cb.call(lambda: simulate_network_call(0))
                    if cb.state.value == "closed":
                        recovered = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.1)

            if not recovered:
                issues.append("网络恢复后熔断器未关闭")
                success = False

            print(f"  总操作: {total_ops}, 错误: {errors}, 最终状态: {cb.state.value}, 恢复成功: {recovered}")

        except Exception as e:
            issues.append(f"场景崩溃: {e}")
            success = False
            error_rate = 0

        self.results.append(TestResult(
            scenario=scenario,
            start_time=start,
            end_time=time.time(),
            success=success,
            total_ops=total_ops,
            error_rate=errors/total_ops if total_ops > 0 else 0,
            avg_latency_ms=0,
            issues=issues
        ))

    async def scenario3_resource_exhaustion(self):
        """场景3: 资源耗尽测试"""
        scenario = "资源耗尽测试"
        print(f"\n--- 💥 场景3: {scenario} ---")
        start = time.time()
        success = True
        issues = []
        total_ops = 0
        errors = 0

        try:
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            queue = EnhancedAdaptiveQueue()

            # 大量入队
            print("  阶段1: 大量入队")
            for i in range(5000):
                await queue.enqueue(f"item_{i}", priority=1)
                total_ops += 1

            if queue.size() != 5000:
                issues.append(f"队列容量不足: 期望5000，实际{queue.size()}")
                success = False

            # 并发消费
            print("  阶段2: 并发消费")
            consumed = 0
            lock = asyncio.Lock()

            async def consumer():
                nonlocal consumed, errors
                for _ in range(1000):
                    try:
                        item = await queue.dequeue(wait_timeout=0.5)
                        if item:
                            async with lock:
                                consumed += 1
                    except Exception:
                        errors += 1

            consumers = [consumer() for _ in range(5)]
            await asyncio.gather(*consumers)

            if consumed != 5000:
                issues.append(f"消费不完整: 期望5000，实际{consumed}")
                success = False

            print(f"  入队: {total_ops}, 消费: {consumed}, 错误: {errors}")

        except Exception as e:
            issues.append(f"场景崩溃: {e}")
            success = False

        self.results.append(TestResult(
            scenario=scenario,
            start_time=start,
            end_time=time.time(),
            success=success,
            total_ops=total_ops,
            error_rate=errors/total_ops if total_ops > 0 else 0,
            avg_latency_ms=0,
            issues=issues
        ))

    async def scenario4_long_running_stability(self):
        """场景4: 长时间稳定性测试"""
        scenario = "长时间稳定性测试"
        print(f"\n--- ⏱️ 场景4: {scenario} ---")
        start = time.time()
        success = True
        issues = []
        total_ops = 0
        errors = 0

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_timeout_protection import (
                EnhancedTimeoutMonitor, enhanced_execute_with_timeout
            )

            bus = EnhancedEventBus(max_history_size=1000)
            monitor = EnhancedTimeoutMonitor()
            await bus.start()

            processed = 0
            async def handler(event):
                nonlocal processed
                processed += 1
                await asyncio.sleep(0.001)

            await bus.subscribe(handler)

            # 持续运行1分钟
            print("  持续运行...")
            end_time = time.time() + 60
            while time.time() < end_time:
                try:
                    await bus.publish({"time": time.time()})
                    total_ops += 1
                    if random.random() < 0.1:
                        await enhanced_execute_with_timeout(
                            asyncio.sleep(random.uniform(0.01, 0.1)),
                            timeout=0.5,
                            monitor=monitor
                        )
                except Exception:
                    errors += 1
                await asyncio.sleep(0.001)

            await bus.stop()

            stats = await monitor.get_stats()
            success_rate = stats.get('success_rate', 0)

            if success_rate < 99:
                issues.append(f"成功率低于99%: {success_rate}%")
                success = False

            print(f"  总操作: {total_ops}, 处理: {processed}, 成功率: {success_rate}%")

        except Exception as e:
            issues.append(f"场景崩溃: {e}")
            success = False

        self.results.append(TestResult(
            scenario=scenario,
            start_time=start,
            end_time=time.time(),
            success=success,
            total_ops=total_ops,
            error_rate=errors/total_ops if total_ops > 0 else 0,
            avg_latency_ms=0,
            issues=issues
        ))

    async def scenario5_component_failure_injection(self):
        """场景5: 组件级故障注入"""
        scenario = "组件级故障注入"
        print(f"\n--- 🔌 场景5: {scenario} ---")
        start = time.time()
        success = True
        issues = []
        total_ops = 0
        errors = 0

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
            )

            bus = EnhancedEventBus()
            cb = EnhancedCircuitBreaker(
                "failure_test",
                EnhancedCircuitBreakerConfig(
                    failure_threshold=2,
                    success_threshold=2,
                    timeout_duration=0.3,
                    min_call_count=2
                )
            )

            await bus.start()

            # 注入故障
            failures = 0
            async def unreliable_operation():
                nonlocal failures
                if failures < 5:
                    failures += 1
                    raise Exception("component_failure")
                return "ok"

            # 测试故障恢复
            recovery_count = 0
            for i in range(15):
                try:
                    await cb.call(unreliable_operation)
                    recovery_count += 1
                except Exception:
                    errors += 1
                total_ops += 1
                await asyncio.sleep(0.1)

            if recovery_count < 8:
                issues.append(f"故障恢复不足: 期望>8，实际{recovery_count}")
                success = False

            await bus.stop()

            print(f"  操作: {total_ops}, 错误: {errors}, 恢复成功: {recovery_count}")

        except Exception as e:
            issues.append(f"场景崩溃: {e}")
            success = False

        self.results.append(TestResult(
            scenario=scenario,
            start_time=start,
            end_time=time.time(),
            success=success,
            total_ops=total_ops,
            error_rate=errors/total_ops if total_ops > 0 else 0,
            avg_latency_ms=0,
            issues=issues
        ))

    def print_summary(self):
        """打印测试摘要"""
        print("\n" + "="*90)
        print("📊 第八轮极端场景测试 - 摘要")
        print("="*90)

        total = len(self.results)
        passed = sum(1 for r in self.results if r.success)

        print(f"\n  测试场景: {total}")
        print(f"  通过: {passed}")
        print(f"  失败: {total - passed}")

        all_issues = []
        for r in self.results:
            all_issues.extend(r.issues)

        if all_issues:
            print(f"\n⚠️ 发现问题 ({len(all_issues)}):")
            for i, issue in enumerate(all_issues, 1):
                print(f"  {i}. {issue}")
        else:
            print("\n✅ 未发现问题！")

        print("\n" + "="*90)

    def generate_report(self):
        """生成完整报告"""
        report_path = "/workspace/backend/EIGHTH_ROUND_REPORT.md"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        total = len(self.results)
        passed = sum(1 for r in self.results if r.success)

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("# Agent Cluster Orchestrator - 第八轮极端场景测试报告\n\n")
            f.write(f"**日期**: {now}\n")
            f.write(f"**测试目标**: 极端场景验证，发现系统深层问题\n")
            f.write(f"**遵循原则**: 第一性原理\n\n")
            f.write("---\n\n")

            f.write("## 测试摘要\n\n")
            f.write(f"- **测试场景**: {total}\n")
            f.write(f"- **通过**: {passed}\n")
            f.write(f"- **失败**: {total - passed}\n\n")

            f.write("## 详细测试结果\n\n")
            for i, result in enumerate(self.results, 1):
                status_emoji = "✅" if result.success else "❌"
                duration = timedelta(seconds=result.end_time - result.start_time)

                f.write(f"### {i}. {result.scenario}\n\n")
                f.write(f"- **状态**: {status_emoji} {'通过' if result.success else '失败'}\n")
                f.write(f"- **耗时**: {duration}\n")
                f.write(f"- **总操作**: {result.total_ops}\n")
                f.write(f"- **错误率**: {result.error_rate*100:.2f}%\n")

                if result.issues:
                    f.write(f"- **问题**:\n")
                    for issue in result.issues:
                        f.write(f"  - ⚠️ {issue}\n")

                f.write("\n")

            all_issues = []
            for r in self.results:
                all_issues.extend(r.issues)

            f.write("## 问题分析与改进建议\n\n")
            if all_issues:
                f.write("### 发现的问题\n")
                for i, issue in enumerate(all_issues, 1):
                    f.write(f"{i}. {issue}\n")
                
                f.write("\n### 改进建议\n")
                f.write("- 根据发现的问题进行针对性修复\n")
                f.write("- 加强边界条件处理\n")
                f.write("- 优化资源管理策略\n")
            else:
                f.write("✅ 系统在极端场景下表现稳定，未发现问题。\n")
                f.write("建议继续进行第九轮测试，覆盖更多边界场景。\n")

            f.write("\n---\n")
            f.write("*第八轮极端场景测试完成*\n")

        print(f"\n📄 报告已保存至: {report_path}")


async def main():
    tester = ExtremeEnvironmentTester()
    await tester.run()


if __name__ == "__main__":
    asyncio.run(main())
