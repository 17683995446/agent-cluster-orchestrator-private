"""
🔬 第八轮深度测试 - 精简版
目标: 通过模拟极端生产环境发现系统深层问题
"""
import asyncio
import time
import random
import sys
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime

sys.path.insert(0, '/workspace/backend')


@dataclass
class TestResult:
    scenario: str
    start_time: float
    end_time: float
    success: bool
    total_ops: int
    error_rate: float
    issues: List[str] = field(default_factory=list)


class ExtremeTester:
    def __init__(self):
        self.results: List[TestResult] = []

    async def run(self):
        print("\n" + "="*80)
        print("🔬 第八轮极端场景测试")
        print(f"   开始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")

        await self.scenario1_concurrent_stress()
        await self.scenario2_network_partition()
        await self.scenario3_queue_limits()
        await self.scenario4_timeout_propagation()
        await self.scenario5_fault_recovery()

        self.print_summary()
        self.generate_report()

    async def scenario1_concurrent_stress(self):
        """场景1: 并发压力测试"""
        scenario = "并发压力测试"
        print(f"\n--- 🚀 {scenario} ---")
        start = time.time()
        success = True
        issues = []
        total_ops = 0
        errors = 0

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            bus = EnhancedEventBus(max_history_size=1000)
            queue = EnhancedAdaptiveQueue()
            await bus.start()

            processed = 0
            async def listener(event):
                nonlocal processed
                processed += 1
                await asyncio.sleep(0.001)

            await bus.subscribe(listener)

            async def producer(id):
                nonlocal total_ops, errors
                for i in range(100):
                    try:
                        await bus.publish({"id": f"{id}-{i}"})
                        await queue.enqueue(f"task-{id}-{i}", priority=5)
                        total_ops += 2
                    except Exception:
                        errors += 1

            producers = [producer(i) for i in range(5)]
            await asyncio.gather(*producers)

            await asyncio.sleep(2)
            await bus.stop()

            recovered = 0
            while queue.size() > 0:
                item = await queue.dequeue(wait_timeout=0.1)
                if item:
                    recovered += 1

            stats = queue.get_stats()
            enqueued = stats.get("enqueued", 0)
            rejected = stats.get("rejected", 0)

            if processed != 500:
                issues.append(f"事件丢失: {processed}/500")
                success = False

            if recovered != enqueued - rejected:
                issues.append(f"队列数据不一致: 入队{enqueued}, 拒绝{rejected}, 恢复{recovered}")
                success = False

            print(f"  完成: {total_ops}操作, 事件: {processed}, 队列: {recovered}")

        except Exception as e:
            issues.append(f"崩溃: {e}")
            success = False

        self.results.append(TestResult(
            scenario=scenario, start_time=start, end_time=time.time(),
            success=success, total_ops=total_ops,
            error_rate=errors/total_ops if total_ops > 0 else 0, issues=issues
        ))

    async def scenario2_network_partition(self):
        """场景2: 网络分区模拟"""
        scenario = "网络分区模拟"
        print(f"\n--- 🌐 {scenario} ---")
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
                    failure_threshold=3, success_threshold=2,
                    timeout_duration=0.3, min_call_count=3
                )
            )

            async def call(failure_rate):
                if random.random() < failure_rate:
                    await asyncio.sleep(0.1)
                    raise Exception("timeout")
                await asyncio.sleep(0.01)

            # 正常网络
            for _ in range(10):
                try:
                    await cb.call(call, 0)
                    total_ops += 1
                except Exception:
                    errors += 1

            # 网络问题 (80%失败)
            for _ in range(15):
                try:
                    await cb.call(call, 0.8)
                    total_ops += 1
                except Exception:
                    errors += 1

            if cb.state.value != "open":
                issues.append("熔断器未打开")
                success = False

            # 等待恢复
            await asyncio.sleep(0.5)

            # 恢复测试
            recovered = False
            for _ in range(10):
                try:
                    await cb.call(call, 0)
                    if cb.state.value == "closed":
                        recovered = True
                        break
                except Exception:
                    pass

            if not recovered:
                issues.append("无法恢复")
                success = False

            print(f"  操作: {total_ops}, 错误: {errors}, 状态: {cb.state.value}, 恢复: {recovered}")

        except Exception as e:
            issues.append(f"崩溃: {e}")
            success = False

        self.results.append(TestResult(
            scenario=scenario, start_time=start, end_time=time.time(),
            success=success, total_ops=total_ops,
            error_rate=errors/total_ops if total_ops > 0 else 0, issues=issues
        ))

    async def scenario3_queue_limits(self):
        """场景3: 队列容量限制测试"""
        scenario = "队列容量限制测试"
        print(f"\n--- 💥 {scenario} ---")
        start = time.time()
        success = True
        issues = []
        total_ops = 0
        errors = 0

        try:
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            queue = EnhancedAdaptiveQueue()

            # 测试容量限制
            for i in range(1200):
                try:
                    await queue.enqueue(f"item-{i}", priority=1)
                    total_ops += 1
                except Exception:
                    errors += 1

            stats = queue.get_stats()
            rejected = stats.get("rejected", 0)

            print(f"  入队: {total_ops}, 拒绝: {rejected}, 队列大小: {queue.size()}")

        except Exception as e:
            issues.append(f"崩溃: {e}")
            success = False

        self.results.append(TestResult(
            scenario=scenario, start_time=start, end_time=time.time(),
            success=success, total_ops=total_ops,
            error_rate=errors/total_ops if total_ops > 0 else 0, issues=issues
        ))

    async def scenario4_timeout_propagation(self):
        """场景4: 超时传播测试"""
        scenario = "超时传播测试"
        print(f"\n--- ⏱️ {scenario} ---")
        start = time.time()
        success = True
        issues = []
        total_ops = 0
        errors = 0

        try:
            from app.core.enhanced_timeout_protection import (
                EnhancedTimeoutMonitor, enhanced_execute_with_timeout
            )

            monitor = EnhancedTimeoutMonitor()

            async def slow_task():
                await asyncio.sleep(0.5)
                return "done"

            # 测试超时
            timeout_count = 0
            for _ in range(5):
                try:
                    await enhanced_execute_with_timeout(slow_task(), timeout=0.2, monitor=monitor)
                except Exception:
                    timeout_count += 1

            stats = await monitor.get_stats()

            if timeout_count != 5:
                issues.append(f"超时检测错误: {timeout_count}/5")
                success = False

            print(f"  超时检测: {timeout_count}/5")

        except Exception as e:
            issues.append(f"崩溃: {e}")
            success = False

        self.results.append(TestResult(
            scenario=scenario, start_time=start, end_time=time.time(),
            success=success, total_ops=5,
            error_rate=errors/5 if 5 > 0 else 0, issues=issues
        ))

    async def scenario5_fault_recovery(self):
        """场景5: 故障恢复测试"""
        scenario = "故障恢复测试"
        print(f"\n--- 🔌 {scenario} ---")
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
                "recovery_test",
                EnhancedCircuitBreakerConfig(
                    failure_threshold=2, success_threshold=2,
                    timeout_duration=0.3, min_call_count=2
                )
            )

            async def fail_func():
                raise Exception("fail")

            async def success_func():
                await asyncio.sleep(0.01)

            # 触发熔断
            for _ in range(5):
                try:
                    await cb.call(fail_func)
                except Exception:
                    errors += 1
                total_ops += 1

            if cb.state.value != "open":
                issues.append("未触发熔断")
                success = False

            # 等待恢复
            await asyncio.sleep(0.5)

            # 测试恢复
            recovered = False
            for _ in range(10):
                try:
                    await cb.call(success_func)
                    if cb.state.value == "closed":
                        recovered = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.1)

            if not recovered:
                issues.append("无法恢复")
                success = False

            print(f"  操作: {total_ops}, 错误: {errors}, 恢复: {recovered}")

        except Exception as e:
            issues.append(f"崩溃: {e}")
            success = False

        self.results.append(TestResult(
            scenario=scenario, start_time=start, end_time=time.time(),
            success=success, total_ops=total_ops,
            error_rate=errors/total_ops if total_ops > 0 else 0, issues=issues
        ))

    def print_summary(self):
        print("\n" + "="*80)
        print("📊 第八轮测试总结")
        print("="*80)

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

        print("\n" + "="*80)

    def generate_report(self):
        report_path = "/workspace/backend/EIGHTH_ROUND_REPORT.md"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("# 第八轮极端场景测试报告\n\n")
            f.write(f"**日期**: {now}\n\n")
            f.write("## 摘要\n\n")

            total = len(self.results)
            passed = sum(1 for r in self.results if r.success)
            f.write(f"- 测试场景: {total}\n")
            f.write(f"- 通过: {passed}\n")
            f.write(f"- 失败: {total - passed}\n\n")

            f.write("## 详细结果\n\n")
            for result in self.results:
                emoji = "✅" if result.success else "❌"
                f.write(f"### {emoji} {result.scenario}\n")
                f.write(f"- 状态: {'通过' if result.success else '失败'}\n")
                if result.issues:
                    f.write(f"- 问题: {', '.join(result.issues)}\n")
                f.write("\n")

            f.write("---\n")
            f.write("*第八轮测试完成*\n")

        print(f"\n📄 报告已保存至: {report_path}")


async def main():
    tester = ExtremeTester()
    await tester.run()


if __name__ == "__main__":
    asyncio.run(main())
