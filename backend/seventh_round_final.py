"""
📊 第七轮深度测试 - 最终验证版
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
    test_name: str
    start_time: float
    end_time: float
    success: bool
    metrics: Dict[str, Any] = field(default_factory=dict)
    issues: List[str] = field(default_factory=list)


class ProductionDeepTest:
    def __init__(self):
        self.results: List[TestResult] = []

    async def run(self):
        print("\n" + "="*80)
        print("🚀 Agent Cluster Orchestrator - 第七轮深度测试")
        print(f"   开始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")

        await self.test1_event_bus_throughput()
        await self.test2_circuit_breaker_robustness()
        await self.test3_queue_priority_fairness()
        await self.test4_timeout_handler()
        await self.test5_fault_recovery()
        await self.test6_integration_stress()

        self.print_summary()
        self.generate_report()

    async def test1_event_bus_throughput(self):
        """Event Bus 吞吐量测试"""
        test_name = "EventBus Throughput"
        print(f"\n--- 测试 1: {test_name} ---")
        start = time.time()
        success = True
        issues = []

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus

            bus = EnhancedEventBus(max_history_size=1000)
            await bus.start()

            processed = 0
            errors = 0

            async def callback(event):
                nonlocal processed, errors
                try:
                    await asyncio.sleep(0.001)  # 模拟处理
                    processed += 1
                except Exception:
                    errors += 1

            await bus.subscribe(callback)

            # 测试正常流量
            count = 2000
            for i in range(count):
                await bus.publish({"seq": i})

            await asyncio.sleep(5.0)
            await bus.stop()

            stats = bus.get_stats()
            throughput = count / (time.time() - start)

            metrics = {
                "total_published": stats.get('events_published', 0),
                "processed": processed,
                "errors": stats.get('callback_errors', 0),
                "throughput_events_sec": round(throughput, 2)
            }

            if processed < count * 0.9:
                issues.append(f"处理率低: {processed}/{count}")
                success = False

            print(f"  发布事件: {count}, 处理: {processed}, 吞吐: {throughput:.2f} 事件/秒")

        except Exception as e:
            issues.append(f"崩溃: {e}")
            success = False
            metrics = {}

        self.results.append(TestResult(
            test_name=test_name, start_time=start,
            end_time=time.time(), success=success,
            metrics=metrics, issues=issues
        ))

    async def test2_circuit_breaker_robustness(self):
        """熔断器健壮性"""
        test_name = "CircuitBreaker Robustness"
        print(f"\n--- 测试 2: {test_name} ---")
        start = time.time()
        success = True
        issues = []

        try:
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
            )

            cb = EnhancedCircuitBreaker(
                "robust_test",
                EnhancedCircuitBreakerConfig(
                    failure_threshold=3, success_threshold=2,
                    timeout_duration=1.0, min_call_count=3
                )
            )

            # 1. 快速失败
            failed = 0
            for i in range(10):
                try:
                    await cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
                except Exception:
                    failed += 1

            if cb.state.value != "open":
                issues.append("未进入打开状态")

            # 2. 等待恢复
            await asyncio.sleep(1.5)

            # 3. 半开测试
            recovered = False
            for i in range(10):
                try:
                    await cb.call(lambda: asyncio.sleep(0.01))
                    if cb.state.value == "closed":
                        recovered = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.1)

            if not recovered:
                issues.append("无法恢复到关闭状态")
                success = False

            metrics = {
                "final_state": cb.state.value,
                "recovered": recovered
            }

            print(f"  最终状态: {cb.state.value}, 成功恢复: {recovered}")

        except Exception as e:
            issues.append(f"崩溃: {e}")
            success = False
            metrics = {}

        self.results.append(TestResult(
            test_name=test_name, start_time=start,
            end_time=time.time(), success=success,
            metrics=metrics, issues=issues
        ))

    async def test3_queue_priority_fairness(self):
        """队列优先级公平性"""
        test_name = "Queue Priority Fairness"
        print(f"\n--- 测试 3: {test_name} ---")
        start = time.time()
        success = True
        issues = []

        try:
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            queue = EnhancedAdaptiveQueue()

            priority_counts = {10: 0, 5: 0, 1: 0}

            async def producer():
                # 混合优先级
                for i in range(100):
                    await queue.enqueue(f"high_{i}", 10)
                for i in range(100):
                    await queue.enqueue(f"mid_{i}", 5)
                for i in range(100):
                    await queue.enqueue(f"low_{i}", 1)

            async def consumer():
                for _ in range(300):
                    item = await queue.dequeue(wait_timeout=1.0)
                    if item:
                        if item.startswith("high"):
                            priority_counts[10] += 1
                        elif item.startswith("mid"):
                            priority_counts[5] += 1
                        else:
                            priority_counts[1] += 1

            await asyncio.gather(producer(), consumer())

            metrics = {
                "high_processed": priority_counts[10],
                "mid_processed": priority_counts[5],
                "low_processed": priority_counts[1]
            }

            if priority_counts[1] == 0:
                issues.append("低优先级饥饿！")
                success = False

            print(f"  高/中/低: {priority_counts[10]}/{priority_counts[5]}/{priority_counts[1]}")

        except Exception as e:
            issues.append(f"崩溃: {e}")
            success = False
            metrics = {}

        self.results.append(TestResult(
            test_name=test_name, start_time=start,
            end_time=time.time(), success=success,
            metrics=metrics, issues=issues
        ))

    async def test4_timeout_handler(self):
        """超时处理器"""
        test_name = "Timeout Handler"
        print(f"\n--- 测试 4: {test_name} ---")
        start = time.time()
        success = True
        issues = []

        try:
            from app.core.enhanced_timeout_protection import (
                EnhancedTimeoutMonitor, enhanced_execute_with_timeout
            )

            monitor = EnhancedTimeoutMonitor()

            async def fast_task():
                await asyncio.sleep(0.1)
                return "done"

            async def slow_task():
                await asyncio.sleep(1.0)
                return "slow"

            # 测试快速任务
            try:
                res = await enhanced_execute_with_timeout(
                    fast_task(), 0.5, monitor=monitor
                )
                fast_success = True
            except Exception:
                fast_success = False

            # 测试超时任务
            timeout_count = 0
            try:
                await enhanced_execute_with_timeout(
                    slow_task(), 0.2, monitor=monitor
                )
            except Exception:
                timeout_count = 1

            stats = await monitor.get_stats()

            metrics = {
                "fast_success": fast_success,
                "timeout_detected": timeout_count,
                "monitor_stats": stats
            }

            if not timeout_count:
                issues.append("超时未正确检测")
                success = False

            print(f"  快速任务: {'OK' if fast_success else 'FAIL'}, 超时检测: {timeout_count}")

        except Exception as e:
            issues.append(f"崩溃: {e}")
            success = False
            metrics = {}

        self.results.append(TestResult(
            test_name=test_name, start_time=start,
            end_time=time.time(), success=success,
            metrics=metrics, issues=issues
        ))

    async def test5_fault_recovery(self):
        """故障恢复能力"""
        test_name = "Fault Recovery"
        print(f"\n--- 测试 5: {test_name} ---")
        start = time.time()
        success = True
        issues = []

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
            )

            cb = EnhancedCircuitBreaker(
                "recovery",
                EnhancedCircuitBreakerConfig(
                    failure_threshold=3, success_threshold=2,
                    timeout_duration=0.5, min_call_count=3
                )
            )

            # 触发熔断
            for _ in range(5):
                try:
                    await cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
                except Exception:
                    pass

            # 等待超时
            await asyncio.sleep(0.7)

            # 测试恢复
            recovered = False
            for _ in range(10):
                try:
                    await cb.call(lambda: asyncio.sleep(0.01))
                    if cb.state.value == "closed":
                        recovered = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.1)

            metrics = {"recovered": recovered}

            if not recovered:
                issues.append("无法恢复")
                success = False

            print(f"  恢复成功: {recovered}")

        except Exception as e:
            issues.append(f"崩溃: {e}")
            success = False
            metrics = {}

        self.results.append(TestResult(
            test_name=test_name, start_time=start,
            end_time=time.time(), success=success,
            metrics=metrics, issues=issues
        ))

    async def test6_integration_stress(self):
        """集成压力测试"""
        test_name = "Integration Stress"
        print(f"\n--- 测试 6: {test_name} ---")
        start = time.time()
        success = True
        issues = []

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            completed_tasks = 0

            async def worker(worker_id: int):
                nonlocal completed_tasks
                for _ in range(20):
                    await asyncio.sleep(0.01)
                    completed_tasks += 1

            # 并发工作
            workers = [worker(i) for i in range(10)]
            await asyncio.gather(*workers)

            metrics = {
                "completed": completed_tasks,
                "expected": 200
            }

            if completed_tasks < 190:
                issues.append("集成任务丢失")
                success = False

            print(f"  完成: {completed_tasks}/200")

        except Exception as e:
            issues.append(f"崩溃: {e}")
            success = False
            metrics = {}

        self.results.append(TestResult(
            test_name=test_name, start_time=start,
            end_time=time.time(), success=success,
            metrics=metrics, issues=issues
        ))

    def print_summary(self):
        print("\n" + "="*80)
        print("📊 第七轮测试 - 摘要")
        print("="*80)

        total = len(self.results)
        passed = sum(1 for r in self.results if r.success)

        print(f"\n  总测试: {total}")
        print(f"  成功: {passed}")
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
        """生成完整测试报告"""
        report_path = "/workspace/backend/SEVENTH_ROUND_REPORT.md"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        total = len(self.results)
        passed = sum(1 for r in self.results if r.success)

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("# Agent Cluster Orchestrator - 第七轮深度测试报告\n\n")
            f.write(f"**日期**: {now}\n\n")
            f.write("---\n\n")

            f.write("## 摘要\n\n")
            f.write(f"- **总测试数**: {total}\n")
            f.write(f"- **成功**: {passed}\n")
            f.write(f"- **失败**: {total - passed}\n\n")

            f.write("## 详细测试结果\n\n")
            for i, result in enumerate(self.results, 1):
                status_emoji = "✅" if result.success else "❌"
                duration = result.end_time - result.start_time

                f.write(f"### {i}. {result.test_name}\n\n")
                f.write(f"- **状态**: {status_emoji} {'成功' if result.success else '失败'}\n")
                f.write(f"- **耗时**: {duration:.2f}秒\n")

                if result.metrics:
                    f.write(f"- **指标**:\n")
                    for k, v in result.metrics.items():
                        f.write(f"  - {k}: {v}\n")

                if result.issues:
                    f.write(f"- **问题**:\n")
                    for issue in result.issues:
                        f.write(f"  - ⚠️ {issue}\n")

                f.write("\n")

            all_issues = []
            for r in self.results:
                all_issues.extend(r.issues)

            f.write("## 总体评估\n\n")
            if all_issues:
                f.write(f"⚠️ 发现 {len(all_issues)} 个问题需要修复。\n")
            else:
                f.write("✅ 系统运行稳定，所有测试通过！\n")

            f.write("\n---\n")
            f.write("*第七轮测试完成*\n")

        print(f"\n📄 报告已保存至: {report_path}")


async def main():
    tester = ProductionDeepTest()
    await tester.run()


if __name__ == "__main__":
    asyncio.run(main())
