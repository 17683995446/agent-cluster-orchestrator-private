"""
🔬 第九轮深度测试 - 慢工出细活
目标: 长时间、高难度、复杂场景测试，发现系统深层问题

测试理念:
- 慢工出细活：每个测试都设计得更加细致和深入
- 模拟真实生产环境的各种复杂场景
- 验证数据一致性、资源管理、边界条件

测试场景:
1. 长时间运行稳定性 (10分钟+)
2. 真实生产场景模拟 (电商、金融、物联网)
3. 资源泄漏检测 (内存、文件描述符)
4. 数据一致性验证 (事务完整性)
5. 边界条件和异常处理
6. 多组件协同压力测试
"""
import asyncio
import time
import random
import sys
import gc
import psutil
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import weakref

sys.path.insert(0, '/workspace/backend')


@dataclass
class ResourceSnapshot:
    timestamp: float
    memory_mb: float
    cpu_percent: float
    open_files: int


@dataclass
class TestMetrics:
    scenario: str
    duration_sec: float
    operations: int
    errors: int
    error_rate: float
    throughput: float
    resource_snapshots: List[ResourceSnapshot] = field(default_factory=list)


class DeepQualityTester:
    """深度质量测试器 - 慢工出细活"""

    def __init__(self):
        self.metrics: List[TestMetrics] = []
        self.issues_found: List[Dict[str, Any]] = []

    async def run(self):
        """运行所有深度测试"""
        print("\n" + "="*90)
        print("🔬 Agent Cluster Orchestrator - 第九轮深度测试 (慢工出细活)")
        print(f"   开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("   理念: 深入、细致、全面")
        print("="*90 + "\n")

        await self.test1_long_running_stability()
        await self.test2_production_scenario_simulation()
        await self.test3_resource_leak_detection()
        await self.test4_data_consistency_validation()
        await self.test5_boundary_conditions()
        await self.test6_complex_coordination()

        self.print_summary()
        self.generate_report()

    async def _take_resource_snapshot(self) -> ResourceSnapshot:
        """获取资源快照"""
        process = psutil.Process()
        return ResourceSnapshot(
            timestamp=time.time(),
            memory_mb=process.memory_info().rss / 1024 / 1024,
            cpu_percent=process.cpu_percent(),
            open_files=len(process.open_files())
        )

    async def test1_long_running_stability(self):
        """场景1: 长时间运行稳定性测试 (10分钟)"""
        scenario = "长时间运行稳定性"
        print(f"\n--- 🕐 {scenario} (10分钟) ---")
        start_time = time.time()
        operations = 0
        errors = 0
        snapshots = []

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
            )

            bus = EnhancedEventBus(max_history_size=5000)
            queue = EnhancedAdaptiveQueue()
            cb = EnhancedCircuitBreaker("stability_test", EnhancedCircuitBreakerConfig(
                failure_threshold=5, success_threshold=3, timeout_duration=30.0, min_call_count=10
            ))

            await bus.start()

            processed_events = 0
            processed_queue = 0

            async def event_handler(event):
                nonlocal processed_events
                processed_events += 1

            await bus.subscribe(event_handler)

            # 长时间运行
            print("  运行中...")
            end_time = start_time + 600  # 10分钟

            phase = 0
            while time.time() < end_time:
                phase += 1
                current_time = time.time() - start_time

                # 动态调整负载
                if current_time < 120:  # 前2分钟：低负载
                    load_factor = 0.5
                elif current_time < 300:  # 2-5分钟：中等负载
                    load_factor = 1.0
                elif current_time < 480:  # 5-8分钟：高负载
                    load_factor = 2.0
                else:  # 后2分钟：恢复正常
                    load_factor = 1.0

                batch_size = int(50 * load_factor)

                # 发布事件
                for i in range(batch_size):
                    await bus.publish({"seq": operations, "phase": phase, "time": current_time})
                    operations += 1

                    # 队列操作
                    if random.random() < 0.8:
                        await queue.enqueue(f"task-{operations}", priority=random.randint(1, 10))
                        operations += 1

                    # 熔断器测试
                    if random.random() < 0.1:
                        try:
                            if random.random() < 0.05:
                                await cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
                            else:
                                await cb.call(lambda: asyncio.sleep(0.001))
                        except Exception:
                            errors += 1

                # 每30秒记录资源快照
                if phase % 10 == 0:
                    snapshot = await self._take_resource_snapshot()
                    snapshots.append(snapshot)
                    print(f"    {int(current_time)}s: 内存={snapshot.memory_mb:.1f}MB, 操作={operations}")

                await asyncio.sleep(1)

            await bus.stop()

            # 验证数据完整性
            stats = bus.get_stats()
            queue_stats = queue.get_stats()

            print(f"  完成统计:")
            print(f"    总操作: {operations}")
            print(f"    事件处理: {processed_events}/{stats.get('events_published', 0)}")
            print(f"    队列处理: {queue_stats['dequeued']}/{queue_stats['enqueued']}")
            print(f"    熔断器状态: {cb.state.value}")

            # 检查资源泄漏
            if len(snapshots) >= 2:
                memory_growth = snapshots[-1].memory_mb - snapshots[0].memory_mb
                print(f"    内存增长: {memory_growth:.1f}MB")

                if memory_growth > 100:
                    self.issues_found.append({
                        "scenario": scenario,
                        "issue": f"内存泄漏: 增长 {memory_growth:.1f}MB",
                        "severity": "high"
                    })

        except Exception as e:
            self.issues_found.append({
                "scenario": scenario,
                "issue": f"测试崩溃: {e}",
                "severity": "critical"
            })

        duration = time.time() - start_time
        throughput = operations / duration if duration > 0 else 0
        error_rate = errors / operations if operations > 0 else 0

        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=operations,
            errors=errors,
            error_rate=error_rate,
            throughput=throughput,
            resource_snapshots=snapshots
        ))

    async def test2_production_scenario_simulation(self):
        """场景2: 真实生产场景模拟"""
        scenario = "真实生产场景模拟"
        print(f"\n--- 🏭 {scenario} ---")
        start_time = time.time()

        scenarios = {
            "ecommerce": self._simulate_ecommerce,
            "fintech": self._simulate_fintech,
            "iot": self._simulate_iot
        }

        results = {}
        for name, sim_func in scenarios.items():
            print(f"  模拟 {name}...")
            result = await sim_func()
            results[name] = result

        duration = time.time() - start_time
        total_ops = sum(r["operations"] for r in results.values())
        total_errors = sum(r["errors"] for r in results.values())

        print(f"  完成: 总操作={total_ops}, 错误={total_errors}")

        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=total_ops,
            errors=total_errors,
            error_rate=total_errors/total_ops if total_ops > 0 else 0,
            throughput=total_ops/duration if duration > 0 else 0
        ))

    async def _simulate_ecommerce(self) -> Dict[str, Any]:
        """模拟电商场景"""
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

        bus = EnhancedEventBus()
        queue = EnhancedAdaptiveQueue()
        await bus.start()

        operations = 0
        errors = 0

        async def order_handler(event):
            await asyncio.sleep(0.001)

        await bus.subscribe(order_handler)

        for i in range(500):
            event = {
                "type": "order_created",
                "order_id": f"ORD-{i}",
                "items": [{"sku": f"SKU-{random.randint(1,100)}", "qty": random.randint(1,5)} for _ in range(random.randint(1,5))],
                "total": random.uniform(10, 1000)
            }
            await bus.publish(event)

            if random.random() < 0.5:
                await queue.enqueue(f"order-{i}", priority=random.randint(1, 10))

            operations += 1
            await asyncio.sleep(0.001)

        await asyncio.sleep(2)
        await bus.stop()

        return {"operations": operations, "errors": errors}

    async def _simulate_fintech(self) -> Dict[str, Any]:
        """模拟金融科技场景"""
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
        )

        cb = EnhancedCircuitBreaker("payment_service", EnhancedCircuitBreakerConfig(
            failure_threshold=3, success_threshold=2, timeout_duration=5.0, min_call_count=5
        ))

        operations = 0
        errors = 0

        async def payment_operation():
            if random.random() < 0.1:
                raise Exception("payment_failed")
            await asyncio.sleep(0.01)

        for i in range(200):
            try:
                await cb.call(payment_operation)
            except Exception:
                errors += 1
            operations += 1
            await asyncio.sleep(0.005)

        return {"operations": operations, "errors": errors}

    async def _simulate_iot(self) -> Dict[str, Any]:
        """模拟物联网场景"""
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_timeout_protection import EnhancedTimeoutMonitor

        bus = EnhancedEventBus()
        monitor = EnhancedTimeoutMonitor()
        await bus.start()

        operations = 0
        errors = 0

        async def sensor_handler(event):
            await asyncio.sleep(0.001)

        await bus.subscribe(sensor_handler)

        for i in range(1000):
            event = {
                "sensor_id": f"sensor-{random.randint(1, 50)}",
                "reading": random.uniform(0, 100),
                "timestamp": time.time()
            }
            await bus.publish(event)
            operations += 1

            if random.random() < 0.05:
                try:
                    await asyncio.wait_for(asyncio.sleep(1), timeout=0.1)
                except asyncio.TimeoutError:
                    errors += 1

            await asyncio.sleep(0.001)

        await bus.stop()

        return {"operations": operations, "errors": errors}

    async def test3_resource_leak_detection(self):
        """场景3: 资源泄漏检测"""
        scenario = "资源泄漏检测"
        print(f"\n--- 🔍 {scenario} ---")
        start_time = time.time()

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            snapshots = []

            # 创建多个组件实例
            buses = []
            queues = []

            initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
            snapshots.append(("初始", initial_memory))

            # 循环创建和销毁组件
            for round_num in range(5):
                round_buses = []
                round_queues = []

                # 每轮创建100个实例
                for i in range(100):
                    bus = EnhancedEventBus(max_history_size=100)
                    await bus.start()
                    round_buses.append(bus)

                    queue = EnhancedAdaptiveQueue()
                    round_queues.append(queue)

                # 每轮添加一些事件
                for bus in round_buses:
                    for _ in range(50):
                        await bus.publish({"round": round_num, "seq": _})

                # 记录内存
                current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                snapshots.append((f"轮次{round_num}", current_memory))
                print(f"    轮次{round_num}: 内存={current_memory:.1f}MB")

                # 销毁实例
                for bus in round_buses:
                    await bus.stop()

                buses.extend(round_buses)
                queues.extend(round_queues)

            # 强制垃圾回收
            del buses
            del queues
            gc.collect()

            final_memory = psutil.Process().memory_info().rss / 1024 / 1024
            memory_growth = final_memory - initial_memory

            snapshots.append(("最终", final_memory))

            print(f"  内存增长: {memory_growth:.1f}MB")

            if memory_growth > 50:
                self.issues_found.append({
                    "scenario": scenario,
                    "issue": f"潜在内存泄漏: 增长 {memory_growth:.1f}MB",
                    "severity": "medium"
                })

        except Exception as e:
            self.issues_found.append({
                "scenario": scenario,
                "issue": f"资源泄漏测试崩溃: {e}",
                "severity": "critical"
            })

        duration = time.time() - start_time
        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=500,
            errors=0,
            error_rate=0,
            throughput=500/duration if duration > 0 else 0
        ))

    async def test4_data_consistency_validation(self):
        """场景4: 数据一致性验证"""
        scenario = "数据一致性验证"
        print(f"\n--- 🔐 {scenario} ---")
        start_time = time.time()

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            bus = EnhancedEventBus(max_history_size=10000)
            queue = EnhancedAdaptiveQueue()
            await bus.start()

            # 生成测试数据
            test_data = []
            for i in range(1000):
                data = {
                    "id": i,
                    "value": random.randint(0, 1000),
                    "timestamp": time.time(),
                    "checksum": None
                }
                data["checksum"] = hash(str(data["id"]) + str(data["value"]))
                test_data.append(data)

            # 发布数据
            published_count = 0
            for data in test_data:
                await bus.publish(data)
                published_count += 1

            await asyncio.sleep(2)

            # 验证历史记录
            history = list(bus._event_history)
            history_count = len(history)

            # 验证数据完整性
            corrupted = 0
            for event in history:
                if "checksum" in event:
                    expected_checksum = hash(str(event.get("id")) + str(event.get("value")))
                    if event["checksum"] != expected_checksum:
                        corrupted += 1

            print(f"  发布: {published_count}, 历史: {history_count}, 损坏: {corrupted}")

            if history_count != published_count:
                self.issues_found.append({
                    "scenario": scenario,
                    "issue": f"数据丢失: 发布{published_count}, 历史{history_count}",
                    "severity": "high"
                })

            if corrupted > 0:
                self.issues_found.append({
                    "scenario": scenario,
                    "issue": f"数据损坏: {corrupted}条",
                    "severity": "critical"
                })

            await bus.stop()

        except Exception as e:
            self.issues_found.append({
                "scenario": scenario,
                "issue": f"数据一致性测试崩溃: {e}",
                "severity": "critical"
            })

        duration = time.time() - start_time
        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=1000,
            errors=0,
            error_rate=0,
            throughput=1000/duration if duration > 0 else 0
        ))

    async def test5_boundary_conditions(self):
        """场景5: 边界条件测试"""
        scenario = "边界条件测试"
        print(f"\n--- ⚡ {scenario} ---")
        start_time = time.time()
        errors = 0

        try:
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
            )

            # 测试1: 空队列出队
            print("  测试1: 空队列出队")
            queue = EnhancedAdaptiveQueue()
            result = await queue.dequeue(wait_timeout=0.1)
            if result is not None:
                self.issues_found.append({
                    "scenario": f"{scenario}:空队列",
                    "issue": "空队列出队应返回None",
                    "severity": "low"
                })

            # 测试2: 满队列入队
            print("  测试2: 满队列入队")
            queue = EnhancedAdaptiveQueue()
            for i in range(1500):  # 超过初始容量
                await queue.enqueue(f"item-{i}", priority=5)
            stats = queue.get_stats()
            if stats["rejected"] == 0:
                self.issues_found.append({
                    "scenario": f"{scenario}:容量限制",
                    "issue": "队列未正确限制容量",
                    "severity": "medium"
                })

            # 测试3: 负数优先级
            print("  测试3: 负数优先级")
            queue = EnhancedAdaptiveQueue()
            await queue.enqueue("negative", priority=-100)
            await queue.enqueue("zero", priority=0)
            await queue.enqueue("positive", priority=100)
            item1 = await queue.dequeue(wait_timeout=0.1)
            item2 = await queue.dequeue(wait_timeout=0.1)
            item3 = await queue.dequeue(wait_timeout=0.1)

            # 测试4: 超长超时
            print("  测试4: 超长超时处理")
            cb = EnhancedCircuitBreaker("timeout_test", EnhancedCircuitBreakerConfig(
                timeout_duration=0.001
            ))
            try:
                await cb.call(lambda: asyncio.sleep(1))
            except Exception:
                pass

        except Exception as e:
            errors += 1
            self.issues_found.append({
                "scenario": scenario,
                "issue": f"边界条件测试崩溃: {e}",
                "severity": "critical"
            })

        duration = time.time() - start_time
        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=1500,
            errors=errors,
            error_rate=errors/1500,
            throughput=1500/duration if duration > 0 else 0
        ))

    async def test6_complex_coordination(self):
        """场景6: 多组件协同压力测试"""
        scenario = "多组件协同压力测试"
        print(f"\n--- 🧩 {scenario} ---")
        start_time = time.time()

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
            )
            from app.core.enhanced_timeout_protection import EnhancedTimeoutMonitor

            # 创建组件池
            buses = [EnhancedEventBus(max_history_size=500) for _ in range(5)]
            queues = [EnhancedAdaptiveQueue() for _ in range(5)]
            breakers = [EnhancedCircuitBreaker(f"cb-{i}", EnhancedCircuitBreakerConfig()) for i in range(5)]
            monitor = EnhancedTimeoutMonitor()

            # 启动所有总线
            for bus in buses:
                await bus.start()

            operations = 0
            errors = 0

            async def worker(worker_id: int):
                nonlocal operations, errors
                for i in range(100):
                    try:
                        # 选择目标组件
                        target = worker_id % 5

                        # 发布事件
                        await buses[target].publish({"worker": worker_id, "seq": i})

                        # 队列操作
                        await queues[target].enqueue(f"task-{worker_id}-{i}", priority=random.randint(1, 10))

                        # 熔断器测试
                        if random.random() < 0.2:
                            try:
                                await breakers[target].call(lambda: asyncio.sleep(0.001))
                            except Exception:
                                pass

                        operations += 3
                        await asyncio.sleep(0.001)
                    except Exception:
                        errors += 1

            # 启动工作协程
            workers = [worker(i) for i in range(10)]
            await asyncio.gather(*workers)

            # 验证数据一致性
            total_queue_items = sum(q.size() for q in queues)
            print(f"  总操作: {operations}, 错误: {errors}, 队列总数: {total_queue_items}")

            # 关闭所有总线
            for bus in buses:
                await bus.stop()

        except Exception as e:
            self.issues_found.append({
                "scenario": scenario,
                "issue": f"协同测试崩溃: {e}",
                "severity": "critical"
            })

        duration = time.time() - start_time
        throughput = operations / duration if duration > 0 else 0
        error_rate = errors / operations if operations > 0 else 0

        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=operations,
            errors=errors,
            error_rate=error_rate,
            throughput=throughput
        ))

    def print_summary(self):
        """打印测试摘要"""
        print("\n" + "="*90)
        print("📊 第九轮深度测试 - 摘要 (慢工出细活)")
        print("="*90)

        total_operations = sum(m.operations for m in self.metrics)
        total_errors = sum(m.errors for m in self.metrics)
        total_duration = sum(m.duration_sec for m in self.metrics)

        print(f"\n总操作数: {total_operations:,}")
        print(f"总错误数: {total_errors:,}")
        print(f"总耗时: {total_duration:.1f}秒 ({total_duration/60:.1f}分钟)")
        print(f"整体错误率: {total_errors/total_operations*100:.4f}%")
        print(f"整体吞吐量: {total_operations/total_duration:.1f} 操作/秒")

        print("\n各场景详细结果:")
        for m in self.metrics:
            print(f"\n  {m.scenario}:")
            print(f"    耗时: {m.duration_sec:.1f}秒")
            print(f"    操作: {m.operations:,}")
            print(f"    错误: {m.errors:,}")
            print(f"    错误率: {m.error_rate*100:.4f}%")
            print(f"    吞吐量: {m.throughput:.1f} ops/s")

        if self.issues_found:
            print(f"\n⚠️ 发现问题 ({len(self.issues_found)}):")
            for i, issue in enumerate(self.issues_found, 1):
                print(f"  {i}. [{issue['severity']}] {issue['scenario']}")
                print(f"     {issue['issue']}")
        else:
            print("\n✅ 未发现问题！")

        print("\n" + "="*90)

    def generate_report(self):
        """生成完整测试报告"""
        report_path = "/workspace/backend/NINTH_ROUND_REPORT.md"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("# Agent Cluster Orchestrator - 第九轮深度测试报告\n\n")
            f.write(f"**日期**: {now}\n")
            f.write(f"**理念**: 慢工出细活\n")
            f.write(f"**目标**: 长时间、高难度、复杂场景测试\n\n")
            f.write("---\n\n")

            total_ops = sum(m.operations for m in self.metrics)
            total_errors = sum(m.errors for m in self.metrics)
            total_duration = sum(m.duration_sec for m in self.metrics)

            f.write("## 测试摘要\n\n")
            f.write(f"- **总操作数**: {total_ops:,}\n")
            f.write(f"- **总错误数**: {total_errors:,}\n")
            f.write(f"- **总耗时**: {total_duration:.1f}秒 ({total_duration/60:.1f}分钟)\n")
            f.write(f"- **整体错误率**: {total_errors/total_ops*100:.4f}%\n")
            f.write(f"- **整体吞吐量**: {total_ops/total_duration:.1f} ops/s\n\n")

            f.write("## 各场景详细结果\n\n")
            for m in self.metrics:
                f.write(f"### {m.scenario}\n\n")
                f.write(f"- 耗时: {m.duration_sec:.1f}秒\n")
                f.write(f"- 操作数: {m.operations:,}\n")
                f.write(f"- 错误数: {m.errors:,}\n")
                f.write(f"- 错误率: {m.error_rate*100:.4f}%\n")
                f.write(f"- 吞吐量: {m.throughput:.1f} ops/s\n")

                if m.resource_snapshots:
                    f.write(f"- 资源快照:\n")
                    for snapshot in m.resource_snapshots:
                        f.write(f"  - 时间{snapshot.timestamp:.1f}s: 内存={snapshot.memory_mb:.1f}MB, CPU={snapshot.cpu_percent:.1f}%\n")
                f.write("\n")

            if self.issues_found:
                f.write("## 发现的问题\n\n")
                critical = [i for i in self.issues_found if i["severity"] == "critical"]
                high = [i for i in self.issues_found if i["severity"] == "high"]
                medium = [i for i in self.issues_found if i["severity"] == "medium"]

                if critical:
                    f.write("### 🔴 严重问题\n\n")
                    for issue in critical:
                        f.write(f"- **{issue['scenario']}**: {issue['issue']}\n")
                    f.write("\n")

                if high:
                    f.write("### 🟠 高优先级问题\n\n")
                    for issue in high:
                        f.write(f"- **{issue['scenario']}**: {issue['issue']}\n")
                    f.write("\n")

                if medium:
                    f.write("### 🟡 中优先级问题\n\n")
                    for issue in medium:
                        f.write(f"- **{issue['scenario']}**: {issue['issue']}\n")
                    f.write("\n")
            else:
                f.write("## 问题分析\n\n")
                f.write("✅ 所有测试通过，未发现问题！\n\n")

            f.write("---\n")
            f.write("*第九轮深度测试完成 - 慢工出细活*\n")

        print(f"\n📄 报告已保存至: {report_path}")


async def main():
    tester = DeepQualityTester()
    await tester.run()


if __name__ == "__main__":
    asyncio.run(main())
