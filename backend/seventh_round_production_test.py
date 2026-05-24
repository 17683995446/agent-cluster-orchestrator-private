"""
🚀 第七轮深度测试 - 真实生产环境模拟
目标: 模拟真实生产环境的复杂场景，发现隐藏的生产级别问题
"""
import asyncio
import time
import random
import sys
import psutil
import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import structlog
from collections import defaultdict, deque

sys.path.insert(0, '/workspace/backend')

logger = structlog.get_logger(__name__)


@dataclass
class ProductionLoadProfile:
    """真实生产负载配置"""
    name: str
    duration: int  # 秒
    max_rps: int
    burst_period: int
    burst_peak: int
    error_rate: float
    slow_request_rate: float
    recovery_period: int
    traffic_pattern: str = "random"  # random, burst, steady, growing


@dataclass
class TestResult:
    """测试结果数据"""
    test_name: str
    start_time: float
    end_time: float
    success: bool
    metrics: Dict[str, Any] = field(default_factory=dict)
    issues_found: List[str] = field(default_factory=list)


class ProductionEnvironmentSimulator:
    """真实生产环境模拟器"""

    def __init__(self):
        self.results: List[TestResult] = []
        self.issues_found: List[str] = []
        self.load_profiles = [
            ProductionLoadProfile(
                name="steady",
                duration=180,
                max_rps=100,
                burst_period=60,
                burst_peak=200,
                error_rate=0.02,
                slow_request_rate=0.05,
                recovery_period=30,
                traffic_pattern="steady"
            ),
            ProductionLoadProfile(
                name="burst_traffic",
                duration=120,
                max_rps=50,
                burst_period=30,
                burst_peak=300,
                error_rate=0.03,
                slow_request_rate=0.1,
                recovery_period=20,
                traffic_pattern="burst"
            ),
            ProductionLoadProfile(
                name="growing_load",
                duration=240,
                max_rps=50,
                burst_period=0,
                burst_peak=0,
                error_rate=0.01,
                slow_request_rate=0.03,
                recovery_period=60,
                traffic_pattern="growing"
            ),
            ProductionLoadProfile(
                name="chaos_test",
                duration=300,
                max_rps=150,
                burst_period=45,
                burst_peak=400,
                error_rate=0.1,
                slow_request_rate=0.15,
                recovery_period=90,
                traffic_pattern="random"
            )
        ]

    async def run(self):
        """运行完整生产环境模拟"""
        print("\n" + "=" * 80)
        print("🚀 第七轮深度测试 - 真实生产环境模拟")
        print(f"   开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80 + "\n")

        # 各组件测试
        await self.test_event_bus_saturation()
        await self.test_circuit_breaker_coordination()
        await self.test_queue_resource_exhaustion()
        await self.test_multi_component_integration()
        await self.test_fault_recovery()
        await self.test_resource_monitoring()

        # 总结
        self.print_summary()

    async def test_event_bus_saturation(self):
        """测试1: EventBus饱和场景"""
        test_name = "event_bus_saturation"
        print(f"\n--- 测试1: {test_name} ---")
        start = time.time()
        success = True
        metrics = {}

        try:
            from app.core.enhanced_event_bus import EnhancedEventBus

            bus = EnhancedEventBus(max_history_size=5000)
            await bus.start()

            processed = 0
            errors = 0
            dropped = 0

            async def callback(event):
                nonlocal processed, errors
                try:
                    if random.random() < 0.02:
                        raise Exception("random callback error")
                    await asyncio.sleep(random.uniform(0.001, 0.05))
                    processed += 1
                except Exception:
                    errors += 1

            await bus.subscribe(callback)

            # 第一阶段: 平稳流量
            print("阶段1: 平稳流量 (30秒)")
            for i in range(30):
                for j in range(50):
                    await bus.publish({"type": "normal", "idx": j})
                await asyncio.sleep(1)

            # 第二阶段: 突发流量
            print("阶段2: 突发流量 (30秒)")
            for i in range(30):
                burst = random.randint(100, 500)
                for j in range(burst):
                    await bus.publish({"type": "burst", "idx": j})
                await asyncio.sleep(1)

            # 第三阶段: 超高负载
            print("阶段3: 极限负载 (30秒)")
            for i in range(30):
                for j in range(200):
                    await bus.publish({"type": "extreme", "idx": j})
                await asyncio.sleep(1)

            await asyncio.sleep(2)
            await bus.stop()

            stats = bus.get_stats()
            metrics = {
                "events_published": stats.get('events_published', 0),
                "events_processed": processed,
                "callback_errors": stats.get('callback_errors', 0),
                "events_dropped": stats.get('events_dropped', 0),
                "history_size": len(bus._event_history)
            }

            if metrics["events_dropped"] > metrics["events_published"] * 0.1:
                self.issues_found.append(f"EventBus掉包率过高: {metrics['events_dropped']/metrics['events_published']*100:.1f}%")
                success = False

            if len(bus._event_history) > 5000:
                self.issues_found.append(f"EventBus历史超限: {len(bus._event_history)} > 5000")
                success = False

        except Exception as e:
            self.issues_found.append(f"EventBus饱和测试崩溃: {e}")
            success = False

        self.results.append(TestResult(
            test_name=test_name,
            start_time=start,
            end_time=time.time(),
            success=success,
            metrics=metrics
        ))

        print(f"  {test_name}: {'✅ 成功' if success else '❌ 失败'}")

    async def test_circuit_breaker_coordination(self):
        """测试2: 多熔断器协调"""
        test_name = "circuit_breaker_coordination"
        print(f"\n--- 测试2: {test_name} ---")
        start = time.time()
        success = True
        metrics = {}

        try:
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )

            # 创建相互依赖的熔断器
            breakers = {
                f"service_{i}": EnhancedCircuitBreaker(
                    f"service_{i}",
                    EnhancedCircuitBreakerConfig(
                        failure_threshold=5 if i == 0 else 3,
                        success_threshold=2,
                        timeout_duration=10.0
                    )
                )
                for i in range(5)
            }

            open_count = defaultdict(int)
            total_calls = defaultdict(int)
            total_errors = defaultdict(int)

            async def service_call(service_idx):
                try:
                    cb = breakers[f"service_{service_idx}"]
                    # 模拟服务间调用
                    async def operation():
                        # 服务0依赖其他服务
                        if service_idx == 0:
                            for j in range(1, 5):
                                try:
                                    await breakers[f"service_{j}"].call(
                                        lambda: asyncio.sleep(random.uniform(0.01, 0.1))
                                    )
                                except Exception:
                                    pass
                        if random.random() < 0.15:
                            raise Exception("service failure")
                        await asyncio.sleep(random.uniform(0.01, 0.05))
                    await cb.call(operation)
                    total_calls[service_idx] += 1
                except Exception:
                    total_errors[service_idx] += 1
                    if breakers[f"service_{service_idx}"].state.value == "open":
                        open_count[service_idx] += 1

            # 运行测试
            for _ in range(120):
                tasks = [asyncio.create_task(service_call(random.randint(0, 4))) for _ in range(20)]
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(0.1)

            metrics = {
                "total_calls": sum(total_calls.values()),
                "total_errors": sum(total_errors.values()),
                "open_events": sum(open_count.values()),
                "breakers_opened": sum(1 for cb in breakers.values() if cb.state.value == "open")
            }

        except Exception as e:
            self.issues_found.append(f"熔断器协调测试崩溃: {e}")
            success = False

        self.results.append(TestResult(
            test_name=test_name,
            start_time=start,
            end_time=time.time(),
            success=success,
            metrics=metrics
        ))

        print(f"  {test_name}: {'✅ 成功' if success else '❌ 失败'}")

    async def test_queue_resource_exhaustion(self):
        """测试3: 队列资源耗尽"""
        test_name = "queue_resource_exhaustion"
        print(f"\n--- 测试3: {test_name} ---")
        start = time.time()
        success = True
        metrics = {}

        try:
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue, EnhancedQueueConfig

            queue = EnhancedAdaptiveQueue(EnhancedQueueConfig(
                initial_capacity=100,
                max_capacity=500,
                growth_cooldown=2.0
            ))

            # 极端写入：超过队列容量很多
            rejected = 0
            accepted = 0
            producers = []

            async def producer(pid):
                nonlocal rejected, accepted
                for j in range(200):
                    result = await queue.enqueue(f"item_{pid}_{j}", priority=random.randint(1, 10))
                    if result:
                        accepted += 1
                    else:
                        rejected += 1
                    await asyncio.sleep(0.001)

            for i in range(5):
                producers.append(asyncio.create_task(producer(i)))

            await asyncio.gather(*producers)

            # 出队
            dequeued = 0
            for _ in range(queue.size() + 50):
                item = await queue.dequeue(wait_timeout=0.1)
                if item:
                    dequeued += 1

            stats = queue.get_stats()
            metrics = {
                "accepted": accepted,
                "rejected": rejected,
                "dequeued": dequeued,
                "final_size": queue.size(),
                "rejection_rate": rejected / max(accepted + rejected, 1)
            }

            if queue.size() > 100:
                self.issues_found.append(f"队列积压: {queue.size()}个")
                success = False

        except Exception as e:
            self.issues_found.append(f"队列资源耗尽测试崩溃: {e}")
            success = False

        self.results.append(TestResult(
            test_name=test_name,
            start_time=start,
            end_time=time.time(),
            success=success,
            metrics=metrics
        ))

        print(f"  {test_name}: {'✅ 成功' if success else '❌ 失败'}")

    async def test_multi_component_integration(self):
        """测试4: 多组件深度集成"""
        test_name = "multi_component_integration"
        print(f"\n--- 测试4: {test_name} ---")
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
            from app.core.enhanced_timeout_protection import (
                EnhancedTimeoutMonitor,
                enhanced_execute_with_timeout
            )

            # 全部组件一起工作
            bus = EnhancedEventBus()
            queue = EnhancedAdaptiveQueue()
            cb = EnhancedCircuitBreaker("integration", EnhancedCircuitBreakerConfig())
            monitor = EnhancedTimeoutMonitor()
            await bus.start()

            operations = 0
            errors = 0
            messages_processed = 0

            async def consumer():
                nonlocal messages_processed
                while True:
                    item = await queue.dequeue(wait_timeout=0.5)
                    if item:
                        messages_processed += 1
                        # 使用熔断器和超时保护处理
                        try:
                            await enhanced_execute_with_timeout(
                                process_message(item),
                                timeout=2.0,
                                monitor=monitor
                            )
                        except Exception:
                            pass

            async def process_message(msg):
                await asyncio.sleep(random.uniform(0.001, 0.1))
                if random.random() < 0.05:
                    raise Exception("processing error")
                await bus.publish({"type": "processed", "msg": msg})

            async def callback(event):
                pass

            await bus.subscribe(callback)

            # 启动消费者
            consumers = [asyncio.create_task(consumer()) for _ in range(3)]

            # 生产负载
            for i in range(180):
                await queue.enqueue(f"load_{i}")
                await asyncio.sleep(0.02)

            await asyncio.sleep(5)
            await bus.stop()
            for c in consumers:
                c.cancel()

            metrics = {
                "operations": operations,
                "errors": errors,
                "messages_processed": messages_processed,
                "queue_size": queue.size()
            }

        except Exception as e:
            self.issues_found.append(f"多组件集成测试崩溃: {e}")
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
        """测试5: 故障恢复能力"""
        test_name = "fault_recovery"
        print(f"\n--- 测试5: {test_name} ---")
        start = time.time()
        success = True
        metrics = {}

        try:
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )
            from app.core.enhanced_event_bus import EnhancedEventBus

            recovery_times = []

            # 测试熔断器恢复
            cb = EnhancedCircuitBreaker(
                "recovery",
                EnhancedCircuitBreakerConfig(
                    failure_threshold=3,
                    success_threshold=2,
                    timeout_duration=2.0
                )
            )

            # 先让它打开
            print("步骤1: 触发熔断")
            for i in range(10):
                try:
                    await cb.call(lambda: (_ for _ in ()).throw(Exception("boom")))
                except Exception:
                    pass

            # 等待超时
            print("步骤2: 等待恢复")
            await asyncio.sleep(3.0)

            # 测试恢复
            print("步骤3: 验证恢复")
            recovery_start = time.time()
            recovered = False

            for attempt in range(20):
                try:
                    await cb.call(lambda: asyncio.sleep(0.001))
                    if cb.state.value == "closed":
                        recovered = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.1)

            if recovered:
                recovery_times.append(time.time() - recovery_start)
            else:
                self.issues_found.append("熔断器无法从打开状态恢复")
                success = False

            metrics = {
                "recovery_time": recovery_times[0] if recovery_times else -1
            }

        except Exception as e:
            self.issues_found.append(f"故障恢复测试崩溃: {e}")
            success = False

        self.results.append(TestResult(
            test_name=test_name,
            start_time=start,
            end_time=time.time(),
            success=success,
            metrics=metrics
        ))

        print(f"  {test_name}: {'✅ 成功' if success else '❌ 失败'}")

    async def test_resource_monitoring(self):
        """测试6: 资源监控和稳定性"""
        test_name = "resource_monitoring"
        print(f"\n--- 测试6: {test_name} ---")
        start = time.time()
        success = True
        metrics = {}

        try:
            import psutil
            process = psutil.Process()

            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            bus = EnhancedEventBus()
            queue = EnhancedAdaptiveQueue()
            await bus.start()

            memory_history = []
            cpu_history = []

            async def monitor():
                for _ in range(60):
                    memory_history.append(process.memory_info().rss / 1024 / 1024)
                    cpu_history.append(process.cpu_percent())
                    await asyncio.sleep(0.5)

            async def load():
                for i in range(30):
                    for j in range(100):
                        await bus.publish({"idx": j})
                        await queue.enqueue(f"q_{i}_{j}")
                    await asyncio.sleep(0.5)
                for _ in range(queue.size()):
                    await queue.dequeue(wait_timeout=0.01)

            await asyncio.gather(monitor(), load())
            await bus.stop()

            metrics = {
                "memory_peak": max(memory_history),
                "memory_avg": sum(memory_history) / len(memory_history),
                "cpu_peak": max(cpu_history),
                "cpu_avg": sum(cpu_history) / len(cpu_history)
            }

            memory_growth = metrics["memory_peak"] - metrics["memory_avg"]
            if memory_growth > 50:
                self.issues_found.append(f"内存增长异常: 峰值比平均高 {memory_growth:.1f}MB")
                success = False

        except Exception as e:
            self.issues_found.append(f"资源监控测试崩溃: {e}")
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
        """打印完整总结"""
        print("\n" + "=" * 80)
        print("📊 第七轮深度测试总结")
        print("=" * 80)

        total_tests = len(self.results)
        passed = sum(1 for r in self.results if r.success)

        print(f"\n总测试: {total_tests}, 通过: {passed}, 失败: {total_tests - passed}")

        if self.issues_found:
            print(f"\n⚠️ 发现问题 ({len(self.issues_found)}):")
            for i, issue in enumerate(self.issues_found):
                print(f"  {i+1}. {issue}")
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
    simulator = ProductionEnvironmentSimulator()
    await simulator.run()


if __name__ == "__main__":
    asyncio.run(main())
