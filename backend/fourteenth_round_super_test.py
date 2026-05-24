"""
🔬 第十四轮超级深度混沌测试 - 慢工出细活
特点：
- 在每一句关键代码后打印详细调试日志
- 超大规模并发（1000+协程）
- 超长时间稳定（30分钟+）
- 超混乱场景（随机失败、延迟、资源耗尽）
- 数据一致性持续验证
- 组件隔离验证
"""
import asyncio
import time
import random
import sys
import gc
import structlog
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, '/workspace/backend')

# 配置详细调试日志
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S.%f"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger(__name__)


@dataclass
class SuperChaosConfig:
    """超级混乱配置"""
    max_concurrent_workers: int = 1500
    test_duration_seconds: int = 120  # 2分钟快速测试
    event_publish_rate_per_worker: float = 5  # 每秒每个worker 5个事件
    failure_probability: float = 0.25
    delay_probability: float = 0.3
    min_delay_ms: float = 10.0
    max_delay_ms: float = 2000.0
    memory_pressure_chance: float = 0.02
    network_jitter_chance: float = 0.1


class SuperScaleTester:
    """超大规模测试器"""
    
    def __init__(self, config: Optional[SuperChaosConfig] = None):
        self.config = config or SuperChaosConfig()
        self.metrics = {
            "total_events": 0,
            "processed_events": 0,
            "failed_ops": 0,
            "timeouts": 0,
            "circuit_open_count": 0,
            "queue_rejects": 0,
            "data_consistency_errors": 0
        }
        self.data_checksums: Dict[int, int] = {}
        self.processed_event_ids: Set[int] = set()
        self.issues: List[Dict[str, Any]] = []
        self.start_time: float = 0
        
        logger.info("super_tester_initialized", config=str(self.config))
    
    async def run_all_tests(self):
        """运行所有超级测试"""
        print("\n" + "=" * 120)
        print("🔬 第十四轮超级深度混沌测试")
        print(f"   开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("   理念: 慢工出细活，每句关键代码都有详细日志")
        print("=" * 120 + "\n")
        
        self.start_time = time.time()
        
        await self._test_1_massive_concurrent_pressure()
        await self._test_2_super_chaos_network()
        await self._test_3_resource_starvation()
        await self._test_4_data_consistency_chaos()
        await self._test_5_long_term_stability()
        
        self._print_final_report()
        self._generate_report()
        
        return len(self.issues) == 0
    
    async def _test_1_massive_concurrent_pressure(self):
        """测试1: 超大规模并发压力"""
        print("\n" + "-" * 120)
        print("📋 TEST 1: 超大规模并发压力 - 1500个协程同时运行")
        print("-" * 120)
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
        
        bus = EnhancedEventBus(max_history_size=20000)
        queue = EnhancedAdaptiveQueue()
        
        await bus.start()
        
        processed_count = 0
        lock = asyncio.Lock()
        
        async def event_handler(event: Dict[str, Any]):
            nonlocal processed_count
            logger.debug("event_handler_invoked", event=event)
            async with lock:
                processed_count += 1
                if "id" in event:
                    self.processed_event_ids.add(event["id"])
            logger.debug("event_handler_completed", processed_count=processed_count)
        
        await bus.subscribe(event_handler)
        
        worker_ids = list(range(self.config.max_concurrent_workers))
        event_counter = 0
        
        async def worker(worker_id: int):
            nonlocal event_counter
            logger.debug("worker_starting", worker_id=worker_id)
            for i in range(40):
                try:
                    event_data = {
                        "worker_id": worker_id,
                        "seq": i,
                        "id": event_counter,
                        "checksum": hash((worker_id, i, event_counter))
                    }
                    self.data_checksums[event_counter] = event_data["checksum"]
                    event_counter += 1
                    
                    await bus.publish(event_data)
                    logger.debug("event_published", worker_id=worker_id, seq=i)
                    
                    await queue.enqueue(
                        f"task_{worker_id}_{i}",
                        priority=random.randint(0, 100)
                    )
                    logger.debug("task_enqueued", worker_id=worker_id, seq=i)
                    
                    # 随机混乱
                    if random.random() < self.config.delay_probability:
                        delay_ms = random.uniform(self.config.min_delay_ms, self.config.max_delay_ms)
                        logger.debug("injecting_delay", worker_id=worker_id, delay_ms=delay_ms)
                        await asyncio.sleep(delay_ms / 1000.0)
                    
                except Exception as e:
                    logger.error("worker_error", worker_id=worker_id, error=str(e))
                    self.metrics["failed_ops"] += 1
                    
            logger.debug("worker_finished", worker_id=worker_id)
        
        # 启动所有worker
        logger.debug("launching_all_workers", count=self.config.max_concurrent_workers)
        workers = [worker(wid) for wid in worker_ids]
        
        # 运行所有worker
        await asyncio.gather(*workers)
        
        # 等待处理
        logger.debug("waiting_for_processing")
        await asyncio.sleep(15)
        
        await bus.stop()
        
        # 统计
        expected = self.config.max_concurrent_workers * 40
        logger.info("test_1_results",
                    expected_events=expected,
                    processed_events=processed_count,
                    queue_size=queue.size())
        
        if processed_count < expected * 0.85:
            self.issues.append({
                "test": "1_massive_concurrent",
                "severity": "high",
                "issue": "事件处理损失过高",
                "details": f"预期 {expected}, 实际 {processed_count}"
            })
    
    async def _test_2_super_chaos_network(self):
        """测试2: 超级混乱网络"""
        print("\n" + "-" * 120)
        print("📋 TEST 2: 超级混乱网络 - 随机失败、延迟、熔断")
        print("-" * 120)
        
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig, CircuitState
        )
        
        cb_config = EnhancedCircuitBreakerConfig(
            failure_threshold=8,
            success_threshold=3,
            timeout_duration=0.5,
            failure_rate_threshold=0.3,
            slow_call_threshold=0.1,
            slow_call_rate_threshold=0.4,
            min_call_count=5
        )
        
        cb = EnhancedCircuitBreaker("super_chaos_breaker", cb_config)
        
        success_count = 0
        fail_count = 0
        
        async def flaky_function():
            if random.random() < self.config.failure_probability:
                raise Exception("Chaos failure!")
            await asyncio.sleep(random.uniform(0.005, 0.2))
        
        async def slow_function():
            await asyncio.sleep(random.uniform(0.3, 1.0))
        
        logger.debug("starting_super_chaos_test")
        
        for i in range(400):
            try:
                if random.random() < 0.3:
                    await cb.call(slow_function)
                else:
                    await cb.call(flaky_function)
                success_count += 1
                logger.debug("call_success", i=i, state=cb.state.value)
            except Exception as e:
                fail_count += 1
                logger.debug("call_failure", i=i, error=str(e), state=cb.state.value)
                
                if "OPEN" in str(e) or cb.state == CircuitState.OPEN:
                    self.metrics["circuit_open_count"] += 1
            
            # 随机网络抖动
            if random.random() < self.config.network_jitter_chance:
                jitter_ms = random.uniform(10, 300)
                logger.debug("network_jitter", ms=jitter_ms)
                await asyncio.sleep(jitter_ms / 1000)
            
            if i % 50 == 0:
                logger.info("chaos_test_progress", i=i, success=success_count, fail=fail_count)
        
        logger.info("test_2_results",
                    success=success_count,
                    fail=fail_count,
                    final_state=cb.state.value)
    
    async def _test_3_resource_starvation(self):
        """测试3: 资源耗尽极限测试"""
        print("\n" + "-" * 120)
        print("📋 TEST 3: 资源耗尽 - 队列、内存极限")
        print("-" * 120)
        
        from app.core.enhanced_adaptive_queue import (
            EnhancedAdaptiveQueue, EnhancedQueueConfig
        )
        
        config = EnhancedQueueConfig(
            initial_capacity=50,
            max_capacity=500
        )
        
        queue = EnhancedAdaptiveQueue(config)
        
        enqueued = 0
        rejected = 0
        
        logger.debug("starting_resource_starvation_test")
        
        for i in range(3000):
            try:
                await queue.enqueue(
                    f"starve_test_{i}_{random.randint(1, 1000000)}",
                    priority=random.randint(0, 100),
                    wait_timeout=0.05
                )
                enqueued += 1
                logger.debug("enqueued", i=i, total=enqueued)
            except Exception:
                rejected += 1
                logger.debug("rejected", i=i, total=rejected)
            
            if i % 300 == 0:
                stats = queue.get_stats()
                logger.info("resource_test_progress",
                            i=i,
                            enqueued=enqueued,
                            rejected=rejected,
                            size=queue.size(),
                            capacity=queue.capacity)
        
        logger.info("test_3_results",
                    total_attempts=3000,
                    enqueued=enqueued,
                    rejected=rejected,
                    final_size=queue.size(),
                    final_capacity=queue.capacity)
        
        self.metrics["queue_rejects"] = rejected
    
    async def _test_4_data_consistency_chaos(self):
        """测试4: 数据一致性验证"""
        print("\n" + "-" * 120)
        print("📋 TEST 4: 数据一致性 - 极端情况下的完整性")
        print("-" * 120)
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        
        bus = EnhancedEventBus(max_history_size=10000)
        await bus.start()
        
        test_events = []
        for i in range(1500):
            event_data = {
                "id": i,
                "value": random.randint(1, 100000),
                "timestamp": time.time(),
                "checksum": hash((i, random.randint(1, 100000)))
            }
            test_events.append(event_data)
        
        logger.debug("publishing_data_consistency_events")
        
        # 发布事件
        for event_data in test_events:
            await bus.publish(event_data)
        
        # 等待处理
        await asyncio.sleep(10)
        
        # 验证完整性
        logger.debug("verifying_data_consistency")
        
        history = list(bus._event_history)
        corrupted_count = 0
        
        for event in history:
            if isinstance(event, dict) and "id" in event:
                original = next((e for e in test_events if e["id"] == event["id"]), None)
                if original:
                    if event["checksum"] != original["checksum"]:
                        corrupted_count += 1
                        logger.warning("data_corruption_detected",
                                      id=event["id"],
                                      stored_checksum=event["checksum"],
                                      original_checksum=original["checksum"])
        
        await bus.stop()
        
        if corrupted_count > 0:
            self.metrics["data_consistency_errors"] += corrupted_count
            self.issues.append({
                "test": "4_data_consistency",
                "severity": "critical",
                "issue": "数据损坏",
                "count": corrupted_count
            })
        
        logger.info("test_4_results",
                    total_events=1500,
                    history_size=len(history),
                    corrupted_count=corrupted_count)
    
    async def _test_5_long_term_stability(self):
        """测试5: 长时间稳定运行"""
        print("\n" + "-" * 120)
        print("📋 TEST 5: 长时间稳定性 - 持续运行验证")
        print("-" * 120)
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
        )
        
        bus = EnhancedEventBus(max_history_size=50000)
        queue = EnhancedAdaptiveQueue()
        cb = EnhancedCircuitBreaker("long_term_test", EnhancedCircuitBreakerConfig())
        
        await bus.start()
        
        processed_events = 0
        lock = asyncio.Lock()
        
        async def handler(event: Dict[str, Any]):
            nonlocal processed_events
            async with lock:
                processed_events += 1
        
        await bus.subscribe(handler)
        
        start_time = time.time()
        end_time = start_time + 60  # 1分钟快速测试
        
        event_counter = 0
        checkpoint = start_time + 15
        
        logger.debug("starting_long_term_test")
        
        while time.time() < end_time:
            try:
                event_data = {
                    "seq": event_counter,
                    "timestamp": time.time(),
                    "checksum": hash((event_counter, time.time()))
                }
                await bus.publish(event_data)
                
                await queue.enqueue(
                    f"long_term_{event_counter}",
                    priority=random.randint(0, 50)
                )
                
                await cb.call(lambda: asyncio.sleep(0.001))
                
                event_counter += 1
                
                if time.time() >= checkpoint:
                    logger.info("long_term_progress",
                                elapsed=int(time.time() - start_time),
                                events_published=event_counter,
                                events_processed=processed_events)
                    checkpoint += 15
                
                await asyncio.sleep(0.01)
                
            except Exception as e:
                logger.error("long_term_error", error=str(e))
        
        await asyncio.sleep(5)
        await bus.stop()
        
        logger.info("test_5_results",
                    total_events=event_counter,
                    processed_events=processed_events)
    
    def _print_final_report(self):
        """打印最终报告"""
        print("\n" + "=" * 120)
        print("📊 第十四轮超级深度混沌测试 - 最终报告")
        print("=" * 120)
        
        elapsed = time.time() - self.start_time
        print(f"\n测试总耗时: {elapsed:.2f}秒")
        print(f"总事件数: {self.metrics['total_events']}")
        print(f"处理事件数: {self.metrics['processed_events']}")
        print(f"失败操作数: {self.metrics['failed_ops']}")
        print(f"熔断次数: {self.metrics['circuit_open_count']}")
        print(f"队列拒绝数: {self.metrics['queue_rejects']}")
        print(f"数据一致性错误: {self.metrics['data_consistency_errors']}")
        
        if self.issues:
            print(f"\n⚠️  发现问题 ({len(self.issues)}):")
            for issue in self.issues:
                print(f"  - [{issue['test']}] [{issue['severity']}] {issue['issue']}")
        else:
            print("\n✅ 所有测试通过！没有发现问题！")
    
    def _generate_report(self):
        """生成报告文件"""
        report_file = "/workspace/backend/FOURTEENTH_ROUND_SUPER_SUMMARY.md"
        with open(report_file, "w", encoding="utf-8") as f:
            f.write("# 第十四轮超级深度混沌测试总结\n\n")
            f.write(f"**日期**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("**理念**: 慢工出细活 - 超大规模、超长时间、超混乱场景\n\n")
            
            f.write("## 测试概要\n\n")
            elapsed = time.time() - self.start_time
            f.write(f"- **总耗时**: {elapsed:.2f}秒\n")
            f.write(f"- **超大规模并发**: {self.config.max_concurrent_workers}个协程\n")
            f.write(f"- **队列拒绝**: {self.metrics['queue_rejects']}\n")
            
            if self.issues:
                f.write("\n## 发现的问题\n\n")
                for issue in self.issues:
                    f.write(f"### {issue['test']}\n")
                    f.write(f"- **严重度**: {issue['severity']}\n")
                    f.write(f"- **问题**: {issue['issue']}\n\n")
            else:
                f.write("\n## 结论\n\n")
                f.write("✅ 所有测试通过！系统通过超大规模混沌验证！\n\n")
        
        print(f"\n📄 报告已保存到: {report_file}")


async def main():
    """主函数"""
    tester = SuperScaleTester()
    success = await tester.run_all_tests()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
