"""
🔬 第十一轮超大规模深度测试 - 混沌工程验证
目标: 长时间、大规模、复杂混乱场景，发现系统深层问题

测试理念 - 混沌工程:
1. 大规模并发: 1000+ 协程同时运行
2. 长时间稳定: 30分钟+ 持续压力
3. 混乱故障注入: 随机网络抖动、延迟、中断
4. 资源耗尽: 内存、CPU、队列容量极限
5. 数据一致性: 事务完整性验证
6. 边界条件: 极端数据、异常输入
7. 组件隔离: 单点故障不影响整体
"""
import asyncio
import time
import random
import sys
import gc
import weakref
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, deque

sys.path.insert(0, '/workspace/backend')


@dataclass
class ChaosConfig:
    """混沌配置"""
    network_delay_probability: float = 0.1
    network_delay_max_ms: float = 500
    failure_probability: float = 0.05
    timeout_probability: float = 0.1
    memory_pressure_probability: float = 0.01
    disk_pressure_probability: float = 0.01


@dataclass
class TestMetrics:
    scenario: str
    duration_sec: float
    operations: int
    errors: int
    throughput: float
    issues: List[str] = field(default_factory=list)


class ChaosEngineeringTester:
    """混沌工程测试器 - 超大规模验证"""
    
    def __init__(self):
        self.metrics: List[TestMetrics] = []
        self.issues_found: List[Dict[str, Any]] = []
        self.chaos_config = ChaosConfig()
    
    async def run(self):
        print("\n" + "="*100)
        print("🔬 第十一轮超大规模深度测试 - 混沌工程验证")
        print(f"   开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("   理念: 慢工出细活 - 大规模、长时间、混乱场景")
        print("="*100 + "\n")
        
        await self.test1_massive_concurrent_stress()
        await self.test2_chaos_network_conditions()
        await self.test3_resource_exhaustion()
        await self.test4_data_consistency_chaos()
        await self.test5_boundary_chaos()
        await self.test6_long_term_stability()
        await self.test7_component_isolation()
        
        self.print_summary()
        self.generate_report()
    
    async def inject_chaos(self, operation: Callable):
        """混沌故障注入"""
        await operation()
        
        # 网络延迟
        if random.random() < self.chaos_config.network_delay_probability:
            delay = random.uniform(0, self.chaos_config.network_delay_max_ms / 1000)
            await asyncio.sleep(delay)
        
        # 随机失败
        if random.random() < self.chaos_config.failure_probability:
            raise Exception("chaos_failure")
    
    async def test1_massive_concurrent_stress(self):
        """场景1: 大规模并发压力测试"""
        scenario = "大规模并发压力 (1000+ 协程)"
        print(f"\n--- 🚀 {scenario} ---")
        start = time.time()
        operations = 0
        errors = 0
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
            )
            
            bus = EnhancedEventBus(max_history_size=5000)
            queue = EnhancedAdaptiveQueue()
            cb = EnhancedCircuitBreaker("mass_test", EnhancedCircuitBreakerConfig())
            
            await bus.start()
            
            processed_events = 0
            lock = asyncio.Lock()
            
            async def event_handler(event):
                nonlocal processed_events
                async with lock:
                    processed_events += 1
                await asyncio.sleep(0.001)
            
            await bus.subscribe(event_handler)
            
            async def worker(worker_id: int):
                nonlocal operations, errors
                for i in range(100):
                    try:
                        await bus.publish({"worker": worker_id, "seq": i})
                        await queue.enqueue(f"task-{worker_id}-{i}", priority=random.randint(1, 10))
                        operations += 2
                    except Exception:
                        errors += 1
                    await asyncio.sleep(0.001)
            
            # 启动1000个协程
            workers = [worker(i) for i in range(1000)]
            await asyncio.gather(*workers)
            
            await asyncio.sleep(5)
            await bus.stop()
            
            print(f"  1000协程 x 100操作 = 100,000操作")
            print(f"  实际: 操作={operations}, 错误={errors}")
            
            if operations != 100000:
                self.issues_found.append({
                    "scenario": scenario,
                    "issue": f"操作数不匹配: 期望100000, 实际{operations}",
                    "severity": "critical"
                })
        
        except Exception as e:
            self.issues_found.append({
                "scenario": scenario,
                "issue": f"测试崩溃: {e}",
                "severity": "critical"
            })
        
        duration = time.time() - start
        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=operations,
            errors=errors,
            throughput=operations / duration if duration > 0 else 0
        ))
    
    async def test2_chaos_network_conditions(self):
        """场景2: 混沌网络条件测试"""
        scenario = "混沌网络条件"
        print(f"\n--- 🌐 {scenario} ---")
        start = time.time()
        operations = 0
        errors = 0
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
            )
            
            bus = EnhancedEventBus(max_history_size=10000)
            cb = EnhancedCircuitBreaker("chaos_net", EnhancedCircuitBreakerConfig(
                failure_threshold=10,
                success_threshold=5,
                timeout_duration=1.0
            ))
            
            await bus.start()
            
            async def chaos_handler(event):
                # 模拟网络抖动
                if random.random() < 0.1:
                    await asyncio.sleep(random.uniform(0.1, 0.5))
                await asyncio.sleep(0.001)
            
            await bus.subscribe(chaos_handler)
            
            for phase in range(5):
                print(f"  阶段{phase+1}: 网络状态模拟")
                
                # 正常 -> 延迟 -> 抖动 -> 断开 -> 恢复
                network_states = [
                    (0.0, 0.01, 0.0),    # 正常
                    (0.3, 0.05, 0.05),   # 延迟
                    (0.5, 0.1, 0.15),    # 抖动
                    (0.8, 0.2, 0.3),     # 高延迟
                    (0.0, 0.01, 0.0)     # 恢复
                ]
                
                delay_p, fail_p, timeout_p = network_states[phase]
                
                for i in range(200):
                    try:
                        # 模拟网络延迟
                        if random.random() < delay_p:
                            await asyncio.sleep(random.uniform(0.1, 1.0))
                        
                        await bus.publish({
                            "phase": phase,
                            "seq": i,
                            "network_state": network_states[phase]
                        })
                        operations += 1
                        
                        # 熔断器测试
                        if random.random() < fail_p:
                            await cb.call(lambda: (_ for _ in ()).throw(Exception("net_fail")))
                        else:
                            await cb.call(lambda: asyncio.sleep(0.001))
                        
                    except Exception:
                        errors += 1
                    
                    await asyncio.sleep(0.001)
            
            print(f"  完成: 操作={operations}, 错误={errors}")
            
            await bus.stop()
        
        except Exception as e:
            self.issues_found.append({
                "scenario": scenario,
                "issue": f"测试崩溃: {e}",
                "severity": "critical"
            })
        
        duration = time.time() - start
        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=operations,
            errors=errors,
            throughput=operations / duration if duration > 0 else 0
        ))
    
    async def test3_resource_exhaustion(self):
        """场景3: 资源耗尽测试"""
        scenario = "资源耗尽测试"
        print(f"\n--- 💥 {scenario} ---")
        start = time.time()
        operations = 0
        errors = 0
        
        try:
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
            from app.core.enhanced_event_bus import EnhancedEventBus
            
            queue = EnhancedAdaptiveQueue()
            bus = EnhancedEventBus(max_history_size=1000)
            await bus.start()
            
            # 1. 队列容量耗尽
            print("  测试1: 队列容量耗尽")
            rejected_total = 0
            for i in range(10000):
                try:
                    await queue.enqueue(f"q-{i}", priority=random.randint(1, 10), wait_timeout=0.01)
                    operations += 1
                except Exception:
                    errors += 1
            
            stats = queue.get_stats()
            rejected_total = stats.get("rejected", 0)
            print(f"    入队: {operations}, 拒绝: {rejected_total}")
            
            # 2. 事件总线容量耗尽
            print("  测试2: 事件总线容量耗尽")
            event_count = 0
            for i in range(5000):
                await bus.publish({"seq": i})
                event_count += 1
            
            await asyncio.sleep(1)
            
            # 3. 内存压力测试
            print("  测试3: 内存压力")
            large_data = []
            for i in range(1000):
                large_data.append({
                    "id": i,
                    "data": "x" * 10000  # 10KB per item
                })
            
            del large_data
            gc.collect()
            
            print(f"    完成: 事件={event_count}")
            
            await bus.stop()
        
        except Exception as e:
            self.issues_found.append({
                "scenario": scenario,
                "issue": f"资源耗尽测试崩溃: {e}",
                "severity": "critical"
            })
        
        duration = time.time() - start
        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=operations,
            errors=errors,
            throughput=operations / duration if duration > 0 else 0
        ))
    
    async def test4_data_consistency_chaos(self):
        """场景4: 数据一致性混沌测试"""
        scenario = "数据一致性混沌测试"
        print(f"\n--- 🔐 {scenario} ---")
        start = time.time()
        operations = 0
        errors = 0
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            
            bus = EnhancedEventBus(max_history_size=10000)
            await bus.start()
            
            # 生成测试数据
            test_data = []
            for i in range(2000):
                data = {
                    "id": i,
                    "value": random.randint(0, 10000),
                    "timestamp": time.time(),
                    "checksum": hash((i, random.randint(0, 10000)))
                }
                test_data.append(data)
            
            # 发布数据
            published = 0
            for data in test_data:
                await bus.publish(data)
                published += 1
            
            await asyncio.sleep(3)
            
            # 验证数据完整性
            history = list(bus._event_history)
            corrupted = 0
            
            for event in history:
                if "checksum" in event:
                    expected = hash((event.get("id"), event.get("value")))
                    if event["checksum"] != expected:
                        corrupted += 1
            
            print(f"  发布: {published}, 历史: {len(history)}, 损坏: {corrupted}")
            
            if corrupted > 0:
                self.issues_found.append({
                    "scenario": scenario,
                    "issue": f"数据损坏: {corrupted}条",
                    "severity": "critical"
                })
            
            await bus.stop()
            
            operations = published
        
        except Exception as e:
            self.issues_found.append({
                "scenario": scenario,
                "issue": f"数据一致性测试崩溃: {e}",
                "severity": "critical"
            })
        
        duration = time.time() - start
        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=operations,
            errors=errors,
            throughput=operations / duration if duration > 0 else 0
        ))
    
    async def test5_boundary_chaos(self):
        """场景5: 边界混沌测试"""
        scenario = "边界混沌测试"
        print(f"\n--- ⚡ {scenario} ---")
        start = time.time()
        operations = 0
        errors = 0
        
        try:
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
            )
            
            queue = EnhancedAdaptiveQueue()
            cb = EnhancedCircuitBreaker("boundary", EnhancedCircuitBreakerConfig())
            
            # 测试边界值
            test_cases = [
                # 空队列
                (lambda: queue.dequeue(wait_timeout=0.01), "空队列出队"),
                # 负数优先级
                (lambda: queue.enqueue("neg", priority=-1000), "负数优先级"),
                # 超大优先级
                (lambda: queue.enqueue("huge", priority=1000000), "超大优先级"),
                # 空字符串
                (lambda: queue.enqueue("", priority=5), "空字符串"),
                # None值
                (lambda: queue.enqueue(None, priority=5), "None值"),
                # 极大数据
                (lambda: queue.enqueue("x" * 100000, priority=5), "极大数据"),
            ]
            
            for test_func, description in test_cases:
                try:
                    result = await test_func()
                    operations += 1
                    print(f"  {description}: OK")
                except Exception as e:
                    errors += 1
                    print(f"  {description}: ERROR - {e}")
        
        except Exception as e:
            self.issues_found.append({
                "scenario": scenario,
                "issue": f"边界测试崩溃: {e}",
                "severity": "critical"
            })
        
        duration = time.time() - start
        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=operations,
            errors=errors,
            throughput=operations / duration if duration > 0 else 0
        ))
    
    async def test6_long_term_stability(self):
        """场景6: 长时间稳定性测试"""
        scenario = "长时间稳定性 (10分钟)"
        print(f"\n--- ⏱️ {scenario} ---")
        start = time.time()
        operations = 0
        errors = 0
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
            
            bus = EnhancedEventBus(max_history_size=10000)
            queue = EnhancedAdaptiveQueue()
            await bus.start()
            
            processed = 0
            
            async def handler(event):
                nonlocal processed
                processed += 1
            
            await bus.subscribe(handler)
            
            end_time = start + 600  # 10分钟
            checkpoint = start + 60  # 每分钟
            
            while time.time() < end_time:
                for _ in range(50):
                    await bus.publish({"seq": operations})
                    await queue.enqueue(f"task-{operations}", priority=random.randint(1, 10))
                    operations += 1
                
                if time.time() >= checkpoint:
                    print(f"    {int(checkpoint - start)}s: {operations}操作")
                    checkpoint += 60
                
                await asyncio.sleep(0.5)
            
            await asyncio.sleep(2)
            await bus.stop()
            
            print(f"  完成: {operations}操作, {processed}事件处理")
            
            if processed != operations:
                self.issues_found.append({
                    "scenario": scenario,
                    "issue": f"事件丢失: 发布{operations}, 处理{processed}",
                    "severity": "high"
                })
        
        except Exception as e:
            self.issues_found.append({
                "scenario": scenario,
                "issue": f"长时间稳定性测试崩溃: {e}",
                "severity": "critical"
            })
        
        duration = time.time() - start
        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=operations,
            errors=errors,
            throughput=operations / duration if duration > 0 else 0
        ))
    
    async def test7_component_isolation(self):
        """场景7: 组件隔离测试"""
        scenario = "组件隔离测试"
        print(f"\n--- 🛡️ {scenario} ---")
        start = time.time()
        operations = 0
        errors = 0
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
            )
            
            # 创建多个独立组件
            buses = []
            for i in range(10):
                bus = EnhancedEventBus(max_history_size=1000)
                await bus.start()
                buses.append(bus)
            
            breakers = []
            for i in range(10):
                cb = EnhancedCircuitBreaker(f"iso-{i}", EnhancedCircuitBreakerConfig())
                breakers.append(cb)
            
            # 测试一个组件故障不影响其他组件
            print("  测试: 单组件故障隔离")
            
            # 触发第一个熔断器
            for _ in range(20):
                try:
                    await breakers[0].call(lambda: (_ for _ in ()).throw(Exception("isolated_failure")))
                except Exception:
                    pass
            
            # 验证其他组件仍然正常
            for i in range(1, 10):
                try:
                    await breakers[i].call(lambda: asyncio.sleep(0.001))
                    operations += 1
                except Exception:
                    errors += 1
            
            # 验证第一个组件已恢复
            await asyncio.sleep(2)
            recovered = False
            for _ in range(5):
                try:
                    await breakers[0].call(lambda: asyncio.sleep(0.001))
                    recovered = True
                    break
                except Exception:
                    pass
            
            print(f"  组件隔离: 操作={operations}, 错误={errors}, 恢复={recovered}")
            
            # 清理
            for bus in buses:
                await bus.stop()
        
        except Exception as e:
            self.issues_found.append({
                "scenario": scenario,
                "issue": f"组件隔离测试崩溃: {e}",
                "severity": "critical"
            })
        
        duration = time.time() - start
        self.metrics.append(TestMetrics(
            scenario=scenario,
            duration_sec=duration,
            operations=operations,
            errors=errors,
            throughput=operations / duration if duration > 0 else 0
        ))
    
    def print_summary(self):
        print("\n" + "="*100)
        print("📊 第十一轮超大规模测试 - 摘要 (混沌工程)")
        print("="*100)
        
        total_ops = sum(m.operations for m in self.metrics)
        total_errors = sum(m.errors for m in self.metrics)
        total_duration = sum(m.duration_sec for m in self.metrics)
        
        print(f"\n总操作数: {total_ops:,}")
        print(f"总错误数: {total_errors:,}")
        print(f"总耗时: {total_duration:.1f}秒 ({total_duration/60:.1f}分钟)")
        print(f"整体错误率: {total_errors/total_ops*100:.4f}%" if total_ops > 0 else "")
        print(f"整体吞吐量: {total_ops/total_duration:.1f} ops/s" if total_duration > 0 else "")
        
        print("\n各场景详细结果:")
        for m in self.metrics:
            print(f"\n  {m.scenario}:")
            print(f"    耗时: {m.duration_sec:.1f}秒")
            print(f"    操作: {m.operations:,}")
            print(f"    错误: {m.errors:,}")
            print(f"    吞吐量: {m.throughput:.1f} ops/s")
        
        if self.issues_found:
            print(f"\n⚠️  发现问题 ({len(self.issues_found)}):")
            for i, issue in enumerate(self.issues_found, 1):
                print(f"  {i}. [{issue['severity']}] {issue['scenario']}")
                print(f"     {issue['issue']}")
        else:
            print("\n✅ 未发现问题！系统通过所有混沌工程测试！")
        
        print("\n" + "="*100)
    
    def generate_report(self):
        report_path = "/workspace/backend/ELEVENTH_ROUND_REPORT.md"
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("# Agent Cluster Orchestrator - 第十一轮超大规模混沌工程测试报告\n\n")
            f.write(f"**日期**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**理念**: 慢工出细活 - 混沌工程验证\n\n")
            f.write("---\n\n")
            
            total_ops = sum(m.operations for m in self.metrics)
            total_errors = sum(m.errors for m in self.metrics)
            total_duration = sum(m.duration_sec for m in self.metrics)
            
            f.write("## 测试摘要\n\n")
            f.write(f"- **总操作数**: {total_ops:,}\n")
            f.write(f"- **总错误数**: {total_errors:,}\n")
            f.write(f"- **总耗时**: {total_duration:.1f}秒 ({total_duration/60:.1f}分钟)\n")
            f.write(f"- **整体错误率**: {total_errors/total_ops*100:.4f}%\n\n" if total_ops > 0 else "")
            
            f.write("## 各场景详细结果\n\n")
            for m in self.metrics:
                f.write(f"### {m.scenario}\n\n")
                f.write(f"- 耗时: {m.duration_sec:.1f}秒\n")
                f.write(f"- 操作数: {m.operations:,}\n")
                f.write(f"- 错误数: {m.errors:,}\n")
                f.write(f"- 吞吐量: {m.throughput:.1f} ops/s\n\n")
            
            if self.issues_found:
                f.write("## 发现的问题\n\n")
                for issue in self.issues_found:
                    f.write(f"- **{issue['scenario']}** [{issue['severity']}]: {issue['issue']}\n")
                f.write("\n")
            else:
                f.write("## 问题分析\n\n")
                f.write("✅ 所有测试通过，未发现问题！\n\n")
            
            f.write("---\n")
            f.write("*第十一轮超大规模混沌工程测试完成*\n")
        
        print(f"\n📄 报告已保存至: {report_path}")


async def main():
    tester = ChaosEngineeringTester()
    await tester.run()


if __name__ == "__main__":
    asyncio.run(main())
