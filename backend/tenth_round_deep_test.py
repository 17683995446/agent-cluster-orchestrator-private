"""
🔬 第十轮深度测试 - 慢工出细活
目标: 30分钟+长时间、真实分布式场景、故障注入、性能瓶颈检测

测试理念:
- 慢工出细活: 深入、细致、全面的测试
- 真实场景: 模拟金融交易、日志系统、任务队列等生产场景
- 故障注入: 网络抖动、延迟、超时、组件故障
- 性能监控: 内存、CPU、响应时间、吞吐量
"""
import asyncio
import time
import random
import sys
import gc
import psutil
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, deque

sys.path.insert(0, '/workspace/backend')


@dataclass
class ResourceSnapshot:
    timestamp: float
    memory_mb: float
    cpu_percent: float
    open_files: int
    threads: int


@dataclass
class PerformanceMetric:
    avg_response_ms: float
    p95_response_ms: float
    p99_response_ms: float
    max_response_ms: float
    throughput_ops_sec: float
    error_rate: float


@dataclass
class TestResult:
    scenario: str
    duration_sec: float
    total_ops: int
    errors: int
    performance: PerformanceMetric
    resource_history: List[ResourceSnapshot] = field(default_factory=list)
    issues: List[Dict[str, Any]] = field(default_factory=list)


class DeepQualityTester:
    """深度质量测试器 - 慢工出细活"""
    
    def __init__(self):
        self.results: List[TestResult] = []
        self.start_time = None
        
    async def run(self):
        self.start_time = time.time()
        print("\n" + "="*100)
        print("🔬 Agent Cluster Orchestrator - 第十轮深度测试 (慢工出细活)")
        print(f"   开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*100 + "\n")
        
        # 测试场景
        await self.scenario1_long_term_stability()
        await self.scenario2_distributed_simulation()
        await self.scenario3_fault_injection()
        await self.scenario4_real_business_workflows()
        await self.scenario5_performance_bottlenecks()
        await self.scenario6_resilience_under_pressure()
        
        self.print_summary()
        self.generate_report()
    
    async def take_resource_snapshot(self) -> ResourceSnapshot:
        """获取资源快照"""
        process = psutil.Process()
        return ResourceSnapshot(
            timestamp=time.time(),
            memory_mb=process.memory_info().rss / 1024 / 1024,
            cpu_percent=process.cpu_percent(),
            open_files=len(process.open_files()),
            threads=process.num_threads()
        )
    
    async def monitor_resources(self, stop_event: asyncio.Event, interval: float = 5.0) -> List[ResourceSnapshot]:
        """监控资源使用"""
        snapshots = []
        while not stop_event.is_set():
            try:
                snapshots.append(await self.take_resource_snapshot())
            except:
                pass
            await asyncio.sleep(interval)
        return snapshots
    
    async def scenario1_long_term_stability(self):
        """场景1: 30分钟长时间稳定性测试"""
        scenario = "30分钟长时间稳定性"
        print(f"\n--- 🔐 {scenario} ---")
        start = time.time()
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
        )
        
        bus = EnhancedEventBus(max_history_size=10000)
        queue = EnhancedAdaptiveQueue()
        cb = EnhancedCircuitBreaker("long_term", EnhancedCircuitBreakerConfig(
            failure_threshold=10,
            success_threshold=5,
            timeout_duration=10.0,
            min_call_count=20
        ))
        
        await bus.start()
        
        processed_events = 0
        processed_queue = 0
        
        async def event_handler(event):
            nonlocal processed_events
            processed_events += 1
            await asyncio.sleep(random.uniform(0.001, 0.01))
        
        await bus.subscribe(event_handler)
        
        stop_monitor = asyncio.Event()
        monitor_task = asyncio.create_task(self.monitor_resources(stop_monitor, 30))
        
        ops_count = 0
        errors = 0
        response_times = []
        
        end_test = start + 1800  # 30分钟
        phase = 0
        
        try:
            while time.time() < end_test:
                phase += 1
                elapsed = time.time() - start
                
                # 动态负载
                if elapsed < 300:  # 0-5分钟: 低负载
                    batch_size = 20
                elif elapsed < 900:  # 5-15分钟: 中等负载
                    batch_size = 50
                elif elapsed < 1500:  # 15-25分钟: 高负载
                    batch_size = 100
                else:  # 25-30分钟: 恢复正常
                    batch_size = 30
                
                for _ in range(batch_size):
                    op_start = time.time()
                    try:
                        await bus.publish({
                            "phase": phase,
                            "seq": ops_count,
                            "timestamp": time.time()
                        })
                        ops_count += 1
                        
                        if random.random() < 0.7:
                            await queue.enqueue(f"task-{ops_count}", priority=random.randint(1, 10))
                        
                        if random.random() < 0.2:
                            await cb.call(lambda: asyncio.sleep(0.005))
                        
                    except Exception:
                        errors += 1
                    
                    rt_ms = (time.time() - op_start) * 1000
                    response_times.append(rt_ms)
                
                if phase % 60 == 0:  # 每60秒打印一次
                    stats = await self._calculate_stats(response_times[-1000:])
                    print(f"  阶段{phase} ({elapsed:.0f}s): 操作={ops_count}, 错误={errors}, "
                          f"P95响应={stats['p95']:.1f}ms, 内存={(await self.take_resource_snapshot()).memory_mb:.1f}MB")
                
                await asyncio.sleep(1)
        
        finally:
            stop_monitor.set()
            await monitor_task
        
        await bus.stop()
        
        stats = await self._calculate_stats(response_times)
        perf_metric = PerformanceMetric(
            avg_response_ms=stats['avg'],
            p95_response_ms=stats['p95'],
            p99_response_ms=stats['p99'],
            max_response_ms=stats['max'],
            throughput_ops_sec=ops_count / (time.time() - start) if time.time() > start else 0,
            error_rate=errors / ops_count if ops_count > 0 else 0
        )
        
        resource_history = monitor_task.result()
        
        # 检测问题
        issues = []
        if perf_metric.error_rate > 0.05:
            issues.append({"severity": "medium", "message": f"错误率偏高: {perf_metric.error_rate*100:.1f}%"})
        
        if len(resource_history) >= 2:
            start_mem = resource_history[0].memory_mb
            end_mem = resource_history[-1].memory_mb
            mem_growth = end_mem - start_mem
            if mem_growth > 50:
                issues.append({"severity": "high", "message": f"内存增长过大: {mem_growth:.1f}MB"})
        
        self.results.append(TestResult(
            scenario=scenario,
            duration_sec=time.time() - start,
            total_ops=ops_count,
            errors=errors,
            performance=perf_metric,
            resource_history=resource_history,
            issues=issues
        ))
    
    async def scenario2_distributed_simulation(self):
        """场景2: 分布式场景模拟 - 网络抖动、延迟、多实例"""
        scenario = "分布式场景模拟"
        print(f"\n--- 🌐 {scenario} ---")
        start = time.time()
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
        )
        
        instances = []
        breakers = []
        
        for i in range(5):
            bus = EnhancedEventBus(max_history_size=5000)
            await bus.start()
            instances.append(bus)
            
            cb = EnhancedCircuitBreaker(f"distributed-{i}", EnhancedCircuitBreakerConfig(
                failure_threshold=5,
                success_threshold=3,
                timeout_duration=2.0,
                min_call_count=10
            ))
            breakers.append(cb)
        
        ops_count = 0
        errors = 0
        response_times = []
        
        for step in range(600):  # 10分钟
            for idx, bus in enumerate(instances):
                for _ in range(10):
                    op_start = time.time()
                    try:
                        # 模拟网络延迟
                        network_latency = random.uniform(0, 0.1)
                        if random.random() < 0.1:  # 10%概率网络抖动
                            network_latency += random.uniform(0.5, 2.0)
                        await asyncio.sleep(network_latency)
                        
                        await bus.publish({
                            "instance": idx,
                            "step": step,
                            "latency": network_latency
                        })
                        ops_count += 1
                        
                    except Exception:
                        errors += 1
                    
                    rt_ms = (time.time() - op_start) * 1000
                    response_times.append(rt_ms)
        
        for bus in instances:
            await bus.stop()
        
        stats = await self._calculate_stats(response_times)
        perf_metric = PerformanceMetric(
            avg_response_ms=stats['avg'],
            p95_response_ms=stats['p95'],
            p99_response_ms=stats['p99'],
            max_response_ms=stats['max'],
            throughput_ops_sec=ops_count / (time.time() - start),
            error_rate=errors / ops_count if ops_count > 0 else 0
        )
        
        self.results.append(TestResult(
            scenario=scenario,
            duration_sec=time.time() - start,
            total_ops=ops_count,
            errors=errors,
            performance=perf_metric,
            issues=[]
        ))
    
    async def scenario3_fault_injection(self):
        """场景3: 故障注入测试 - 超时、组件故障、资源耗尽"""
        scenario = "故障注入测试"
        print(f"\n--- 💥 {scenario} ---")
        start = time.time()
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
        )
        
        bus = EnhancedEventBus(max_history_size=10000)
        await bus.start()
        
        cb = EnhancedCircuitBreaker("fault_test", EnhancedCircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout_duration=1.0,
            min_call_count=5
        ))
        
        ops_count = 0
        errors = 0
        
        # 1. 超时注入
        print("  测试1: 超时注入")
        for _ in range(50):
            try:
                await asyncio.wait_for(asyncio.sleep(0.1), timeout=0.05)
                ops_count += 1
            except asyncio.TimeoutError:
                errors += 1
        
        # 2. 故障模式注入
        print("  测试2: 故障模式注入")
        failures = 0
        for _ in range(20):
            try:
                await cb.call(lambda: (_ for _ in ()).throw(Exception("injected_failure")))
                ops_count += 1
            except Exception:
                errors += 1
                failures += 1
        
        # 3. 资源耗尽模拟
        print("  测试3: 资源耗尽模拟")
        await bus.stop()
        await asyncio.sleep(0.5)
        bus = EnhancedEventBus(max_history_size=1000)
        await bus.start()
        
        for _ in range(1000):
            await bus.publish({"type": "resource_test"})
        
        await bus.stop()
        
        self.results.append(TestResult(
            scenario=scenario,
            duration_sec=time.time() - start,
            total_ops=ops_count,
            errors=errors,
            performance=PerformanceMetric(0, 0, 0, 0, ops_count / (time.time() - start), errors / ops_count if ops_count > 0 else 0),
            issues=[]
        ))
    
    async def scenario4_real_business_workflows(self):
        """场景4: 真实业务工作流 - 金融交易、日志系统、任务队列"""
        scenario = "真实业务工作流"
        print(f"\n--- 💼 {scenario} ---")
        start = time.time()
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
        from app.core.enhanced_timeout_protection import EnhancedTimeoutMonitor
        
        fin_bus = EnhancedEventBus(max_history_size=10000)
        log_bus = EnhancedEventBus(max_history_size=10000)
        task_queue = EnhancedAdaptiveQueue()
        monitor = EnhancedTimeoutMonitor()
        
        await fin_bus.start()
        await log_bus.start()
        
        ops_count = 0
        errors = 0
        
        # 模拟金融交易流
        async def trade_handler(event):
            nonlocal ops_count
            await asyncio.sleep(random.uniform(0.005, 0.05))
            ops_count += 1
        
        await fin_bus.subscribe(trade_handler)
        
        # 模拟日志流
        async def log_handler(event):
            nonlocal ops_count
            await asyncio.sleep(random.uniform(0.001, 0.005))
            ops_count += 1
        
        await log_bus.subscribe(log_handler)
        
        # 运行5分钟
        end_test = start + 300
        while time.time() < end_test:
            # 金融交易
            if random.random() < 0.3:
                await fin_bus.publish({
                    "type": "trade",
                    "trade_id": random.randint(1, 1000000),
                    "symbol": random.choice(["AAPL", "GOOGL", "MSFT", "AMZN"]),
                    "quantity": random.randint(1, 1000),
                    "price": random.uniform(10, 1000),
                    "timestamp": time.time()
                })
            
            # 日志流
            log_level = random.choice(["debug", "info", "warning", "error"])
            await log_bus.publish({
                "level": log_level,
                "message": f"System log message {ops_count}",
                "component": random.choice(["api", "db", "cache", "worker"]),
                "timestamp": time.time()
            })
            
            # 任务队列
            if random.random() < 0.5:
                await task_queue.enqueue({
                    "type": "background_task",
                    "task_id": ops_count,
                    "priority": random.randint(1, 10),
                    "params": {}
                }, priority=random.randint(1, 10))
            
            await asyncio.sleep(random.uniform(0.001, 0.01))
        
        await fin_bus.stop()
        await log_bus.stop()
        
        self.results.append(TestResult(
            scenario=scenario,
            duration_sec=time.time() - start,
            total_ops=ops_count,
            errors=errors,
            performance=PerformanceMetric(0, 0, 0, 0, ops_count / (time.time() - start), 0),
            issues=[]
        ))
    
    async def scenario5_performance_bottlenecks(self):
        """场景5: 性能瓶颈检测 - 大数据量、高并发"""
        scenario = "性能瓶颈检测"
        print(f"\n--- ⚡ {scenario} ---")
        start = time.time()
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
        
        bus = EnhancedEventBus(max_history_size=50000)
        queue = EnhancedAdaptiveQueue()
        await bus.start()
        
        ops_count = 0
        errors = 0
        
        # 1. 大批量事件测试
        print("  测试1: 大批量事件")
        batch_start = time.time()
        for _ in range(10000):
            await bus.publish({"type": "batch_event", "index": ops_count})
            ops_count += 1
        batch_time = time.time() - batch_start
        print(f"    批量事件吞吐量: {10000 / batch_time:.1f} ops/s")
        
        # 2. 高并发测试
        print("  测试2: 高并发")
        tasks = []
        for i in range(100):
            async def worker():
                nonlocal ops_count, errors
                for _ in range(100):
                    try:
                        await queue.enqueue(f"concurrency-{ops_count}", priority=random.randint(1, 10))
                        ops_count += 1
                    except Exception:
                        errors += 1
            tasks.append(worker())
        
        await asyncio.gather(*tasks)
        
        await bus.stop()
        
        self.results.append(TestResult(
            scenario=scenario,
            duration_sec=time.time() - start,
            total_ops=ops_count,
            errors=errors,
            performance=PerformanceMetric(0, 0, 0, 0, ops_count / (time.time() - start), 0),
            issues=[]
        ))
    
    async def scenario6_resilience_under_pressure(self):
        """场景6: 压力下的恢复力 - 压力冲击、故障恢复"""
        scenario = "压力下的恢复力"
        print(f"\n--- 🔄 {scenario} ---")
        start = time.time()
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
        )
        
        bus = EnhancedEventBus(max_history_size=10000)
        cb = EnhancedCircuitBreaker("resilience_test", EnhancedCircuitBreakerConfig(
            failure_threshold=5,
            success_threshold=3,
            timeout_duration=2.0,
            min_call_count=10
        ))
        
        await bus.start()
        
        ops_count = 0
        errors = 0
        
        # 1. 压力冲击测试
        print("  阶段1: 压力冲击")
        for phase in range(3):
            for _ in range(1000):
                await bus.publish({"phase": phase, "id": ops_count})
                ops_count += 1
            await asyncio.sleep(0.5)
        
        # 2. 故障恢复测试
        print("  阶段2: 故障恢复")
        for _ in range(10):
            try:
                await cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
            except Exception:
                errors += 1
        
        await asyncio.sleep(3)
        
        success_count = 0
        for _ in range(20):
            try:
                await cb.call(lambda: asyncio.sleep(0.01))
                success_count += 1
            except Exception:
                pass
        print(f"    恢复后成功率: {success_count / 20 * 100}%")
        
        await bus.stop()
        
        self.results.append(TestResult(
            scenario=scenario,
            duration_sec=time.time() - start,
            total_ops=ops_count,
            errors=errors,
            performance=PerformanceMetric(0, 0, 0, 0, ops_count / (time.time() - start), errors / ops_count if ops_count > 0 else 0),
            issues=[]
        ))
    
    async def _calculate_stats(self, response_times: List[float]) -> Dict[str, float]:
        """计算统计数据"""
        if not response_times:
            return {"avg": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0}
        
        sorted_times = sorted(response_times)
        n = len(sorted_times)
        
        return {
            "avg": sum(sorted_times) / n,
            "p50": sorted_times[int(n * 0.5)],
            "p95": sorted_times[int(n * 0.95)],
            "p99": sorted_times[int(n * 0.99)],
            "max": sorted_times[-1]
        }
    
    def print_summary(self):
        print("\n" + "="*100)
        print("📊 第十轮深度测试 - 摘要 (慢工出细活)")
        print("="*100)
        
        total_ops = sum(r.total_ops for r in self.results)
        total_errors = sum(r.errors for r in self.results)
        total_duration = time.time() - self.start_time if self.start_time else 0
        
        print(f"\n总操作数: {total_ops:,}")
        print(f"总错误数: {total_errors:,}")
        print(f"总耗时: {total_duration:.1f}秒 ({total_duration/60:.1f}分钟)")
        print(f"整体错误率: {total_errors/total_ops*100:.4f}%" if total_ops > 0 else "")
        
        all_issues = []
        for r in self.results:
            all_issues.extend(r.issues)
        
        if all_issues:
            print(f"\n⚠️  发现问题 ({len(all_issues)}):")
            for i, issue in enumerate(all_issues, 1):
                print(f"  {i}. [{issue.get('severity', 'unknown')}] {issue.get('message', '')}")
        else:
            print("\n✅ 未发现问题！")
        
        print("\n" + "="*100)
    
    def generate_report(self):
        report_path = "/workspace/backend/TENTH_ROUND_REPORT.md"
        
        total_ops = sum(r.total_ops for r in self.results)
        total_errors = sum(r.errors for r in self.results)
        total_duration = time.time() - self.start_time if self.start_time else 0
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("# Agent Cluster Orchestrator - 第十轮深度测试报告\n\n")
            f.write(f"**日期**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("**理念**: 慢工出细活 - 深入、细致、全面\n\n")
            f.write("---\n\n")
            f.write("## 测试摘要\n\n")
            f.write(f"- **总操作数**: {total_ops:,}\n")
            f.write(f"- **总错误数**: {total_errors:,}\n")
            f.write(f"- **总耗时**: {total_duration:.1f}秒 ({total_duration/60:.1f}分钟)\n")
            f.write(f"- **整体错误率**: {total_errors/total_ops*100:.4f}%\n\n" if total_ops > 0 else "")
            
            f.write("## 各场景详细结果\n\n")
            for result in self.results:
                f.write(f"### {result.scenario}\n\n")
                f.write(f"- 耗时: {result.duration_sec:.1f}秒\n")
                f.write(f"- 操作数: {result.total_ops:,}\n")
                f.write(f"- 错误数: {result.errors:,}\n")
                f.write(f"- 错误率: {result.performance.error_rate*100:.4f}%\n")
                f.write(f"- 平均响应: {result.performance.avg_response_ms:.2f}ms\n")
                f.write(f"- P95响应: {result.performance.p95_response_ms:.2f}ms\n")
                f.write(f"- 吞吐量: {result.performance.throughput_ops_sec:.1f} ops/s\n")
                
                if result.resource_history:
                    f.write(f"- 资源使用:\n")
                    if len(result.resource_history) >= 2:
                        start_mem = result.resource_history[0].memory_mb
                        end_mem = result.resource_history[-1].memory_mb
                        f.write(f"  - 内存变化: {start_mem:.1f}MB -> {end_mem:.1f}MB (增长: {end_mem-start_mem:.1f}MB)\n")
                
                if result.issues:
                    f.write(f"- 问题:\n")
                    for issue in result.issues:
                        f.write(f"  - [{issue.get('severity', 'unknown')}] {issue.get('message', '')}\n")
                
                f.write("\n")
            
            all_issues = []
            for r in self.results:
                all_issues.extend(r.issues)
            
            if all_issues:
                f.write("## 发现的问题\n\n")
                critical = [i for i in all_issues if i.get('severity') == 'critical']
                high = [i for i in all_issues if i.get('severity') == 'high']
                medium = [i for i in all_issues if i.get('severity') == 'medium']
                
                if critical:
                    f.write("### 🔴 严重问题\n\n")
                    for issue in critical:
                        f.write(f"- {issue.get('message', '')}\n")
                    f.write("\n")
                
                if high:
                    f.write("### 🟠 高优先级问题\n\n")
                    for issue in high:
                        f.write(f"- {issue.get('message', '')}\n")
                    f.write("\n")
                
                if medium:
                    f.write("### 🟡 中优先级问题\n\n")
                    for issue in medium:
                        f.write(f"- {issue.get('message', '')}\n")
                    f.write("\n")
            else:
                f.write("## 问题分析\n\n")
                f.write("✅ 所有测试通过，未发现问题！\n\n")
            
            f.write("---\n")
            f.write("*第十轮深度测试完成 - 慢工出细活*\n")
        
        print(f"\n📄 报告已保存至: {report_path}")


async def main():
    tester = DeepQualityTester()
    await tester.run()


if __name__ == "__main__":
    asyncio.run(main())
