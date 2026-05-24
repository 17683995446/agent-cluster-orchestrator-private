"""
🔬 第十轮深度测试 - 简化版
目标: 快速验证系统，发现潜在问题
"""
import asyncio
import time
import random
import sys
import psutil
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime

sys.path.insert(0, '/workspace/backend')


@dataclass
class ResourceSnapshot:
    timestamp: float
    memory_mb: float
    cpu_percent: float


@dataclass
class PerformanceMetric:
    avg_response_ms: float
    p95_response_ms: float
    throughput_ops_sec: float
    error_rate: float


@dataclass
class TestResult:
    scenario: str
    duration_sec: float
    total_ops: int
    errors: int
    performance: PerformanceMetric
    resource_history: List[ResourceSnapshot]
    issues: List[Dict[str, Any]]


class DeepQualityTester:
    """深度质量测试器 - 简化版"""
    
    def __init__(self):
        self.results: List[TestResult] = []
        self.start_time = None
    
    async def run(self):
        self.start_time = time.time()
        print("\n" + "="*80)
        print("🔬 Agent Cluster Orchestrator - 第十轮深度测试 (简化版)")
        print(f"   开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")
        
        await self.scenario1_basic_stability()
        await self.scenario2_network_simulation()
        await self.scenario3_fault_tolerance()
        await self.scenario4_real_business()
        await self.scenario5_performance_test()
        
        self.print_summary()
        self.generate_report()
    
    async def take_resource_snapshot(self) -> ResourceSnapshot:
        process = psutil.Process()
        return ResourceSnapshot(
            timestamp=time.time(),
            memory_mb=process.memory_info().rss / 1024 / 1024,
            cpu_percent=process.cpu_percent()
        )
    
    async def scenario1_basic_stability(self):
        """场景1: 基础稳定性测试"""
        scenario = "基础稳定性测试"
        print(f"\n--- 🔐 {scenario} ---")
        start = time.time()
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
        
        bus = EnhancedEventBus(max_history_size=5000)
        queue = EnhancedAdaptiveQueue()
        await bus.start()
        
        processed_events = 0
        
        async def event_handler(event):
            nonlocal processed_events
            processed_events += 1
            await asyncio.sleep(random.uniform(0.001, 0.01))
        
        await bus.subscribe(event_handler)
        
        ops_count = 0
        errors = 0
        response_times = []
        resource_history = []
        
        for step in range(200):
            resource_history.append(await self.take_resource_snapshot())
            
            for _ in range(10):
                op_start = time.time()
                try:
                    await bus.publish({"step": step, "seq": ops_count})
                    await queue.enqueue(f"task-{ops_count}", priority=random.randint(1, 10))
                    ops_count += 1
                except Exception:
                    errors += 1
                
                rt_ms = (time.time() - op_start) * 1000
                response_times.append(rt_ms)
            
            await asyncio.sleep(0.01)
        
        await asyncio.sleep(1)
        await bus.stop()
        
        avg_rt = sum(response_times) / len(response_times) if response_times else 0
        sorted_rt = sorted(response_times)
        p95_rt = sorted_rt[int(len(sorted_rt)*0.95)] if sorted_rt else 0
        
        issues = []
        if errors / ops_count > 0.01:
            issues.append({"severity": "medium", "message": f"错误率过高: {errors/ops_count*100:.1f}%"})
        
        if len(resource_history) >= 2:
            mem_growth = resource_history[-1].memory_mb - resource_history[0].memory_mb
            if mem_growth > 20:
                issues.append({"severity": "high", "message": f"内存增长: {mem_growth:.1f}MB"})
        
        perf = PerformanceMetric(
            avg_response_ms=avg_rt,
            p95_response_ms=p95_rt,
            throughput_ops_sec=ops_count / (time.time() - start),
            error_rate=errors / ops_count if ops_count else 0
        )
        
        print(f"  操作: {ops_count}, 处理: {processed_events}, 错误: {errors}, P95: {p95_rt:.1f}ms")
        
        self.results.append(TestResult(
            scenario=scenario,
            duration_sec=time.time()-start,
            total_ops=ops_count,
            errors=errors,
            performance=perf,
            resource_history=resource_history,
            issues=issues
        ))
    
    async def scenario2_network_simulation(self):
        """场景2: 网络模拟测试"""
        scenario = "网络模拟测试"
        print(f"\n--- 🌐 {scenario} ---")
        start = time.time()
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
        )
        
        bus = EnhancedEventBus(max_history_size=5000)
        cb = EnhancedCircuitBreaker("network", EnhancedCircuitBreakerConfig(
            failure_threshold=5,
            success_threshold=3,
            timeout_duration=1.0,
            min_call_count=10
        ))
        
        await bus.start()
        
        ops_count = 0
        errors = 0
        
        for step in range(100):
            for _ in range(20):
                try:
                    network_delay = random.uniform(0, 0.05)
                    if random.random() < 0.1:
                        network_delay += random.uniform(0.1, 1.0)
                    await asyncio.sleep(network_delay)
                    
                    await bus.publish({"type": "network_test", "step": step})
                    ops_count += 1
                    
                    if random.random() < 0.2:
                        await cb.call(lambda: asyncio.sleep(0.01))
                
                except Exception:
                    errors += 1
        
        await bus.stop()
        
        print(f"  操作: {ops_count}, 错误: {errors}")
        
        self.results.append(TestResult(
            scenario=scenario,
            duration_sec=time.time()-start,
            total_ops=ops_count,
            errors=errors,
            performance=PerformanceMetric(0, 0, ops_count / (time.time()-start), errors / ops_count if ops_count else 0),
            resource_history=[],
            issues=[]
        ))
    
    async def scenario3_fault_tolerance(self):
        """场景3: 容错测试"""
        scenario = "容错测试"
        print(f"\n--- 💥 {scenario} ---")
        start = time.time()
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
        )
        
        bus = EnhancedEventBus(max_history_size=2000)
        cb = EnhancedCircuitBreaker("fault", EnhancedCircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout_duration=0.5,
            min_call_count=5
        ))
        
        await bus.start()
        
        ops_count = 0
        errors = 0
        
        async def fail_func():
            raise Exception("injected failure")
        
        async def success_func():
            await asyncio.sleep(0.005)
        
        # 故障注入
        for _ in range(20):
            try:
                await cb.call(fail_func)
            except Exception:
                errors += 1
        
        await asyncio.sleep(1)
        
        # 恢复测试
        success_count = 0
        for _ in range(20):
            try:
                await cb.call(success_func)
                success_count += 1
            except Exception:
                pass
        
        for _ in range(100):
            await bus.publish({"type": "test"})
        
        await bus.stop()
        
        print(f"  恢复后成功率: {success_count / 20 * 100:.0f}%")
        
        self.results.append(TestResult(
            scenario=scenario,
            duration_sec=time.time()-start,
            total_ops=140,
            errors=errors,
            performance=PerformanceMetric(0, 0, 140 / (time.time()-start), errors / 140),
            resource_history=[],
            issues=[]
        ))
    
    async def scenario4_real_business(self):
        """场景4: 真实业务场景"""
        scenario = "真实业务场景"
        print(f"\n--- 💼 {scenario} ---")
        start = time.time()
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
        
        log_bus = EnhancedEventBus(max_history_size=5000)
        task_queue = EnhancedAdaptiveQueue()
        
        await log_bus.start()
        
        ops_count = 0
        errors = 0
        
        for step in range(100):
            for _ in range(20):
                await log_bus.publish({
                    "level": random.choice(["debug", "info", "warning", "error"]),
                    "component": random.choice(["api", "db", "worker"]),
                    "message": f"log {ops_count}"
                })
                
                await task_queue.enqueue(
                    {"type": "background", "id": ops_count},
                    priority=random.randint(1, 10)
                )
                
                ops_count += 1
        
        await log_bus.stop()
        
        print(f"  操作: {ops_count}, 队列大小: {task_queue.size()}")
        
        self.results.append(TestResult(
            scenario=scenario,
            duration_sec=time.time()-start,
            total_ops=ops_count,
            errors=errors,
            performance=PerformanceMetric(0, 0, ops_count / (time.time()-start), 0),
            resource_history=[],
            issues=[]
        ))
    
    async def scenario5_performance_test(self):
        """场景5: 性能测试"""
        scenario = "性能测试"
        print(f"\n--- ⚡ {scenario} ---")
        start = time.time()
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        
        bus = EnhancedEventBus(max_history_size=10000)
        await bus.start()
        
        ops_count = 0
        response_times = []
        
        batch_start = time.time()
        for _ in range(2000):
            op_start = time.time()
            await bus.publish({"index": ops_count})
            response_times.append((time.time() - op_start) * 1000)
            ops_count += 1
        batch_time = time.time() - batch_start
        
        await bus.stop()
        
        avg_rt = sum(response_times) / len(response_times) if response_times else 0
        sorted_rt = sorted(response_times)
        p95_rt = sorted_rt[int(len(sorted_rt)*0.95)] if sorted_rt else 0
        
        print(f"  批量测试: 2000 ops in {batch_time:.2f}s = {2000 / batch_time:.1f} ops/s")
        print(f"  平均响应: {avg_rt:.2f}ms, P95: {p95_rt:.2f}ms")
        
        self.results.append(TestResult(
            scenario=scenario,
            duration_sec=time.time()-start,
            total_ops=ops_count,
            errors=0,
            performance=PerformanceMetric(avg_rt, p95_rt, ops_count / (time.time()-start), 0),
            resource_history=[],
            issues=[]
        ))
    
    def print_summary(self):
        print("\n" + "="*80)
        print("📊 第十轮深度测试 - 摘要")
        print("="*80)
        
        total_ops = sum(r.total_ops for r in self.results)
        total_errors = sum(r.errors for r in self.results)
        
        print(f"\n总操作: {total_ops:,}")
        print(f"总错误: {total_errors:,}")
        print(f"整体错误率: {total_errors/total_ops*100:.2f}%" if total_ops else "")
        
        all_issues = []
        for r in self.results:
            all_issues.extend(r.issues)
        
        if all_issues:
            print(f"\n⚠️  发现问题 ({len(all_issues)}):")
            for i, issue in enumerate(all_issues, 1):
                print(f"  {i}. [{issue['severity']}] {issue['message']}")
        else:
            print("\n✅ 未发现问题！")
        
        print("\n" + "="*80)
    
    def generate_report(self):
        report_path = "/workspace/backend/TENTH_ROUND_SUMMARY.md"
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("# Agent Cluster Orchestrator - 第十轮深度测试总结\n\n")
            f.write(f"**日期**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("---\n\n")
            
            total_ops = sum(r.total_ops for r in self.results)
            total_errors = sum(r.errors for r in self.results)
            
            f.write("## 摘要\n\n")
            f.write(f"- 总操作: {total_ops:,}\n")
            f.write(f"- 总错误: {total_errors:,}\n")
            f.write(f"- 错误率: {total_errors/total_ops*100:.4f}%\n\n" if total_ops else "")
            
            f.write("## 详细结果\n\n")
            for result in self.results:
                f.write(f"### {result.scenario}\n\n")
                f.write(f"- 耗时: {result.duration_sec:.1f}s\n")
                f.write(f"- 操作: {result.total_ops:,}\n")
                f.write(f"- 错误: {result.errors:,}\n")
                f.write(f"- 错误率: {result.performance.error_rate*100:.2f}%\n")
                f.write(f"- 吞吐量: {result.performance.throughput_ops_sec:.1f} ops/s\n")
                if result.performance.avg_response_ms:
                    f.write(f"- 平均响应: {result.performance.avg_response_ms:.2f}ms\n")
                    f.write(f"- P95响应: {result.performance.p95_response_ms:.2f}ms\n")
                
                if result.issues:
                    f.write(f"- 问题:\n")
                    for issue in result.issues:
                        f.write(f"  - [{issue['severity']}] {issue['message']}\n")
                
                f.write("\n")
            
            f.write("---\n")
            f.write("*第十轮深度测试完成*\n")
        
        print(f"\n📄 报告已保存至: {report_path}")


async def main():
    tester = DeepQualityTester()
    await tester.run()


if __name__ == "__main__":
    asyncio.run(main())
