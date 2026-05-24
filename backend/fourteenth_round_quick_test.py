"""
🔬 第十四轮超级深度测试 - 快速验证版本
特点：
- 详细日志
- 精简规模（200个协程）
- 快速执行（30秒）
"""
import asyncio
import time
import random
import sys
import structlog
from typing import Any, Dict
from datetime import datetime

sys.path.insert(0, '/workspace/backend')

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S.%f"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger(__name__)


class QuickSuperTester:
    """快速超级测试器"""
    
    def __init__(self):
        self.issues = []
        self.stats = {
            "total_events": 0,
            "processed_events": 0,
            "failed": 0
        }
    
    async def run(self):
        print("\n" + "=" * 100)
        print("🔬 第十四轮超级深度测试 - 快速验证")
        print(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 100 + "\n")
        
        await self._test_massive_concurrent()
        await self._test_chaos_circuit()
        await self._test_queue_capacity()
        
        self._print_summary()
        self._save_report()
        
        return len(self.issues) == 0
    
    async def _test_massive_concurrent(self):
        print("\n" + "-" * 100)
        print("📋 测试1: 大规模并发 - 200协程")
        print("-" * 100)
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
        
        bus = EnhancedEventBus(max_history_size=10000)
        queue = EnhancedAdaptiveQueue()
        await bus.start()
        
        processed = 0
        lock = asyncio.Lock()
        
        async def handler(event):
            nonlocal processed
            async with lock:
                processed += 1
        
        await bus.subscribe(handler)
        
        async def worker(wid):
            for i in range(30):
                await bus.publish({"wid": wid, "seq": i})
                await queue.enqueue(f"t{wid}_{i}", priority=random.randint(0, 100))
                if random.random() < 0.2:
                    await asyncio.sleep(random.uniform(0.01, 0.2))
        
        logger.debug("launching_workers", count=200)
        workers = [worker(i) for i in range(200)]
        await asyncio.gather(*workers)
        
        await asyncio.sleep(8)
        await bus.stop()
        
        expected = 200 * 30
        print(f"   预期: {expected}, 实际: {processed}")
        
        if processed < expected * 0.9:
            self.issues.append({
                "test": "massive_concurrent",
                "severity": "high",
                "issue": "事件处理不足"
            })
        
        self.stats["total_events"] = expected
        self.stats["processed_events"] = processed
    
    async def _test_chaos_circuit(self):
        print("\n" + "-" * 100)
        print("📋 测试2: 混乱熔断器")
        print("-" * 100)
        
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker,
            EnhancedCircuitBreakerConfig,
            CircuitState
        )
        
        cb = EnhancedCircuitBreaker(
            "chaos_test",
            EnhancedCircuitBreakerConfig(
                failure_threshold=5,
                success_threshold=2,
                timeout_duration=0.5,
                slow_call_threshold=0.05,
                slow_call_rate_threshold=0.4,
                min_call_count=3
            )
        )
        
        success = 0
        fail = 0
        
        async def flaky():
            if random.random() < 0.3:
                raise Exception("Flaky!")
            if random.random() < 0.2:
                await asyncio.sleep(0.3)
        
        logger.debug("starting_chaos_circuit_test")
        
        for i in range(250):
            try:
                await cb.call(flaky)
                success += 1
            except Exception:
                fail += 1
            
            if i % 50 == 0:
                print(f"   进度 {i}: state={cb.state.value}, success={success}, fail={fail}")
        
        print(f"   最终: state={cb.state.value}, success={success}, fail={fail}")
    
    async def _test_queue_capacity(self):
        print("\n" + "-" * 100)
        print("📋 测试3: 队列容量极限")
        print("-" * 100)
        
        from app.core.enhanced_adaptive_queue import (
            EnhancedAdaptiveQueue, EnhancedQueueConfig
        )
        
        config = EnhancedQueueConfig(initial_capacity=30, max_capacity=300)
        queue = EnhancedAdaptiveQueue(config)
        
        enqueued = 0
        rejected = 0
        
        logger.debug("starting_queue_capacity_test")
        
        for i in range(1500):
            try:
                await queue.enqueue(f"q{i}", priority=random.randint(0, 100), wait_timeout=0.03)
                enqueued += 1
            except Exception:
                rejected += 1
            
            if i % 200 == 0:
                print(f"   进度 {i}: size={queue.size()}, capacity={queue.capacity}, enqueued={enqueued}, rejected={rejected}")
        
        print(f"   最终: enqueued={enqueued}, rejected={rejected}")
        print(f"   队列统计: {queue.get_stats()}")
    
    def _print_summary(self):
        print("\n" + "=" * 100)
        print("📊 快速验证结果")
        print("=" * 100)
        
        if self.issues:
            print(f"\n⚠️ 发现 {len(self.issues)} 个问题:")
            for issue in self.issues:
                print(f"  - [{issue['test']}] [{issue['severity']}] {issue['issue']}")
        else:
            print("\n✅ 所有测试通过！没有问题！")
    
    def _save_report(self):
        with open("/workspace/backend/FOURTEENTH_QUICK_REPORT.md", "w", encoding="utf-8") as f:
            f.write("# 第十四轮超级测试 - 快速验证报告\n\n")
            f.write(f"**日期**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("## 概要\n\n")
            f.write(f"- 总事件数: {self.stats['total_events']}\n")
            f.write(f"- 处理事件数: {self.stats['processed_events']}\n\n")
            
            if self.issues:
                f.write("## 问题\n\n")
                for issue in self.issues:
                    f.write(f"- {issue['test']}: {issue['issue']}\n")
            else:
                f.write("## 结论\n\n")
                f.write("✅ 所有测试通过！\n")


async def main():
    tester = QuickSuperTester()
    return 0 if await tester.run() else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
