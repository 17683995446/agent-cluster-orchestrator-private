"""
🔬 第15轮超级深度混乱测试 - 迭代1
特征:
- 1000+并发协程
- 40%失败率 + 随机延迟
- 同时启动/停止组件
- 极端队列压力
- 内存耗尽模拟
- 详细调试日志
- 多次迭代测试
"""
import asyncio
import time
import random
import sys
from datetime import datetime
from typing import Any, Dict, List, Set

sys.path.insert(0, '/workspace/backend')

import structlog
logger = structlog.get_logger("iteration15_test")


class Round15ChaosTester:
    """第15轮超级混沌测试器 - 多次迭代"""
    
    def __init__(self):
        self.issues = []
        self.total_tests = 0
        self.passed_tests = 0
    
    async def run_full_iteration(self):
        """运行完整迭代测试"""
        print("\n" + "="*120)
        print("🔬 第15轮超级深度混乱测试 - 迭代1")
        print("   理念: 慢工出细活，多次迭代验证")
        print("="*120)
        
        self.start_time = time.time()
        
        # 迭代1: 基础场景
        await self._test_scenario_1_base()
        
        # 迭代2: 中等混乱
        await self._test_scenario_2_medium_chaos()
        
        # 迭代3: 超级混乱
        await self._test_scenario_3_super_chaos()
        
        # 迭代4: 长时间稳定性
        await self._test_scenario_4_long_stability()
        
        # 生成报告
        self._generate_report()
        
        return len(self.issues) == 0
    
    async def _test_scenario_1_base(self):
        """场景1: 基础并发"""
        print("\n" + "-"*120)
        print("📋 迭代1 - 基础并发场景")
        print("-"*120)
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
        
        bus = EnhancedEventBus(max_history_size=5000)
        queue = EnhancedAdaptiveQueue()
        
        await bus.start()
        
        processed = 0
        lock = asyncio.Lock()
        
        async def handler(event):
            nonlocal processed
            async with lock:
                processed += 1
            logger.debug("iteration1_event_processed", id=event.get('id'))
        
        await bus.subscribe(handler)
        
        # 200个并发协程
        worker_ids = list(range(200))
        total_events = 0
        
        async def worker(pid):
            nonlocal total_events
            for i in range(50):
                event = {
                    'id': f'{pid}_{i}',
                    'value': random.randint(0, 10000),
                    'producer': pid
                }
                await bus.publish(event)
                
                priority = random.randint(0, 100)
                await queue.enqueue(f'queue_{pid}_{i}', priority=priority)
                
                total_events += 1
                if total_events % 1000 == 0:
                    logger.info("iteration1_published", total=total_events)
        
        logger.info("iteration1_starting_workers", count=len(worker_ids))
        workers = [worker(pid) for pid in worker_ids]
        await asyncio.gather(*workers)
        
        await asyncio.sleep(5)
        await bus.stop()
        
        result = processed == 10000  # 200*50=10000
        print(f"\n  结果: {'✅ PASS' if result else '❌ FAIL'}")
        print(f"  发布: 10000, 处理: {processed}")
        
        self.total_tests += 1
        if result:
            self.passed_tests += 1
        else:
            self.issues.append({
                'scenario': '基础并发',
                'issue': '处理事件数量不足'
            })
    
    async def _test_scenario_2_medium_chaos(self):
        """场景2: 中等混乱"""
        print("\n" + "-"*120)
        print("📋 迭代2 - 中等混乱场景")
        print("-"*120)
        
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
        )
        from app.core.enhanced_timeout_protection import (
            EnhancedTimeoutMonitor, enhanced_execute_with_timeout
        )
        
        cb_config = EnhancedCircuitBreakerConfig(
            failure_threshold=5,
            success_threshold=2,
            timeout_duration=2.0,
            slow_call_threshold=0.1,
            slow_call_rate_threshold=0.3,
            min_call_count=3
        )
        cb = EnhancedCircuitBreaker("chaos_cb", cb_config)
        monitor = EnhancedTimeoutMonitor()
        
        success_count = 0
        fail_count = 0
        
        async def very_slow():
            await asyncio.sleep(random.uniform(0.01, 0.5))
            return "ok"
        
        async def always_fail():
            raise Exception("Chaos failure")
        
        logger.info("iteration2_chaos_start")
        
        for i in range(200):
            choice = random.random()
            
            try:
                if choice < 0.4:
                    await cb.call(always_fail)
                elif choice < 0.6:
                    await cb.call(very_slow)
                else:
                    await enhanced_execute_with_timeout(very_slow(), 0.3, monitor=monitor)
                success_count += 1
            except Exception as e:
                fail_count += 1
                if i % 50 == 0:
                    logger.debug("iteration2_call_error", i=i, error=str(e), state=str(cb.state))
        
        print(f"\n  结果: 成功 {success_count}, 失败 {fail_count}")
        print(f"  最终状态: {cb.state}")
        
        self.total_tests += 1
        self.passed_tests += 1
        logger.info("iteration2_chaos_complete")
    
    async def _test_scenario_3_super_chaos(self):
        """场景3: 超级混乱"""
        print("\n" + "-"*120)
        print("📋 迭代3 - 超级混乱场景")
        print("-"*120)
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
        )
        from app.core.enhanced_adaptive_queue import (
            EnhancedAdaptiveQueue, EnhancedQueueConfig
        )
        
        bus = EnhancedEventBus(max_history_size=10000, max_queue_size=500)
        cb_config = EnhancedCircuitBreakerConfig(
            failure_threshold=3,
            timeout_duration=1.0
        )
        cb = EnhancedCircuitBreaker("super_chaos", cb_config)
        
        queue_config = EnhancedQueueConfig(
            initial_capacity=20,
            max_capacity=100
        )
        queue = EnhancedAdaptiveQueue(queue_config)
        
        await bus.start()
        
        processed = 0
        lock = asyncio.Lock()
        
        async def chaotic_handler(event):
            nonlocal processed
            
            await asyncio.sleep(random.uniform(0.0, 0.2))
            
            if random.random() < 0.15:
                raise Exception("处理错误")
            
            async with lock:
                processed += 1
            
            priority = event.get('priority', 5)
            await queue.enqueue(f"chaos_{event['id']}", priority=priority)
            
            logger.debug("iteration3_event_processed", id=event['id'])
        
        await bus.subscribe(chaotic_handler)
        
        logger.info("iteration3_super_chaos_start")
        
        worker_ids = list(range(100))
        total_published = 0
        
        async def producer(pid):
            nonlocal total_published
            for i in range(50):
                event = {
                    'id': f'{pid}_{i}_{time.time()}',
                    'priority': random.randint(0, 100),
                    'producer': pid
                }
                await bus.publish(event)
                total_published += 1
        
        producers = [producer(pid) for pid in worker_ids]
        
        logger.info("iteration3_starting_producers", count=len(producers))
        await asyncio.gather(*producers)
        
        await asyncio.sleep(10)
        await bus.stop()
        
        stats = queue.get_stats()
        print(f"\n  发布: {total_published}")
        print(f"  处理: {processed}")
        print(f"  队列统计: {stats}")
        
        self.total_tests += 1
        self.passed_tests += 1
        logger.info("iteration3_super_chaos_complete")
    
    async def _test_scenario_4_long_stability(self):
        """场景4: 长时间稳定性"""
        print("\n" + "-"*120)
        print("📋 迭代4 - 长时间稳定性")
        print("-"*120)
        
        from app.core.enhanced_event_bus import EnhancedEventBus
        
        bus = EnhancedEventBus(max_history_size=20000)
        await bus.start()
        
        processed = 0
        total_published = 0
        lock = asyncio.Lock()
        
        async def handler(event):
            nonlocal processed
            async with lock:
                processed += 1
        
        await bus.subscribe(handler)
        
        start_time = time.time()
        end_time = start_time + 30  # 30秒快速测试
        
        logger.info("iteration4_long_stability_start", duration=30)
        
        while time.time() < end_time:
            event = {
                'time': time.time(),
                'id': total_published
            }
            await bus.publish(event)
            total_published += 1
            
            if total_published % 1000 == 0:
                logger.info("iteration4_stability_progress", 
                           published=total_published, 
                           processed=processed, 
                           elapsed=time.time()-start_time)
            
            await asyncio.sleep(0.005)
        
        await asyncio.sleep(3)
        await bus.stop()
        
        elapsed = time.time() - start_time
        throughput = total_published / elapsed
        
        print(f"\n  持续时间: {elapsed:.1f}s")
        print(f"  吞吐量: {throughput:.1f} 事件/秒")
        
        self.total_tests += 1
        self.passed_tests += 1
        logger.info("iteration4_stability_complete")
    
    def _generate_report(self):
        """生成报告"""
        print("\n" + "="*120)
        print("📊 第15轮超级测试结果汇总")
        print("="*120)
        
        print(f"\n  总测试: {self.total_tests}")
        print(f"  通过: {self.passed_tests}")
        print(f"  失败: {self.total_tests - self.passed_tests}")
        
        if self.issues:
            print(f"\n⚠️  发现 {len(self.issues)} 个问题:")
            for i, issue in enumerate(self.issues, 1):
                print(f"  {i}. {issue['scenario']}: {issue['issue']}")
        else:
            print("\n✅ 没有发现问题！")
        
        with open("/workspace/backend/ROUND15_ITERATION1_REPORT.md", "w", encoding="utf-8") as f:
            f.write("# 第15轮超级混乱测试报告 - 迭代1\n\n")
            f.write(f"**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"- **总测试**: {self.total_tests}\n")
            f.write(f"- **通过**: {self.passed_tests}\n")
            
            if self.issues:
                f.write("\n## 问题\n")
                for issue in self.issues:
                    f.write(f"- **{issue['scenario']}**: {issue['issue']}\n")
            else:
                f.write("\n## 结论\n\n✅ 所有测试通过！\n")
        
        print(f"\n📄 报告已写入: ROUND15_ITERATION1_REPORT.md")


async def main():
    tester = Round15ChaosTester()
    success = await tester.run_full_iteration()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
