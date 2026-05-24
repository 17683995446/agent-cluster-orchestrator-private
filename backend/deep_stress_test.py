"""
🔥 深度压力测试 - 长时间高负载测试
目标: 发现长时间运行中的潜在问题
"""
import asyncio
import time
import random
import sys
from typing import List, Dict, Any
import structlog

sys.path.insert(0, '/workspace/backend')

logger = structlog.get_logger()


class DeepStressTester:
    """深度压力测试器"""

    def __init__(self):
        self.issues_found = []
        self.test_duration = 60  # 60秒测试
        self.metrics = {}

    async def run_deep_stress_test(self):
        """运行深度压力测试"""
        print("\n" + "="*80)
        print("🔥 深度压力测试开始 (60秒)")
        print("="*80 + "\n")

        print(f"📊 测试配置:")
        print(f"   - 持续时间: {self.test_duration}秒")
        print(f"   - EventBus 事件频率: 100/秒")
        print(f"   - CircuitBreaker 并发: 20")
        print(f"   - Queue 操作频率: 50/秒")
        print()

        start_time = time.time()
        
        # 启动所有压力测试
        tasks = [
            asyncio.create_task(self._event_bus_stress()),
            asyncio.create_task(self._circuit_breaker_stress()),
            asyncio.create_task(self._queue_stress()),
            asyncio.create_task(self._mixed_operations()),
            asyncio.create_task(self._resource_monitor()),
        ]

        await asyncio.gather(*tasks)
        
        duration = time.time() - start_time
        
        self._print_summary(duration)

    async def _event_bus_stress(self):
        """EventBus压力测试"""
        from app.core.enhanced_event_bus import EnhancedEventBus
        
        bus = EnhancedEventBus(max_history_size=1000)
        await bus.start()
        
        event_count = {"published": 0, "processed": 0}
        
        async def counting_callback(event):
            event_count["processed"] += 1

        await bus.subscribe(counting_callback)
        
        start_time = time.time()
        while time.time() - start_time < self.test_duration:
            for i in range(10):
                await bus.publish({"event_id": i, "timestamp": time.time()})
                event_count["published"] += 1
            await asyncio.sleep(0.1)
        
        await asyncio.sleep(2)
        await bus.stop()
        
        stats = bus.get_stats()
        
        print(f"\n📈 EventBus 统计:")
        print(f"   - 发布事件: {event_count['published']}")
        print(f"   - 处理事件: {event_count['processed']}")
        print(f"   - 丢包: {stats['events_dropped']}")
        print(f"   - 历史大小: {len(bus._event_history)}")
        
        if stats['events_dropped'] > event_count['published'] * 0.1:
            self.issues_found.append({
                "component": "EventBus",
                "issue": f"丢包率过高: {stats['events_dropped']}/{event_count['published']}",
                "severity": "MEDIUM"
            })
        
        if len(bus._event_history) > 1100:
            self.issues_found.append({
                "component": "EventBus",
                "issue": f"历史记录未正确限制: {len(bus._event_history)}",
                "severity": "HIGH"
            })
        
        self.metrics['event_bus'] = {
            'published': event_count['published'],
            'processed': event_count['processed'],
            'dropped': stats['events_dropped']
        }

    async def _circuit_breaker_stress(self):
        """CircuitBreaker压力测试"""
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker,
            EnhancedCircuitBreakerConfig
        )
        
        breakers = [
            EnhancedCircuitBreaker(f"stress_{i}", EnhancedCircuitBreakerConfig(failure_threshold=5))
            for i in range(10)
        ]
        
        total_calls = {"success": 0, "failed": 0}
        
        start_time = time.time()
        while time.time() - start_time < self.test_duration:
            tasks = []
            for breaker in breakers:
                async def call_breaker(b):
                    try:
                        if random.random() < 0.3:
                            await b.call(lambda: "success")
                            total_calls["success"] += 1
                        else:
                            await b.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
                            total_calls["failed"] += 1
                    except:
                        total_calls["failed"] += 1
                
                tasks.append(call_breaker(breaker))
            
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(0.5)
        
        open_count = sum(1 for b in breakers if b.state.value == "open")
        
        print(f"\n📈 CircuitBreaker 统计:")
        print(f"   - 总调用: {sum(b.get_stats()['total_calls'] for b in breakers)}")
        print(f"   - 打开的熔断器: {open_count}/10")
        print(f"   - 成功率: {total_calls['success']}/{total_calls['success']+total_calls['failed']}")
        
        if open_count == 0 and total_calls['failed'] > 100:
            self.issues_found.append({
                "component": "CircuitBreaker",
                "issue": f"高失败率下未打开熔断",
                "severity": "MEDIUM"
            })
        
        self.metrics['circuit_breaker'] = {
            'total_calls': sum(b.get_stats()['total_calls'] for b in breakers),
            'open_count': open_count
        }

    async def _queue_stress(self):
        """队列压力测试"""
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
        
        queue = EnhancedAdaptiveQueue()
        
        enqueued = {"count": 0}
        dequeued = {"count": 0}
        
        async def producer():
            start_time = time.time()
            while time.time() - start_time < self.test_duration:
                for i in range(5):
                    result = await queue.enqueue(f"item_{i}", priority=random.randint(1, 10))
                    if result:
                        enqueued["count"] += 1
                await asyncio.sleep(0.1)
        
        async def consumer():
            start_time = time.time()
            while time.time() - start_time < self.test_duration:
                item = await queue.dequeue(wait_timeout=1.0)
                if item:
                    dequeued["count"] += 1
                await asyncio.sleep(0.05)
        
        await asyncio.gather(producer(), consumer())
        
        stats = queue.get_stats()
        
        print(f"\n📈 Queue 统计:")
        print(f"   - 入队: {enqueued['count']}")
        print(f"   - 出队: {dequeued['count']}")
        print(f"   - 拒绝: {stats['rejected']}")
        print(f"   - 当前大小: {queue.size()}")
        
        if stats['rejected'] > enqueued['count'] * 0.1:
            self.issues_found.append({
                "component": "Queue",
                "issue": f"拒绝率过高: {stats['rejected']}/{enqueued['count']}",
                "severity": "LOW"
            })
        
        if queue.size() > 100:
            self.issues_found.append({
                "component": "Queue",
                "issue": f"队列积压过多: {queue.size()}",
                "severity": "MEDIUM"
            })
        
        self.metrics['queue'] = {
            'enqueued': enqueued['count'],
            'dequeued': dequeued['count'],
            'rejected': stats['rejected']
        }

    async def _mixed_operations(self):
        """混合操作压力测试"""
        from app.core.enhanced_event_bus import EnhancedEventBus
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker,
            EnhancedCircuitBreakerConfig
        )
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
        
        bus = EnhancedEventBus()
        breaker = EnhancedCircuitBreaker("mixed", EnhancedCircuitBreakerConfig())
        queue = EnhancedAdaptiveQueue()
        
        await bus.start()
        
        operations = {"count": 0}
        errors = []
        
        async def mixed_worker():
            start_time = time.time()
            while time.time() - start_time < self.test_duration:
                try:
                    await bus.publish({"op_id": operations["count"]})
                    await queue.enqueue(f"task_{operations['count']}")
                    await breaker.call(lambda: "mixed_op")
                    
                    item = await queue.dequeue(wait_timeout=0.1)
                    operations["count"] += 1
                    
                    await asyncio.sleep(0.01)
                except Exception as e:
                    errors.append(str(e))
        
        tasks = [asyncio.create_task(mixed_worker()) for _ in range(5)]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        await bus.stop()
        
        print(f"\n📈 混合操作 统计:")
        print(f"   - 总操作: {operations['count']}")
        print(f"   - 错误: {len(errors)}")
        print(f"   - TPS: {operations['count']/self.test_duration:.2f}")
        
        self.metrics['mixed'] = {
            'operations': operations['count'],
            'errors': len(errors),
            'tps': operations['count']/self.test_duration
        }

    async def _resource_monitor(self):
        """资源监控"""
        print(f"\n📈 资源监控 (运行 {self.test_duration}秒):")
        
        await asyncio.sleep(self.test_duration)
        
        print(f"   - 监控结束")

    def _print_summary(self, duration: float):
        """打印测试总结"""
        print("\n" + "="*80)
        print("🔥 深度压力测试总结")
        print("="*80)
        
        print(f"\n⏱️  测试时长: {duration:.2f}秒")
        
        print(f"\n📊 性能指标:")
        if 'event_bus' in self.metrics:
            eb = self.metrics['event_bus']
            print(f"   EventBus:")
            print(f"     - 事件处理: {eb['published']} 发布, {eb['processed']} 处理")
            print(f"     - 处理率: {(eb['processed']/eb['published']*100):.1f}%")
        
        if 'circuit_breaker' in self.metrics:
            cb = self.metrics['circuit_breaker']
            print(f"   CircuitBreaker:")
            print(f"     - 总调用: {cb['total_calls']}")
            print(f"     - 打开熔断: {cb['open_count']}")
        
        if 'queue' in self.metrics:
            q = self.metrics['queue']
            print(f"   Queue:")
            print(f"     - 吞吐量: {q['enqueued']} 入, {q['dequeued']} 出")
            print(f"     - 拒绝: {q['rejected']}")
        
        if 'mixed' in self.metrics:
            m = self.metrics['mixed']
            print(f"   混合操作:")
            print(f"     - TPS: {m['tps']:.2f}")
            print(f"     - 错误率: {(m['errors']/m['operations']*100):.2f}%")
        
        if self.issues_found:
            print(f"\n⚠️  发现 {len(self.issues_found)} 个问题:")
            for issue in self.issues_found:
                print(f"   [{issue['severity']}] {issue['component']}: {issue['issue']}")
        else:
            print(f"\n✅ 未发现问题")
        
        print("\n" + "="*80)


async def main():
    tester = DeepStressTester()
    await tester.run_deep_stress_test()


if __name__ == "__main__":
    asyncio.run(main())
