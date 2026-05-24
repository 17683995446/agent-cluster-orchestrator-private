"""
🚀 5分钟深度压力测试 - 中等时长真实运行测试
"""
import asyncio
import time
import random
import sys
import psutil
import os
from typing import List, Dict, Any
from datetime import datetime
import structlog

sys.path.insert(0, '/workspace/backend')

logger = structlog.get_logger()


class FiveMinuteStressTest:
    """5分钟深度压力测试"""

    def __init__(self):
        self.test_duration = 300  # 5分钟
        self.bugs_found = []
        self.start_time = None
        self.metrics = {
            "event_bus": {"published": 0, "processed": 0, "dropped": 0},
            "circuit_breaker": {"total_calls": 0, "open_count": 0},
            "queue": {"enqueued": 0, "dequeued": 0, "rejected": 0},
            "resource": {"cpu_avg": 0, "memory_peak": 0, "memory_avg": 0}
        }

    async def run(self):
        """运行5分钟测试"""
        self.start_time = time.time()
        
        print("\n" + "="*80)
        print("🚀 5分钟深度压力测试")
        print(f"   开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   测试时长: {self.test_duration}秒 (5分钟)")
        print("="*80 + "\n")

        # 创建并发测试任务
        test_tasks = [
            asyncio.create_task(self._event_bus_stress()),
            asyncio.create_task(self._circuit_breaker_stress()),
            asyncio.create_task(self._queue_stress()),
            asyncio.create_task(self._mixed_operations()),
            asyncio.create_task(self._resource_monitor()),
        ]

        # 运行所有测试
        await asyncio.gather(*test_tasks)

        duration = time.time() - self.start_time
        self._print_summary(duration)

    async def _event_bus_stress(self):
        """EventBus压力测试"""
        from app.core.enhanced_event_bus import EnhancedEventBus

        bus = EnhancedEventBus(max_history_size=1000)
        await bus.start()

        processed_count = 0
        
        async def callback(event):
            nonlocal processed_count
            processed_count += 1
            # 模拟一些处理延迟
            if random.random() < 0.001:
                raise ValueError("Random error")

        await bus.subscribe(callback)

        start = time.time()
        iteration = 0
        last_report = start
        
        while time.time() - start < self.test_duration:
            iteration += 1
            
            # 批量发布事件
            batch_size = random.randint(50, 150)
            for i in range(batch_size):
                await bus.publish({
                    "event_id": f"{iteration}_{i}",
                    "timestamp": time.time(),
                    "data": "x" * 100
                })
                self.metrics["event_bus"]["published"] += 1
            
            # 定期报告
            now = time.time()
            if now - last_report >= 30:  # 每30秒报告一次
                elapsed = now - start
                print(f"[EventBus] {elapsed/60:.1f}分钟: "
                      f"发布={self.metrics['event_bus']['published']}, "
                      f"处理={processed_count}")
                last_report = now
            
            # 短暂休眠
            await asyncio.sleep(random.uniform(0.05, 0.15))

        await asyncio.sleep(2)
        await bus.stop()

        stats = bus.get_stats()
        self.metrics["event_bus"]["processed"] = processed_count
        self.metrics["event_bus"]["dropped"] = stats.get('events_dropped', 0)
        
        print(f"[EventBus] 完成: 发布={self.metrics['event_bus']['published']}, "
              f"处理={processed_count}, 历史={len(bus._event_history)}")

        # 检测问题
        if len(bus._event_history) > 1100:
            self.bugs_found.append({
                "component": "EventBus",
                "issue": f"历史超限: {len(bus._event_history)} > 1000",
                "severity": "HIGH"
            })

    async def _circuit_breaker_stress(self):
        """熔断器压力测试"""
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker,
            EnhancedCircuitBreakerConfig
        )

        # 创建多个熔断器
        breakers = {
            f"cb_{i}": EnhancedCircuitBreaker(
                f"cb_{i}",
                EnhancedCircuitBreakerConfig(failure_threshold=5)
            )
            for i in range(15)
        }

        start = time.time()
        iteration = 0
        last_report = start

        while time.time() - start < self.test_duration:
            iteration += 1
            
            # 并发调用所有熔断器
            tasks = []
            for name, breaker in breakers.items():
                async def call_breaker(b):
                    try:
                        if random.random() < 0.8:
                            await b.call(lambda: "success")
                        else:
                            await b.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
                    except:
                        pass
                    return b.get_stats()['total_calls']
                
                tasks.append(call_breaker(breaker))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            total_calls = sum(r for r in results if isinstance(r, int))
            self.metrics["circuit_breaker"]["total_calls"] += total_calls
            
            # 定期报告
            now = time.time()
            if now - last_report >= 30:
                elapsed = now - start
                open_count = sum(1 for b in breakers.values() if b.state.value == "open")
                self.metrics["circuit_breaker"]["open_count"] = open_count
                print(f"[CircuitBreaker] {elapsed/60:.1f}分钟: "
                      f"总调用={self.metrics['circuit_breaker']['total_calls']}, "
                      f"打开={open_count}/15")
                last_report = now
            
            await asyncio.sleep(random.uniform(0.02, 0.1))

        print(f"[CircuitBreaker] 完成: 总调用={self.metrics['circuit_breaker']['total_calls']}")

    async def _queue_stress(self):
        """队列压力测试"""
        from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

        queue = EnhancedAdaptiveQueue()

        start = time.time()
        iteration = 0
        last_report = start

        async def producer():
            nonlocal iteration
            while time.time() - start < self.test_duration:
                iteration += 1
                
                # 批量入队
                batch = random.randint(10, 50)
                for i in range(batch):
                    result = await queue.enqueue(
                        f"item_{iteration}_{i}",
                        priority=random.randint(1, 10)
                    )
                    if result:
                        self.metrics["queue"]["enqueued"] += 1
                    else:
                        self.metrics["queue"]["rejected"] += 1
                
                await asyncio.sleep(random.uniform(0.02, 0.15))

        async def consumer():
            while time.time() - start < self.test_duration:
                item = await queue.dequeue(wait_timeout=1.0)
                if item:
                    self.metrics["queue"]["dequeued"] += 1
                await asyncio.sleep(random.uniform(0.01, 0.08))

        # 启动多个生产者消费者
        producers = [asyncio.create_task(producer()) for _ in range(3)]
        consumers = [asyncio.create_task(consumer()) for _ in range(5)]

        # 定期报告
        while time.time() - start < self.test_duration:
            await asyncio.sleep(30)
            elapsed = time.time() - start
            print(f"[Queue] {elapsed/60:.1f}分钟: "
                  f"入={self.metrics['queue']['enqueued']}, "
                  f"出={self.metrics['queue']['dequeued']}, "
                  f"拒={self.metrics['queue']['rejected']}, "
                  f"队={queue.size()}")

        await asyncio.gather(*producers, *consumers)

        print(f"[Queue] 完成: 入={self.metrics['queue']['enqueued']}, "
              f"出={self.metrics['queue']['dequeued']}, "
              f"拒={self.metrics['queue']['rejected']}")

    async def _mixed_operations(self):
        """混合操作测试"""
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

        async def callback(event):
            pass

        await bus.subscribe(callback)

        operations = {"count": 0, "errors": 0}
        start = time.time()

        async def worker():
            while time.time() - start < self.test_duration:
                try:
                    # EventBus
                    await bus.publish({"op_id": operations["count"]})
                    
                    # Queue
                    await queue.enqueue(f"task_{operations['count']}")
                    item = await queue.dequeue(wait_timeout=0.1)
                    
                    # CircuitBreaker
                    if random.random() < 0.9:
                        await breaker.call(lambda: "mixed")
                    else:
                        try:
                            await breaker.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
                        except:
                            pass
                    
                    operations["count"] += 1
                    await asyncio.sleep(0.01)
                except Exception as e:
                    operations["errors"] += 1

        # 启动多个worker
        workers = [asyncio.create_task(worker()) for _ in range(10)]
        
        # 定期报告
        while time.time() - start < self.test_duration:
            await asyncio.sleep(30)
            elapsed = time.time() - start
            tps = operations["count"] / elapsed
            print(f"[MixedOps] {elapsed/60:.1f}分钟: "
                  f"操作={operations['count']}, "
                  f"TPS={tps:.1f}, "
                  f"错误={operations['errors']}")

        await asyncio.gather(*workers, return_exceptions=True)
        await bus.stop()

        print(f"[MixedOps] 完成: 总操作={operations['count']}, TPS={operations['count']/self.test_duration:.1f}")

    async def _resource_monitor(self):
        """资源监控"""
        process = psutil.Process(os.getpid())
        
        cpu_samples = []
        memory_samples = []
        
        start = time.time()
        last_report = start

        while time.time() - start < self.test_duration:
            try:
                cpu = process.cpu_percent()
                mem = process.memory_info().rss / 1024 / 1024
                
                cpu_samples.append(cpu)
                memory_samples.append(mem)
                
                now = time.time()
                if now - last_report >= 30:
                    elapsed = now - start
                    avg_cpu = sum(cpu_samples) / len(cpu_samples)
                    avg_mem = sum(memory_samples) / len(memory_samples)
                    print(f"[Resource] {elapsed/60:.1f}分钟: "
                          f"CPU={avg_cpu:.1f}%, "
                          f"内存={avg_mem:.1f}MB")
                    last_report = now
                
                await asyncio.sleep(5)
                
            except Exception as e:
                print(f"[Resource] 错误: {e}")
                await asyncio.sleep(5)

        self.metrics["resource"]["cpu_avg"] = sum(cpu_samples) / len(cpu_samples)
        self.metrics["resource"]["memory_avg"] = sum(memory_samples) / len(memory_samples)
        self.metrics["resource"]["memory_peak"] = max(memory_samples)
        
        print(f"[Resource] 完成: CPU平均={self.metrics['resource']['cpu_avg']:.1f}%, "
              f"内存峰值={self.metrics['resource']['memory_peak']:.1f}MB")

    def _print_summary(self, duration: float):
        """打印测试总结"""
        print("\n" + "="*80)
        print("📊 5分钟深度压力测试报告")
        print("="*80)
        
        print(f"\n⏱️  测试时长: {duration:.2f}秒 ({duration/60:.1f}分钟)")
        
        print(f"\n📈 性能指标:")
        
        print(f"\n  EventBus:")
        print(f"    - 发布事件: {self.metrics['event_bus']['published']}")
        print(f"    - 处理事件: {self.metrics['event_bus']['processed']}")
        print(f"    - 丢包: {self.metrics['event_bus']['dropped']}")
        print(f"    - 处理率: {self.metrics['event_bus']['processed']/max(self.metrics['event_bus']['published'], 1)*100:.1f}%")
        
        print(f"\n  CircuitBreaker:")
        print(f"    - 总调用: {self.metrics['circuit_breaker']['total_calls']}")
        print(f"    - 打开熔断: {self.metrics['circuit_breaker']['open_count']}/15")
        
        print(f"\n  Queue:")
        print(f"    - 入队: {self.metrics['queue']['enqueued']}")
        print(f"    - 出队: {self.metrics['queue']['dequeued']}")
        print(f"    - 拒绝: {self.metrics['queue']['rejected']}")
        print(f"    - 吞吐量: {self.metrics['queue']['enqueued']/duration:.1f}/秒")
        
        print(f"\n  Resource:")
        print(f"    - CPU平均: {self.metrics['resource']['cpu_avg']:.1f}%")
        print(f"    - 内存平均: {self.metrics['resource']['memory_avg']:.1f}MB")
        print(f"    - 内存峰值: {self.metrics['resource']['memory_peak']:.1f}MB")
        
        if self.bugs_found:
            print(f"\n⚠️  发现 {len(self.bugs_found)} 个问题:")
            for bug in self.bugs_found:
                print(f"  [{bug['severity']}] {bug['component']}: {bug['issue']}")
        else:
            print(f"\n✅ 未发现问题 - 系统运行稳定!")
        
        print("\n" + "="*80)


async def main():
    tester = FiveMinuteStressTest()
    await tester.run()


if __name__ == "__main__":
    print("⚠️  此测试将运行5分钟")
    response = input("是否继续? (yes/no): ")
    
    if response.lower() == "yes":
        asyncio.run(main())
    else:
        print("测试已取消")
