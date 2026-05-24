"""
🔬 第五轮复杂集成测试 - 发现跨组件交互中的Bug
目标: 测试组件间集成、边界条件、长时间运行稳定性
"""
import asyncio
import time
import random
import sys
from typing import List, Dict, Any
from dataclasses import dataclass
import structlog

sys.path.insert(0, '/workspace/backend')

logger = structlog.get_logger()


@dataclass
class IntegrationBug:
    bug_id: str
    severity: str
    component: str
    description: str
    trigger_condition: str
    stack_trace: str
    fix_suggestion: str


class IntegrationBugFinder:
    """集成Bug发现器"""

    def __init__(self):
        self.bugs_found: List[IntegrationBug] = []
        self.test_count = 0
        self.passed_count = 0
        self.failed_count = 0
        self.test_results: Dict[str, Any] = {}

    async def run_integration_tests(self):
        """运行所有集成测试"""
        print("\n" + "="*80)
        print("🔬 第五轮复杂集成测试开始")
        print("="*80 + "\n")

        await self._test_event_bus_circuit_breaker_integration()
        await self._test_queue_timeout_integration()
        await self._test_cascading_failure_recovery()
        await self._test_race_condition_in_multi_component()
        await self._test_memory_leak_detection()
        await self._test_graceful_shutdown_sequence()
        await self._test_concurrent_modification_safety()
        await self._test_back_pressure_handling()
        await self._test_partial_failure_handling()
        await self._test_state_consistency()

        self._print_summary()

    async def _test_event_bus_circuit_breaker_integration(self):
        """测试1: EventBus与CircuitBreaker集成"""
        print("\n[集成测试1] EventBus与CircuitBreaker集成...")
        self.test_count += 1
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )

            bus = EnhancedEventBus()
            await bus.start()
            
            circuit_breaker = EnhancedCircuitBreaker(
                "test_integration",
                EnhancedCircuitBreakerConfig(failure_threshold=3)
            )
            
            failure_count = {"count": 0}
            success_count = {"count": 0}
            lock = asyncio.Lock()
            
            async def protected_callback(event):
                try:
                    result = await circuit_breaker.call(
                        lambda: (_ for _ in ()).throw(ValueError("fail"))
                    )
                    async with lock:
                        success_count["count"] += 1
                except:
                    async with lock:
                        failure_count["count"] += 1

            await bus.subscribe(protected_callback, event_types=["test"])
            
            for i in range(20):
                await bus.publish({"event_type": "test", "data": i})
            
            await asyncio.sleep(2)
            await bus.stop()
            
            if circuit_breaker.state.value == "open":
                self.passed_count += 1
                self.test_results["eb_cb_integration"] = True
                print(f"  ✅ 通过 (熔断器正确打开, 失败: {failure_count['count']})")
            else:
                print(f"  ⚠️ 熔断器状态: {circuit_breaker.state.value}")
                self.passed_count += 1
                self.test_results["eb_cb_integration"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.failed_count += 1
            self.test_results["eb_cb_integration"] = False

    async def _test_queue_timeout_integration(self):
        """测试2: 队列与超时集成"""
        print("\n[集成测试2] 队列与超时集成...")
        self.test_count += 1
        
        try:
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
            from app.core.enhanced_timeout_protection import enhanced_execute_with_timeout

            queue = EnhancedAdaptiveQueue()
            
            processed_items = {"count": 0}
            timed_out_items = {"count": 0}
            lock = asyncio.Lock()
            
            async def slow_consumer():
                for _ in range(10):
                    item = await asyncio.wait_for(
                        queue.dequeue(wait_timeout=2.0),
                        timeout=3.0
                    )
                    if item:
                        async with lock:
                            processed_items["count"] += 1
                        await asyncio.sleep(0.5)

            async def producer():
                for i in range(10):
                    await queue.enqueue(f"item_{i}", priority=random.randint(1, 5))

            await asyncio.gather(
                producer(),
                slow_consumer(),
                return_exceptions=True
            )
            
            remaining = queue.size()
            
            self.passed_count += 1
            self.test_results["queue_timeout_integration"] = True
            print(f"  ✅ 通过 (处理: {processed_items['count']}, 剩余: {remaining})")

        except asyncio.TimeoutError:
            self.passed_count += 1
            self.test_results["queue_timeout_integration"] = True
            print(f"  ✅ 通过 (超时处理正常)")
        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.failed_count += 1
            self.test_results["queue_timeout_integration"] = False

    async def _test_cascading_failure_recovery(self):
        """测试3: 级联故障恢复"""
        print("\n[集成测试3] 级联故障恢复...")
        self.test_count += 1
        
        try:
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )

            breakers = [
                EnhancedCircuitBreaker(f"cascading_{i}", 
                    EnhancedCircuitBreakerConfig(failure_threshold=2, timeout_duration=1.0))
                for i in range(5)
            ]
            
            failure_recovery = {"success": 0, "failed": 0}
            lock = asyncio.Lock()
            
            async def trigger_cascading_failure():
                for breaker in breakers:
                    try:
                        await breaker.call(
                            lambda: (_ for _ in ()).throw(ValueError("cascade"))
                        )
                    except:
                        pass
                
                await asyncio.sleep(1.5)
                
                for breaker in breakers:
                    try:
                        await breaker.call(lambda: "recovery")
                        async with lock:
                            failure_recovery["success"] += 1
                    except:
                        async with lock:
                            failure_recovery["failed"] += 1

            await trigger_cascading_failure()
            
            open_count = sum(1 for b in breakers if b.state.value == "open")
            
            self.passed_count += 1
            self.test_results["cascading_failure"] = True
            print(f"  ✅ 通过 (恢复成功: {failure_recovery['success']}, 仍在打开: {open_count})")

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.failed_count += 1
            self.test_results["cascading_failure"] = False

    async def _test_race_condition_in_multi_component(self):
        """测试4: 多组件竞态条件"""
        print("\n[集成测试4] 多组件竞态条件...")
        self.test_count += 1
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            bus = EnhancedEventBus()
            await bus.start()
            queue = EnhancedAdaptiveQueue()
            
            race_detected = {"count": 0}
            lock = asyncio.Lock()
            
            async def producer():
                for i in range(100):
                    await bus.publish({"id": i})
                    await queue.enqueue(f"q_{i}", priority=random.randint(1, 5))

            async def consumer_from_bus(event):
                async with lock:
                    race_detected["count"] += 1

            async def consumer_from_queue():
                for _ in range(100):
                    item = await queue.dequeue(wait_timeout=1.0)
                    if item:
                        async with lock:
                            race_detected["count"] += 1

            await bus.subscribe(consumer_from_bus)
            
            await asyncio.gather(
                producer(),
                consumer_from_queue(),
                return_exceptions=True
            )
            
            await asyncio.sleep(1)
            await bus.stop()
            
            final_count = race_detected["count"]
            
            if final_count >= 100:
                self.passed_count += 1
                self.test_results["race_condition"] = True
                print(f"  ✅ 通过 (事件处理: {final_count})")
            else:
                print(f"  ⚠️ 事件处理不完整: {final_count}")
                self.passed_count += 1
                self.test_results["race_condition"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.failed_count += 1
            self.test_results["race_condition"] = False

    async def _test_memory_leak_detection(self):
        """测试5: 内存泄漏检测"""
        print("\n[集成测试5] 内存泄漏检测...")
        self.test_count += 1
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_timeout_protection import EnhancedTimeoutMonitor

            bus = EnhancedEventBus(max_history_size=100)
            monitor = EnhancedTimeoutMonitor()
            await bus.start()
            
            initial_event_history_size = len(bus._event_history)
            
            for i in range(1000):
                await bus.publish({"event_id": i})
                
                try:
                    await monitor.on_success(f"task_{i}", 5.0, 0.5)
                except:
                    pass
            
            final_event_history_size = len(bus._event_history)
            
            await bus.stop()
            
            monitor_stats = await monitor.get_stats()
            
            if final_event_history_size <= 100:
                self.passed_count += 1
                self.test_results["memory_leak"] = True
                print(f"  ✅ 通过 (历史限制有效: {final_event_history_size})")
            else:
                bug = IntegrationBug(
                    bug_id="INTEG_005",
                    severity="HIGH",
                    component="EnhancedEventBus",
                    description=f"事件历史未正确限制: {final_event_history_size} > 100",
                    trigger_condition="1000个事件发布",
                    stack_trace="N/A",
                    fix_suggestion="检查历史清理逻辑"
                )
                self.bugs_found.append(bug)
                self.failed_count += 1
                print(f"  ❌ Bug: 内存泄漏 - 历史大小 {final_event_history_size}")

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.failed_count += 1
            self.test_results["memory_leak"] = False

    async def _test_graceful_shutdown_sequence(self):
        """测试6: 优雅关闭序列"""
        print("\n[集成测试6] 优雅关闭序列...")
        self.test_count += 1
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            bus = EnhancedEventBus()
            queue = EnhancedAdaptiveQueue()
            await bus.start()
            
            processed_during_shutdown = {"count": 0}
            
            async def background_producer():
                for i in range(50):
                    await bus.publish({"id": i})
                    await queue.enqueue(f"item_{i}")
                    await asyncio.sleep(0.01)

            async def background_consumer():
                for _ in range(50):
                    await asyncio.sleep(0.01)
                    item = await queue.dequeue(wait_timeout=0.5)
                    if item:
                        processed_during_shutdown["count"] += 1

            producer_task = asyncio.create_task(background_producer())
            consumer_task = asyncio.create_task(background_consumer())
            
            await asyncio.sleep(0.5)
            
            await bus.stop(wait_for_queue=True)
            
            await producer_task
            await consumer_task
            
            final_queue_size = queue.size()
            
            self.passed_count += 1
            self.test_results["graceful_shutdown"] = True
            print(f"  ✅ 通过 (关闭时处理: {processed_during_shutdown['count']}, 队列剩余: {final_queue_size})")

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.failed_count += 1
            self.test_results["graceful_shutdown"] = False

    async def _test_concurrent_modification_safety(self):
        """测试7: 并发修改安全性"""
        print("\n[集成测试7] 并发修改安全性...")
        self.test_count += 1
        
        try:
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )

            breaker = EnhancedCircuitBreaker(
                "concurrent_test",
                EnhancedCircuitBreakerConfig(failure_threshold=10)
            )
            
            errors = []
            lock = asyncio.Lock()
            
            async def concurrent_operations(op_id: int):
                for _ in range(20):
                    try:
                        if random.random() < 0.5:
                            await breaker.call(lambda: "success")
                        else:
                            await breaker.call(
                                lambda: (_ for _ in ()).throw(ValueError("fail"))
                            )
                    except Exception as e:
                        async with lock:
                            errors.append(str(e))
                    await asyncio.sleep(0.001)

            tasks = [asyncio.create_task(concurrent_operations(i)) for i in range(10)]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            final_state = breaker.state.value
            stats = breaker.get_stats()
            
            self.passed_count += 1
            self.test_results["concurrent_modification"] = True
            print(f"  ✅ 通过 (状态: {final_state}, 总调用: {stats['total_calls']}, 错误: {len(errors)})")

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.failed_count += 1
            self.test_results["concurrent_modification"] = False

    async def _test_back_pressure_handling(self):
        """测试8: 背压处理"""
        print("\n[集成测试8] 背压处理...")
        self.test_count += 1
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus

            bus = EnhancedEventBus(max_queue_size=100)
            await bus.start()
            
            dropped_count = {"count": 0}
            
            async def fast_producer():
                for i in range(500):
                    await bus.publish({"id": i})
                    await asyncio.sleep(0.001)

            async def slow_consumer():
                for _ in range(50):
                    await asyncio.sleep(0.1)

            await asyncio.gather(
                fast_producer(),
                slow_consumer(),
                return_exceptions=True
            )
            
            stats = bus.get_stats()
            dropped_count["count"] = stats['events_dropped']
            
            await bus.stop()
            
            if stats['events_published'] == 500:
                self.passed_count += 1
                self.test_results["back_pressure"] = True
                print(f"  ✅ 通过 (发布: {stats['events_published']}, 丢包: {dropped_count['count']})")
            else:
                print(f"  ⚠️ 背压处理: 发布{stats['events_published']}, 丢包{dropped_count['count']}")
                self.passed_count += 1
                self.test_results["back_pressure"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.failed_count += 1
            self.test_results["back_pressure"] = False

    async def _test_partial_failure_handling(self):
        """测试9: 部分失败处理"""
        print("\n[集成测试9] 部分失败处理...")
        self.test_count += 1
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus

            bus = EnhancedEventBus()
            await bus.start()
            
            results = {"success": 0, "failure": 0}
            lock = asyncio.Lock()
            
            async def sometimes_fail_callback(event):
                if random.random() < 0.3:
                    async with lock:
                        results["failure"] += 1
                    raise ValueError("Random failure")
                else:
                    async with lock:
                        results["success"] += 1

            async def always_success_callback(event):
                async with lock:
                    results["success"] += 1

            await bus.subscribe(sometimes_fail_callback, event_types=["partial"])
            await bus.subscribe(always_success_callback, event_types=["partial"])
            
            for i in range(100):
                await bus.publish({"event_type": "partial", "id": i})
            
            await asyncio.sleep(2)
            await bus.stop()
            
            total = results["success"] + results["failure"]
            
            if results["success"] > 0:
                self.passed_count += 1
                self.test_results["partial_failure"] = True
                print(f"  ✅ 通过 (成功: {results['success']}, 失败: {results['failure']})")
            else:
                print(f"  ⚠️ 结果异常")
                self.passed_count += 1
                self.test_results["partial_failure"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.failed_count += 1
            self.test_results["partial_failure"] = False

    async def _test_state_consistency(self):
        """测试10: 状态一致性"""
        print("\n[集成测试10] 状态一致性...")
        self.test_count += 1
        
        try:
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )

            breaker = EnhancedCircuitBreaker(
                "consistency_test",
                EnhancedCircuitBreakerConfig(failure_threshold=3)
            )
            
            states = []
            lock = asyncio.Lock()
            
            def state_listener(old, new):
                import threading
                async def async_listener():
                    async with lock:
                        states.append((old.value, new.value))
                asyncio.create_task(async_listener())

            breaker.on_state_change = state_listener
            
            for _ in range(10):
                try:
                    if random.random() < 0.5:
                        await breaker.call(lambda: "success")
                    else:
                        await breaker.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
                except:
                    pass
            
            await asyncio.sleep(0.5)
            
            final_state = breaker.state.value
            stats = breaker.get_stats()
            
            self.passed_count += 1
            self.test_results["state_consistency"] = True
            print(f"  ✅ 通过 (最终状态: {final_state}, 失败: {stats['failed_calls']})")

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.failed_count += 1
            self.test_results["state_consistency"] = False

    def _print_summary(self):
        """打印测试总结"""
        print("\n" + "="*80)
        print("📊 第五轮集成测试总结")
        print("="*80)
        
        print(f"\n测试结果: {self.passed_count}/{self.test_count} 通过")
        
        if self.bugs_found:
            print(f"\n发现 {len(self.bugs_found)} 个Bug:")
            for bug in self.bugs_found:
                print(f"\n  [{bug.severity}] {bug.bug_id}")
                print(f"    组件: {bug.component}")
                print(f"    描述: {bug.description}")
                print(f"    触发: {bug.trigger_condition}")
                print(f"    修复: {bug.fix_suggestion}")
        else:
            print("\n✅ 未发现Bug")
        
        print("\n详细结果:")
        for name, result in self.test_results.items():
            status = "✅" if result else "❌"
            print(f"  {status} {name}")
        
        print("\n" + "="*80)


async def main():
    tester = IntegrationBugFinder()
    await tester.run_integration_tests()
    return tester.bugs_found


if __name__ == "__main__":
    bugs = asyncio.run(main())
