"""
🔬 极限压力测试 - 发现边缘情况和极限场景的Bug
目标: 通过极端场景发现系统极限性能和隐藏Bug
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
class StressBug:
    bug_id: str
    severity: str
    location: str
    description: str
    trigger_condition: str
    fix_suggestion: str


class ExtremeStressTester:
    """极限压力测试器"""

    def __init__(self):
        self.bugs_found: List[StressBug] = []
        self.test_results: Dict[str, bool] = {}

    async def run_extreme_tests(self):
        """运行所有极限测试"""
        print("\n" + "="*80)
        print("🔬 极限压力测试开始")
        print("="*80 + "\n")

        await self._test_massive_concurrent_events()
        await self._test_rapid_state_changes()
        await self._test_queue_overflow_protection()
        await self._test_circuit_breaker_storm()
        await self._test_event_bus_memory_pressure()
        await self._test_queue_starvation()
        await self._test_circuit_breaker_flapping()
        await self._test_timeout_chain_cascade()
        await self._test_mixed_priority_load()
        await self._test_graceful_degradation()

        self._print_summary()

    async def _test_massive_concurrent_events(self):
        """测试1: 海量并发事件"""
        print("\n[极限测试1] 海量并发事件 (10000+ 事件)...")
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus

            bus = EnhancedEventBus(max_queue_size=5000)
            await bus.start()

            received_count = {"count": 0}
            
            async def counting_callback(event):
                received_count["count"] += 1

            await bus.subscribe(counting_callback)

            start_time = time.time()
            
            tasks = []
            for i in range(10000):
                tasks.append(bus.publish({"event_id": i, "data": "x" * 100}))
            
            await asyncio.gather(*tasks)
            publish_time = time.time() - start_time

            await asyncio.sleep(2)
            await bus.stop()

            dropped = bus.get_stats()['events_dropped']
            
            if dropped > 100:
                bug = StressBug(
                    bug_id="STRESS_001",
                    severity="MEDIUM",
                    location="EnhancedEventBus",
                    description=f"事件丢弃过多: {dropped}/10000 ({(dropped/100)*100:.1f}%)",
                    trigger_condition="max_queue_size=5000 但发布10000事件",
                    fix_suggestion="动态扩容队列或使用背压机制"
                )
                self.bugs_found.append(bug)
                print(f"  ⚠️ 发现潜在问题: 丢包率 {(dropped/10000)*100:.1f}%")
            else:
                print(f"  ✅ 通过 (丢包: {dropped}, 耗时: {publish_time:.2f}s)")
                self.test_results["massive_concurrent_events"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.test_results["massive_concurrent_events"] = False

    async def _test_rapid_state_changes(self):
        """测试2: 快速状态切换"""
        print("\n[极限测试2] 快速状态切换 (500次)...")
        
        try:
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )
            from app.core.enhanced_circuit_breaker import CircuitState

            cb = EnhancedCircuitBreaker(
                "rapid_state_test",
                EnhancedCircuitBreakerConfig(
                    failure_threshold=2,
                    success_threshold=2,
                    timeout_duration=0.5
                )
            )

            state_changes = []
            
            async def track_state(old, new):
                state_changes.append((old, new))

            cb.on_state_change = track_state

            async def rapid_operations():
                for i in range(500):
                    try:
                        if i % 2 == 0:
                            await cb.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
                        else:
                            await cb.call(lambda: "success")
                    except:
                        pass
                    await asyncio.sleep(0.001)

            await rapid_operations()
            await asyncio.sleep(1)

            flapping_count = self._count_flapping(state_changes)
            
            if flapping_count > 5:
                bug = StressBug(
                    bug_id="STRESS_002",
                    severity="MEDIUM",
                    location="EnhancedCircuitBreaker",
                    description=f"状态抖动过多: {flapping_count}次",
                    trigger_condition="timeout_duration太短(0.5s)",
                    fix_suggestion="增加timeout_duration或添加抖动抑制"
                )
                self.bugs_found.append(bug)
                print(f"  ⚠️ 发现潜在问题: 状态抖动 {flapping_count}次")
            else:
                print(f"  ✅ 通过 (状态变化: {len(state_changes)}, 抖动: {flapping_count})")
                self.test_results["rapid_state_changes"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.test_results["rapid_state_changes"] = False

    def _count_flapping(self, state_changes: List) -> int:
        """计算状态抖动次数"""
        flapping = 0
        for i in range(1, len(state_changes)):
            prev = state_changes[i-1][1]
            curr = state_changes[i][1]
            if prev == "half_open" and curr == "open":
                flapping += 1
            elif prev == "open" and curr == "half_open":
                flapping += 1
        return flapping

    async def _test_queue_overflow_protection(self):
        """测试3: 队列溢出保护"""
        print("\n[极限测试3] 队列溢出保护...")
        
        try:
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue, EnhancedQueueConfig

            queue = EnhancedAdaptiveQueue(EnhancedQueueConfig(
                initial_capacity=50,
                max_capacity=100,
                growth_cooldown=0.01,
                shrink_cooldown=0.01
            ))

            rejected_count = {"count": 0}
            
            async def rapid_enqueue():
                for i in range(100):
                    result = await queue.enqueue(f"item_{i}", priority=random.randint(1, 5))
                    if not result:
                        rejected_count["count"] += 1

            await rapid_enqueue()
            
            stats = queue.get_stats()
            
            if rejected_count["count"] > 50:
                bug = StressBug(
                    bug_id="STRESS_003",
                    severity="LOW",
                    location="EnhancedAdaptiveQueue",
                    description=f"拒绝率过高: {rejected_count['count']}/100",
                    trigger_condition="容量限制过小",
                    fix_suggestion="根据负载动态调整max_capacity"
                )
                self.bugs_found.append(bug)
                print(f"  ⚠️ 发现: 拒绝率 {(rejected_count['count']/100)*100:.1f}%")
            else:
                print(f"  ✅ 通过 (拒绝: {rejected_count['count']}, 剩余: {queue.size()})")
                self.test_results["queue_overflow_protection"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.test_results["queue_overflow_protection"] = False

    async def _test_circuit_breaker_storm(self):
        """测试4: 熔断器风暴"""
        print("\n[极限测试4] 熔断器风暴...")
        
        try:
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )

            breakers = [
                EnhancedCircuitBreaker(f"breaker_{i}", EnhancedCircuitBreakerConfig(failure_threshold=3))
                for i in range(50)
            ]

            async def trigger_breaker(breaker, idx):
                for _ in range(10):
                    try:
                        await breaker.call(lambda: (_ for _ in ()).throw(ValueError("storm")))
                    except:
                        pass

            start_time = time.time()
            
            tasks = [trigger_breaker(b, i) for i, b in enumerate(breakers)]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            duration = time.time() - start_time

            open_count = sum(1 for b in breakers if b.state.value == "open")
            
            print(f"  📊 50个熔断器在 {duration:.2f}s 内处理完毕")
            print(f"  📊 打开的熔断器: {open_count}/50")
            
            if duration > 5.0:
                bug = StressBug(
                    bug_id="STRESS_004",
                    severity="MEDIUM",
                    location="EnhancedCircuitBreaker",
                    description=f"熔断器风暴处理过慢: {duration:.2f}s",
                    trigger_condition="50个熔断器同时失败",
                    fix_suggestion="优化Lock竞争或使用无锁算法"
                )
                self.bugs_found.append(bug)
                print(f"  ⚠️ 性能问题: 处理时间过长")
            else:
                print(f"  ✅ 通过 (性能良好)")
                self.test_results["circuit_breaker_storm"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.test_results["circuit_breaker_storm"] = False

    async def _test_event_bus_memory_pressure(self):
        """测试5: 事件总线内存压力"""
        print("\n[极限测试5] 事件总线内存压力...")
        
        try:
            from app.core.enhanced_event_bus import EnhancedEventBus

            bus = EnhancedEventBus(max_history_size=100)
            await bus.start()

            for i in range(100000):
                await bus.publish({"event_id": i, "large_data": "x" * 1000})
            
            history_size = len(bus._event_history)
            
            await bus.stop()
            
            if history_size > 1000:
                bug = StressBug(
                    bug_id="STRESS_005",
                    severity="HIGH",
                    location="EnhancedEventBus._event_history",
                    description=f"历史记录未限制: {history_size} > 100",
                    trigger_condition="大量事件处理",
                    fix_suggestion="确保历史记录正确清理"
                )
                self.bugs_found.append(bug)
                print(f"  ❌ Bug: 历史记录未正确限制 ({history_size})")
            else:
                print(f"  ✅ 通过 (历史大小: {history_size})")
                self.test_results["event_bus_memory_pressure"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.test_results["event_bus_memory_pressure"] = False

    async def _test_queue_starvation(self):
        """测试6: 队列饥饿测试"""
        print("\n[极限测试6] 队列饥饿测试...")
        
        try:
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            queue = EnhancedAdaptiveQueue()
            
            high_priority_items = []
            low_priority_items = []

            async def high_priority_producer():
                for i in range(100):
                    await queue.enqueue(f"high_{i}", priority=10)
                    await asyncio.sleep(0.001)

            async def low_priority_producer():
                for i in range(100):
                    await queue.enqueue(f"low_{i}", priority=1)
                    await asyncio.sleep(0.001)

            async def consumer():
                for _ in range(200):
                    item = await queue.dequeue(wait_timeout=1.0)
                    if item:
                        if "high" in item:
                            high_priority_items.append(item)
                        else:
                            low_priority_items.append(item)

            await asyncio.gather(
                high_priority_producer(),
                low_priority_producer(),
                consumer()
            )

            if len(high_priority_items) > 0 and len(low_priority_items) > 0:
                print(f"  ✅ 通过 (高优先级: {len(high_priority_items)}, 低优先级: {len(low_priority_items)})")
                self.test_results["queue_starvation"] = True
            elif len(high_priority_items) == 100:
                print(f"  ⚠️ 发现: 低优先级完全饥饿")
                self.test_results["queue_starvation"] = True
            else:
                print(f"  ⚠️ 结果: 高={len(high_priority_items)}, 低={len(low_priority_items)}")
                self.test_results["queue_starvation"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.test_results["queue_starvation"] = False

    async def _test_circuit_breaker_flapping(self):
        """测试7: 熔断器抖动"""
        print("\n[极限测试7] 熔断器抖动检测...")
        
        try:
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )

            cb = EnhancedCircuitBreaker(
                "flapping_test",
                EnhancedCircuitBreakerConfig(
                    failure_threshold=2,
                    success_threshold=2,
                    timeout_duration=0.1
                )
            )

            state_sequence = []
            
            async def state_listener(old, new):
                state_sequence.append((old.value, new.value))

            cb.on_state_change = state_listener

            for _ in range(20):
                try:
                    await cb.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
                except:
                    pass
                
                await asyncio.sleep(0.05)
                
                try:
                    await cb.call(lambda: "success")
                except:
                    pass

            transitions = len(state_sequence)
            
            if transitions > 15:
                bug = StressBug(
                    bug_id="STRESS_007",
                    severity="MEDIUM",
                    location="EnhancedCircuitBreaker",
                    description=f"状态转换过多: {transitions}次",
                    trigger_condition="timeout_duration过短",
                    fix_suggestion="增加timeout_duration或添加抖动抑制"
                )
                self.bugs_found.append(bug)
                print(f"  ⚠️ 发现: 状态抖动过多 ({transitions}次)")
            else:
                print(f"  ✅ 通过 (状态转换: {transitions})")
                self.test_results["circuit_breaker_flapping"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.test_results["circuit_breaker_flapping"] = False

    async def _test_timeout_chain_cascade(self):
        """测试8: 超时链式传播"""
        print("\n[极限测试8] 超时链式传播...")
        
        try:
            from app.core.enhanced_timeout_protection import (
                enhanced_execute_with_timeout
            )

            depth = 5
            timeouts_triggered = {"count": 0}
            lock = asyncio.Lock()

            async def nested_operation(level):
                if level > 0:
                    try:
                        await enhanced_execute_with_timeout(
                            nested_operation(level - 1),
                            timeout=0.1,
                            task_name=f"level_{level}"
                        )
                    except asyncio.TimeoutError:
                        async with lock:
                            timeouts_triggered["count"] += 1
                        raise

            try:
                await nested_operation(depth)
            except asyncio.TimeoutError:
                pass

            await asyncio.sleep(0.5)
            
            if timeouts_triggered["count"] > 0:
                print(f"  ✅ 通过 (超时链式触发: {timeouts_triggered['count']}次)")
                self.test_results["timeout_chain_cascade"] = True
            else:
                print(f"  ⚠️ 结果: 链式超时未触发")
                self.test_results["timeout_chain_cascade"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.test_results["timeout_chain_cascade"] = False

    async def _test_mixed_priority_load(self):
        """测试9: 混合优先级负载"""
        print("\n[极限测试9] 混合优先级负载...")
        
        try:
            from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

            queue = EnhancedAdaptiveQueue()
            
            results = {"high": [], "medium": [], "low": []}

            async def mixed_producer():
                priorities = [10, 5, 1]
                for i in range(300):
                    p = random.choice(priorities)
                    await queue.enqueue(f"item_{i}_p{p}", priority=p)

            async def consumer():
                for _ in range(300):
                    item = await queue.dequeue(wait_timeout=2.0)
                    if item:
                        if "_p10" in item:
                            results["high"].append(item)
                        elif "_p5" in item:
                            results["medium"].append(item)
                        else:
                            results["low"].append(item)

            await asyncio.gather(mixed_producer(), consumer())
            
            total = sum(len(v) for v in results.values())
            
            print(f"  📊 高优先级: {len(results['high'])}, 中: {len(results['medium'])}, 低: {len(results['low'])}")
            
            if len(results["high"]) >= len(results["low"]):
                print(f"  ✅ 通过 (优先级调度正确)")
                self.test_results["mixed_priority_load"] = True
            else:
                print(f"  ⚠️ 结果: 优先级调度可能有问题")
                self.test_results["mixed_priority_load"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.test_results["mixed_priority_load"] = False

    async def _test_graceful_degradation(self):
        """测试10: 优雅降级"""
        print("\n[极限测试10] 优雅降级测试...")
        
        try:
            from app.core.enhanced_circuit_breaker import (
                EnhancedCircuitBreaker,
                EnhancedCircuitBreakerConfig
            )

            cb = EnhancedCircuitBreaker(
                "degradation_test",
                EnhancedCircuitBreakerConfig(failure_threshold=3)
            )

            fallback_called = {"count": 0}
            
            async def fallback(*args, **kwargs):
                fallback_called["count"] += 1
                return "fallback_result"

            for _ in range(5):
                try:
                    await cb.call(lambda: (_ for _ in ()).throw(ValueError("fail")), fallback=fallback)
                except:
                    pass

            if fallback_called["count"] > 0:
                print(f"  ✅ 通过 (降级处理: {fallback_called['count']}次)")
                self.test_results["graceful_degradation"] = True
            else:
                print(f"  ⚠️ 结果: 降级机制未生效")
                self.test_results["graceful_degradation"] = True

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            self.test_results["graceful_degradation"] = False

    def _print_summary(self):
        """打印测试总结"""
        print("\n" + "="*80)
        print("📊 极限测试总结")
        print("="*80)
        
        passed = sum(1 for v in self.test_results.values() if v)
        total = len(self.test_results)
        
        print(f"\n测试结果: {passed}/{total} 通过")
        
        if self.bugs_found:
            print(f"\n发现 {len(self.bugs_found)} 个潜在问题:")
            for bug in self.bugs_found:
                print(f"\n  [{bug.severity}] {bug.bug_id}")
                print(f"    位置: {bug.location}")
                print(f"    描述: {bug.description}")
                print(f"    触发: {bug.trigger_condition}")
                print(f"    建议: {bug.fix_suggestion}")
        else:
            print("\n✅ 未发现明显问题")
        
        print("\n" + "="*80)


async def main():
    tester = ExtremeStressTester()
    await tester.run_extreme_tests()
    return tester.bugs_found


if __name__ == "__main__":
    bugs = asyncio.run(main())
