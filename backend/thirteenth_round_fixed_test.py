"""
🔬 第十三轮全面覆盖测试 - 修复版本
"""
import asyncio
import time
import sys
from datetime import datetime
from typing import List

sys.path.insert(0, '/workspace/backend')


class TestResult:
    def __init__(self, test_name: str):
        self.test_name = test_name
        self.passed = False
        self.expected = None
        self.actual = None
        self.error = None


class ComprehensiveTestSuite:
    def __init__(self):
        self.results: List[TestResult] = []
        self.total_tests = 0
        self.passed_tests = 0
    
    def record(self, result: TestResult):
        self.results.append(result)
        self.total_tests += 1
        if result.passed:
            self.passed_tests += 1
    
    def print_summary(self):
        print("\n" + "=" * 80)
        print(f"📊 测试结果")
        print(f"   测试总数: {self.total_tests}")
        print(f"   通过: {self.passed_tests}")
        print(f"   失败: {self.total_tests - self.passed_tests}")
        print(f"   通过率: {self.passed_tests / self.total_tests * 100:.1f}%")
        print("=" * 80)
        
        failed = [r for r in self.results if not r.passed]
        if failed:
            print("\n❌ 失败的测试:")
            for r in failed:
                print(f"   - {r.test_name}")
                print(f"     预期: {r.expected}")
                print(f"     实际: {r.actual}")
                if r.error:
                    print(f"     错误: {r.error}")
        
        return len(failed) == 0


async def test_event_bus(suite):
    """事件总线测试"""
    print("\n🔌 事件总线测试")
    from app.core.enhanced_event_bus import EnhancedEventBus
    
    # 1. 基本功能
    result = TestResult("EventBus - 基本发布订阅")
    bus = EnhancedEventBus(max_history_size=100)
    await bus.start()
    
    processed = 0
    async def handler(e):
        nonlocal processed
        processed += 1
    
    await bus.subscribe(handler)
    for i in range(50):
        await bus.publish({"seq": i})
    
    await asyncio.sleep(1)
    await bus.stop()
    
    result.passed = processed == 50
    result.expected = 50
    result.actual = processed
    suite.record(result)
    
    # 2. 队列满丢弃
    result = TestResult("EventBus - 队列满丢弃")
    bus = EnhancedEventBus(max_queue_size=5)
    await bus.start()
    
    async def slow_handler(e):
        await asyncio.sleep(0.5)
    
    await bus.subscribe(slow_handler)
    
    for i in range(10):
        await bus.publish({"seq": i})
    
    await asyncio.sleep(1)
    stats = bus.get_stats()
    await bus.stop()
    
    result.passed = stats['events_dropped'] > 0
    result.expected = ">0"
    result.actual = stats['events_dropped']
    suite.record(result)
    
    # 3. 同步回调
    result = TestResult("EventBus - 同步回调")
    bus = EnhancedEventBus()
    await bus.start()
    
    count = [0]
    def sync_handler(e):
        count[0] += 1
    
    await bus.subscribe(sync_handler)
    await bus.publish({"test": 1})
    await asyncio.sleep(0.1)
    await bus.stop()
    
    result.passed = count[0] == 1
    result.expected = 1
    result.actual = count[0]
    suite.record(result)
    
    # 4. 回调超时
    result = TestResult("EventBus - 回调超时")
    bus = EnhancedEventBus(callback_timeout=0.1)
    await bus.start()
    
    async def slow(e):
        await asyncio.sleep(1)
    
    await bus.subscribe(slow)
    await bus.publish({"test": 1})
    await asyncio.sleep(0.3)
    stats = bus.get_stats()
    await bus.stop()
    
    result.passed = stats['callback_errors'] == 1
    result.expected = 1
    result.actual = stats['callback_errors']
    suite.record(result)
    
    # 5. 回调异常
    result = TestResult("EventBus - 回调异常")
    bus = EnhancedEventBus()
    await bus.start()
    
    async def error(e):
        raise Exception("Handler error")
    
    await bus.subscribe(error)
    await bus.publish({"test": 1})
    await asyncio.sleep(0.1)
    stats = bus.get_stats()
    await bus.stop()
    
    result.passed = stats['callback_errors'] == 1
    result.expected = 1
    result.actual = stats['callback_errors']
    suite.record(result)


async def test_circuit_breaker(suite):
    """熔断器测试"""
    print("\n⚡ 熔断器测试")
    from app.core.enhanced_circuit_breaker import (
        EnhancedCircuitBreaker,
        EnhancedCircuitBreakerConfig,
        CircuitState
    )
    
    # 1. OPEN状态
    result = TestResult("CircuitBreaker - OPEN状态")
    cb = EnhancedCircuitBreaker("test-open", EnhancedCircuitBreakerConfig(
        failure_threshold=3,
        min_call_count=3
    ))
    
    async def fail():
        raise Exception("Fail")
    
    for _ in range(5):
        try:
            await cb.call(fail)
        except:
            pass
    
    result.passed = cb.state == CircuitState.OPEN
    result.expected = "OPEN"
    result.actual = str(cb.state)
    suite.record(result)
    
    # 2. OPEN状态拒绝
    result = TestResult("CircuitBreaker - OPEN状态拒绝")
    cb = EnhancedCircuitBreaker("test-reject", EnhancedCircuitBreakerConfig(
        failure_threshold=2,
        min_call_count=2
    ))
    
    for _ in range(3):
        try:
            await cb.call(fail)
        except:
            pass
    
    rejected = False
    try:
        await cb.call(fail)
    except Exception as e:
        if "OPEN" in str(e):
            rejected = True
    
    result.passed = rejected
    result.expected = "True"
    result.actual = str(rejected)
    suite.record(result)
    
    # 3. OPEN状态fallback
    result = TestResult("CircuitBreaker - OPEN状态fallback")
    cb = EnhancedCircuitBreaker("test-fallback", EnhancedCircuitBreakerConfig(
        failure_threshold=2,
        min_call_count=2
    ))
    
    for _ in range(3):
        try:
            await cb.call(fail)
        except:
            pass
    
    def fallback():
        return "fallback_result"
    
    try:
        result_val = await cb.call(fail, fallback=fallback)
        result.passed = result_val == "fallback_result"
        result.expected = "fallback_result"
        result.actual = result_val
    except Exception as e:
        result.passed = False
        result.error = str(e)
    
    suite.record(result)
    
    # 4. 状态转换
    result = TestResult("CircuitBreaker - 状态转换回调")
    transitions = []
    
    def on_transition(old, new):
        transitions.append((old, new))
    
    cb = EnhancedCircuitBreaker("test-cb", 
        EnhancedCircuitBreakerConfig(failure_threshold=2, min_call_count=2),
        on_state_change=on_transition
    )
    
    for _ in range(3):
        try:
            await cb.call(fail)
        except:
            pass
    
    result.passed = len(transitions) > 0
    result.expected = ">0"
    result.actual = len(transitions)
    suite.record(result)
    
    # 5. 慢调用检测
    result = TestResult("CircuitBreaker - 慢调用检测")
    cb = EnhancedCircuitBreaker("test-slow", EnhancedCircuitBreakerConfig(
        slow_call_threshold=0.01,
        slow_call_rate_threshold=0.5,
        min_call_count=2
    ))
    
    async def slow():
        await asyncio.sleep(0.15)
    
    for _ in range(4):
        try:
            await cb.call(slow)
        except:
            pass
    
    result.passed = cb.state == CircuitState.OPEN
    result.expected = "OPEN"
    result.actual = str(cb.state)
    suite.record(result)


async def test_adaptive_queue(suite):
    """自适应队列测试"""
    print("\n📦 自适应队列测试")
    from app.core.enhanced_adaptive_queue import (
        EnhancedAdaptiveQueue,
        EnhancedQueueConfig
    )
    
    # 1. 基本功能
    result = TestResult("AdaptiveQueue - 基本入队出队")
    queue = EnhancedAdaptiveQueue()
    
    for i in range(10):
        await queue.enqueue(f"item-{i}", priority=i)
    
    items = []
    for _ in range(5):
        item = await queue.dequeue()
        items.append(item)
    
    result.passed = len(items) == 5
    result.expected = 5
    result.actual = len(items)
    suite.record(result)
    
    # 2. 拒绝低优先级
    result = TestResult("AdaptiveQueue - 拒绝低优先级")
    config = EnhancedQueueConfig(initial_capacity=5, max_capacity=10)
    queue = EnhancedAdaptiveQueue(config)
    
    for i in range(5):
        await queue.enqueue(f"low-{i}", priority=1)
    
    await queue.enqueue("high", priority=10)
    
    result.passed = queue.size() == 5
    result.expected = 5
    result.actual = queue.size()
    suite.record(result)
    
    # 3. 扩容
    result = TestResult("AdaptiveQueue - 自动扩容")
    config = EnhancedQueueConfig(initial_capacity=5, max_capacity=100)
    queue = EnhancedAdaptiveQueue(config)
    
    # 所有项目优先级相同，新项目优先级更低，会触发扩容
    for i in range(6):
        await queue.enqueue(f"item-{i}", priority=5)
    
    # 现在队列满，新项目优先级为1（更低），会扩容
    await queue.enqueue("new-item", priority=1)
    
    result.passed = queue.capacity > 5
    result.expected = ">5"
    result.actual = queue.capacity
    suite.record(result)
    
    # 4. 等待超时
    result = TestResult("AdaptiveQueue - 等待超时")
    config = EnhancedQueueConfig(initial_capacity=5, max_capacity=5)
    queue = EnhancedAdaptiveQueue(config)
    
    for i in range(5):
        await queue.enqueue(f"item-{i}", priority=5)
    
    success = await queue.enqueue("test", priority=1, wait_timeout=0)
    
    result.passed = not success
    result.expected = "False"
    result.actual = str(success)
    suite.record(result)
    
    # 5. 空队列超时
    result = TestResult("AdaptiveQueue - 空队列超时")
    queue = EnhancedAdaptiveQueue()
    
    item = await queue.dequeue(wait_timeout=0.1)
    
    result.passed = item is None
    result.expected = "None"
    result.actual = str(item)
    suite.record(result)
    
    # 6. 边界条件
    result = TestResult("AdaptiveQueue - 边界条件(相等优先级)")
    queue = EnhancedAdaptiveQueue()
    
    await queue.enqueue("item", priority=5)
    
    can_reject = queue._can_reject_low_priority(5)
    
    result.passed = can_reject
    result.expected = "True"
    result.actual = str(can_reject)
    suite.record(result)


async def main():
    print("\n" + "=" * 80)
    print("🔬 第十三轮全面覆盖测试 - 修复版本")
    print(f"   开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    suite = ComprehensiveTestSuite()
    
    try:
        await test_event_bus(suite)
        await test_circuit_breaker(suite)
        await test_adaptive_queue(suite)
        
        all_passed = suite.print_summary()
        
        with open("/workspace/backend/THIRTEENTH_ROUND_FIXED_REPORT.md", "w") as f:
            f.write("# 第十三轮全面覆盖测试报告（修复版）\n\n")
            f.write(f"**日期**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"- **测试总数**: {suite.total_tests}\n")
            f.write(f"- **通过**: {suite.passed_tests}\n")
            f.write(f"- **失败**: {suite.total_tests - suite.passed_tests}\n")
            f.write(f"- **通过率**: {suite.passed_tests / suite.total_tests * 100:.1f}%\n\n")
            
            f.write("## 各组件测试\n\n")
            
            event_bus_tests = [r for r in suite.results if "EventBus" in r.test_name]
            cb_tests = [r for r in suite.results if "CircuitBreaker" in r.test_name]
            queue_tests = [r for r in suite.results if "AdaptiveQueue" in r.test_name]
            
            f.write(f"### 事件总线 ({len(event_bus_tests)} 测试)\n\n")
            for r in event_bus_tests:
                status = "✅" if r.passed else "❌"
                f.write(f"- {status} {r.test_name}\n")
            
            f.write(f"\n### 熔断器 ({len(cb_tests)} 测试)\n\n")
            for r in cb_tests:
                status = "✅" if r.passed else "❌"
                f.write(f"- {status} {r.test_name}\n")
            
            f.write(f"\n### 自适应队列 ({len(queue_tests)} 测试)\n\n")
            for r in queue_tests:
                status = "✅" if r.passed else "❌"
                f.write(f"- {status} {r.test_name}\n")
            
            if all_passed:
                f.write("\n✅ 所有测试通过！\n")
            else:
                f.write("\n⚠️ 部分测试失败。\n")
        
        print(f"\n📄 报告: /workspace/backend/THIRTEENTH_ROUND_FIXED_REPORT.md")
        
        return 0 if all_passed else 1
        
    except Exception as e:
        import traceback
        print(f"\n❌ 错误: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(asyncio.run(main()))
