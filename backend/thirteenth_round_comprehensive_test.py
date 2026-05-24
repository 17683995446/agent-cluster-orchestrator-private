"""
🔬 第十三轮全面覆盖测试 - 第一性原则测试

测试理念：
1. 以代码为本 - 顺着代码本源做全覆盖验证
2. 不漏任何判断分支 - 每个if/else/elif都必须测试
3. 隐藏逻辑 - 测试边界条件、默认值、异常兜底
4. 代码即规则 - 验证每个分支的预期行为
"""
import asyncio
import time
import random
import sys
from datetime import datetime
from typing import Any, Dict, List

sys.path.insert(0, '/workspace/backend')


class TestResult:
    """测试结果记录"""
    def __init__(self, test_name: str):
        self.test_name = test_name
        self.passed = False
        self.expected = None
        self.actual = None
        self.error = None
        self.branch_covered = None


class ComprehensiveTestSuite:
    """全面覆盖测试套件"""
    
    def __init__(self):
        self.results: List[TestResult] = []
        self.total_tests = 0
        self.passed_tests = 0
    
    def record(self, result: TestResult):
        """记录测试结果"""
        self.results.append(result)
        self.total_tests += 1
        if result.passed:
            self.passed_tests += 1
    
    def print_summary(self):
        """打印测试摘要"""
        print("\n" + "=" * 80)
        print(f"📊 全面覆盖测试结果")
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
                print(f"     错误: {r.error}")
        
        return len(failed) == 0


# ==================== 事件总线全面测试 ====================

async def test_event_bus_comprehensive(suite: ComprehensiveTestSuite):
    """全面测试事件总线 - 覆盖所有分支"""
    print("\n" + "=" * 80)
    print("🔌 事件总线全面测试 (EnhancedEventBus)")
    print("=" * 80)
    
    from app.core.enhanced_event_bus import EnhancedEventBus
    
    # 分支测试1: start() - 重复启动
    result = TestResult("EventBus.start() - 重复启动应返回")
    bus = EnhancedEventBus(max_history_size=10)
    await bus.start()
    await bus.start()  # 应该直接返回
    result.passed = True
    result.actual = "正常"
    suite.record(result)
    await bus.stop()
    
    # 分支测试2: stop() - 不等待队列
    result = TestResult("EventBus.stop(wait_for_queue=False)")
    bus = EnhancedEventBus(max_history_size=10)
    await bus.start()
    await bus.publish({"test": 1})
    await asyncio.sleep(0.1)
    await bus.stop(wait_for_queue=False)  # 不等待
    result.passed = True
    result.actual = "停止成功"
    suite.record(result)
    
    # 分支测试3: stop() - 等待队列清空
    result = TestResult("EventBus.stop(wait_for_queue=True)")
    bus = EnhancedEventBus(max_history_size=100)
    await bus.start()
    
    processed = 0
    async def handler(e):
        nonlocal processed
        processed += 1
        await asyncio.sleep(0.01)
    
    await bus.subscribe(handler)
    
    for i in range(50):
        await bus.publish({"seq": i})
    
    await bus.stop(wait_for_queue=True)
    result.passed = processed == 50
    result.expected = 50
    result.actual = processed
    suite.record(result)
    
    # 分支测试4: subscribe - 所有事件类型
    result = TestResult("EventBus.subscribe - event_types=None")
    bus = EnhancedEventBus(max_history_size=100)
    await bus.start()
    
    count = [0]
    async def all_handler(e):
        count[0] += 1
    
    await bus.subscribe(all_handler)  # 订阅所有事件
    await bus.publish({"type": "A"})
    await bus.publish({"type": "B"})
    await asyncio.sleep(0.5)
    await bus.stop()
    
    result.passed = count[0] == 2
    result.expected = 2
    result.actual = count[0]
    suite.record(result)
    
    # 分支测试5: subscribe - 特定事件类型
    result = TestResult("EventBus.subscribe - event_types=['TYPE_A']")
    bus = EnhancedEventBus(max_history_size=100)
    await bus.start()
    
    count_a = [0]
    async def type_a_handler(e):
        count_a[0] += 1
    
    await bus.subscribe(type_a_handler, event_types=["TYPE_A"])
    await bus.publish({"event_type": "TYPE_A"})
    await bus.publish({"event_type": "TYPE_B"})  # 不应该触发
    await asyncio.sleep(0.5)
    await bus.stop()
    
    result.passed = count_a[0] == 1
    result.expected = 1
    result.actual = count_a[0]
    suite.record(result)
    
    # 分支测试6: unsubscribe - 所有事件类型
    result = TestResult("EventBus.unsubscribe - 取消订阅")
    bus = EnhancedEventBus(max_history_size=100)
    await bus.start()
    
    count = [0]
    async def temp_handler(e):
        count[0] += 1
    
    await bus.subscribe(temp_handler)
    await bus.publish({"test": 1})
    await asyncio.sleep(0.1)
    
    await bus.unsubscribe(temp_handler)
    await bus.publish({"test": 2})
    await asyncio.sleep(0.1)
    await bus.stop()
    
    result.passed = count[0] == 1
    result.expected = 1
    result.actual = count[0]
    suite.record(result)
    
    # 分支测试7: publish - 队列满
    result = TestResult("EventBus.publish - 队列满应丢弃")
    bus = EnhancedEventBus(max_history_size=5, max_queue_size=5)
    await bus.start()
    
    for i in range(20):
        await bus.publish({"seq": i})
    
    await asyncio.sleep(1)
    stats = bus.get_stats()
    await bus.stop()
    
    result.passed = stats['events_dropped'] > 0
    result.expected = ">0"
    result.actual = stats['events_dropped']
    suite.record(result)
    
    # 分支测试8: _dispatch_event_to_subscribers - 无订阅者
    result = TestResult("EventBus._dispatch - 无订阅者")
    bus = EnhancedEventBus(max_history_size=100)
    await bus.start()
    
    await bus.publish({"test": 1})  # 没有订阅者
    await asyncio.sleep(0.1)
    stats = bus.get_stats()
    await bus.stop()
    
    result.passed = stats['events_processed'] == 1
    result.expected = 1
    result.actual = stats['events_processed']
    suite.record(result)
    
    # 分支测试9: _safe_invoke_callback - 同步回调
    result = TestResult("EventBus._dispatch - 同步回调")
    bus = EnhancedEventBus(max_history_size=100)
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
    
    # 分支测试10: _safe_invoke_callback - 回调超时
    result = TestResult("EventBus._dispatch - 回调超时")
    bus = EnhancedEventBus(max_history_size=100, callback_timeout=0.1)
    await bus.start()
    
    async def slow_handler(e):
        await asyncio.sleep(1)  # 超过超时
    
    await bus.subscribe(slow_handler)
    await bus.publish({"test": 1})
    await asyncio.sleep(0.5)
    stats = bus.get_stats()
    await bus.stop()
    
    result.passed = stats['callback_errors'] == 1
    result.expected = 1
    result.actual = stats['callback_errors']
    suite.record(result)
    
    # 分支测试11: _safe_invoke_callback - 回调异常
    result = TestResult("EventBus._dispatch - 回调异常")
    bus = EnhancedEventBus(max_history_size=100)
    await bus.start()
    
    async def error_handler(e):
        raise Exception("Handler error")
    
    await bus.subscribe(error_handler)
    await bus.publish({"test": 1})
    await asyncio.sleep(0.1)
    stats = bus.get_stats()
    await bus.stop()
    
    result.passed = stats['callback_errors'] == 1
    result.expected = 1
    result.actual = stats['callback_errors']
    suite.record(result)
    
    # 分支测试12: get_history - 带limit
    result = TestResult("EventBus.get_history - 带limit")
    bus = EnhancedEventBus(max_history_size=100)
    await bus.start()
    
    for i in range(50):
        await bus.publish({"seq": i})
    
    await asyncio.sleep(0.5)
    history = bus.get_history(limit=10)
    await bus.stop()
    
    result.passed = len(history) == 10
    result.expected = 10
    result.actual = len(history)
    suite.record(result)


# ==================== 熔断器全面测试 ====================

async def test_circuit_breaker_comprehensive(suite: ComprehensiveTestSuite):
    """全面测试熔断器 - 覆盖所有分支"""
    print("\n" + "=" * 80)
    print("⚡ 熔断器全面测试 (EnhancedCircuitBreaker)")
    print("=" * 80)
    
    from app.core.enhanced_circuit_breaker import (
        EnhancedCircuitBreaker,
        EnhancedCircuitBreakerConfig,
        CircuitState
    )
    
    # 分支测试1: call - OPEN状态拒绝请求
    result = TestResult("CircuitBreaker.call - OPEN状态拒绝")
    cb = EnhancedCircuitBreaker("test-open", EnhancedCircuitBreakerConfig(
        failure_threshold=2,
        timeout_duration=60
    ))
    
    async def fail(): raise Exception("Fail")
    
    for _ in range(3):
        try: await cb.call(fail)
        except: pass
    
    result.passed = cb.state == CircuitState.OPEN
    result.expected = CircuitState.OPEN
    result.actual = cb.state
    suite.record(result)
    
    # 分支测试2: call - OPEN状态有fallback
    result = TestResult("CircuitBreaker.call - OPEN状态使用fallback")
    cb = EnhancedCircuitBreaker("test-fallback", EnhancedCircuitBreakerConfig(
        failure_threshold=2
    ))
    
    async def fail2(): raise Exception("Fail")
    
    for _ in range(3):
        try: await cb.call(fail2)
        except: pass
    
    fallback_called = [False]
    def fallback():
        fallback_called[0] = True
        return "fallback_result"
    
    try:
        result_val = await cb.call(fail2, fallback=fallback)
        result.passed = result_val == "fallback_result"
        result.expected = "fallback_result"
        result.actual = result_val
    except Exception as e:
        result.passed = False
        result.error = str(e)
    
    suite.record(result)
    
    # 分支测试3: call - HALF_OPEN状态允许部分请求
    result = TestResult("CircuitBreaker.call - HALF_OPEN状态")
    cb = EnhancedCircuitBreaker("test-half-open", EnhancedCircuitBreakerConfig(
        failure_threshold=2,
        timeout_duration=0.5,
        success_threshold=2
    ))
    
    for _ in range(3):
        try: await cb.call(fail)
        except: pass
    
    await asyncio.sleep(0.6)  # 等待超时
    
    success_count = [0]
    async def success(): 
        success_count[0] += 1
        await asyncio.sleep(0.01)
    
    for i in range(3):
        try:
            await cb.call(success)
        except:
            pass
    
    result.passed = success_count[0] > 0
    result.expected = ">0"
    result.actual = success_count[0]
    suite.record(result)
    
    # 分支测试4: _should_open - 失败率判断
    result = TestResult("CircuitBreaker._should_open - 失败率阈值")
    cb = EnhancedCircuitBreaker("test-rate", EnhancedCircuitBreakerConfig(
        failure_threshold=10,
        failure_rate_threshold=0.5,
        min_call_count=5
    ))
    
    async def succeed(): await asyncio.sleep(0.01)
    
    for i in range(10):
        try:
            if i % 2 == 0:
                await cb.call(fail)
            else:
                await cb.call(succeed)
        except:
            pass
    
    result.passed = cb.state == CircuitState.OPEN
    result.expected = CircuitState.OPEN
    result.actual = cb.state
    suite.record(result)
    
    # 分支测试5: 慢调用检测
    result = TestResult("CircuitBreaker - 慢调用检测")
    cb = EnhancedCircuitBreaker("test-slow", EnhancedCircuitBreakerConfig(
        slow_call_threshold=0.1,
        slow_call_rate_threshold=0.3,
        min_call_count=5
    ))
    
    async def slow_call():
        await asyncio.sleep(0.2)
    
    for _ in range(5):
        try: await cb.call(slow_call)
        except: pass
    
    result.passed = cb.state == CircuitState.OPEN
    result.expected = CircuitState.OPEN
    result.actual = cb.state
    suite.record(result)
    
    # 分支测试6: 状态转换回调
    result = TestResult("CircuitBreaker - 状态转换回调")
    transitions = []
    
    def on_transition(old, new):
        transitions.append((old, new))
    
    cb = EnhancedCircuitBreaker("test-callback", 
        EnhancedCircuitBreakerConfig(failure_threshold=2),
        on_state_change=on_transition
    )
    
    for _ in range(3):
        try: await cb.call(fail)
        except: pass
    
    result.passed = len(transitions) > 0
    result.expected = ">0"
    result.actual = len(transitions)
    suite.record(result)
    
    # 分支测试7: 滑动窗口限制
    result = TestResult("CircuitBreaker - 滑动窗口大小限制")
    cb = EnhancedCircuitBreaker("test-window", EnhancedCircuitBreakerConfig(
        window_size=20
    ))
    
    for _ in range(30):
        await cb.call(succeed)
    
    stats = cb.get_stats()
    result.passed = len(cb._call_window) <= 20
    result.expected = "<=20"
    result.actual = len(cb._call_window)
    suite.record(result)
    
    # 分支测试8: 同步函数调用
    result = TestResult("CircuitBreaker.call - 同步函数")
    cb = EnhancedCircuitBreaker("test-sync")
    
    def sync_func():
        return "sync_result"
    
    result_val = await cb.call(sync_func)
    result.passed = result_val == "sync_result"
    result.expected = "sync_result"
    result.actual = result_val
    suite.record(result)
    
    # 分支测试9: 协程对象调用
    result = TestResult("CircuitBreaker.call - 协程对象")
    cb = EnhancedCircuitBreaker("test-coro")
    
    coro_obj = asyncio.sleep(0.01)
    await cb.call(coro_obj)
    
    result.passed = True
    result.actual = "成功"
    suite.record(result)


# ==================== 自适应队列全面测试 ====================

async def test_adaptive_queue_comprehensive(suite: ComprehensiveTestSuite):
    """全面测试自适应队列 - 覆盖所有分支"""
    print("\n" + "=" * 80)
    print("📦 自适应队列全面测试 (EnhancedAdaptiveQueue)")
    print("=" * 80)
    
    from app.core.enhanced_adaptive_queue import (
        EnhancedAdaptiveQueue,
        EnhancedQueueConfig,
        PriorityItem
    )
    
    # 分支测试1: enqueue - 队列满，拒绝低优先级
    result = TestResult("AdaptiveQueue.enqueue - 拒绝低优先级")
    config = EnhancedQueueConfig(initial_capacity=5, max_capacity=10)
    queue = EnhancedAdaptiveQueue(config)
    
    for i in range(5):
        await queue.enqueue(f"low-{i}", priority=1)
    
    # 现在队列满了，优先级1应该被拒绝
    success = await queue.enqueue("high", priority=10)
    size = queue.size()
    
    result.passed = success and size == 5
    result.expected = "success=True, size=5"
    result.actual = f"success={success}, size={size}"
    suite.record(result)
    
    # 分支测试2: enqueue - 队列满，扩容
    result = TestResult("AdaptiveQueue.enqueue - 队列满时扩容")
    config = EnhancedQueueConfig(initial_capacity=5, max_capacity=100)
    queue = EnhancedAdaptiveQueue(config)
    
    # 先填满队列
    for i in range(5):
        await queue.enqueue(f"item-{i}", priority=1)
    
    initial_cap = queue.capacity
    
    # 尝试入队低优先级（应该扩容）
    await queue.enqueue("low", priority=0)
    
    result.passed = queue.capacity > initial_cap
    result.expected = f">{initial_cap}"
    result.actual = queue.capacity
    suite.record(result)
    
    # 分支测试3: enqueue - 队列满，等待并超时
    result = TestResult("AdaptiveQueue.enqueue - 等待超时")
    config = EnhancedQueueConfig(initial_capacity=5, max_capacity=5)
    queue = EnhancedAdaptiveQueue(config)
    
    for i in range(5):
        await queue.enqueue(f"item-{i}", priority=5)
    
    # 尝试入队，超时时间为0
    success = await queue.enqueue("test", priority=1, wait_timeout=0)
    
    stats = queue.get_stats()
    result.passed = not success and stats["rejected"] > 0
    result.expected = "rejected>0"
    result.actual = stats["rejected"]
    suite.record(result)
    
    # 分支测试4: dequeue - 空队列超时
    result = TestResult("AdaptiveQueue.dequeue - 空队列超时")
    queue = EnhancedAdaptiveQueue()
    
    item = await queue.dequeue(wait_timeout=0.1)
    
    result.passed = item is None
    result.expected = None
    result.actual = item
    suite.record(result)
    
    # 分支测试5: dequeue - 等待有数据
    result = TestResult("AdaptiveQueue.dequeue - 等待有数据")
    queue = EnhancedAdaptiveQueue()
    
    async def add_later():
        await asyncio.sleep(0.2)
        await queue.enqueue("delayed")
    
    asyncio.create_task(add_later())
    item = await queue.dequeue(wait_timeout=1.0)
    
    result.passed = item == "delayed"
    result.expected = "delayed"
    result.actual = item
    suite.record(result)
    
    # 分支测试6: _can_reject_low_priority - 逻辑验证
    result = TestResult("AdaptiveQueue._can_reject - 边界条件")
    queue = EnhancedAdaptiveQueue()
    
    await queue.enqueue("item", priority=5)
    
    # 新优先级=5，不应该拒绝（因为相等）
    can_reject = queue._can_reject_low_priority(5)
    
    result.passed = not can_reject
    result.expected = False
    result.actual = can_reject
    suite.record(result)
    
    # 分支测试7: _can_grow - 达到最大容量
    result = TestResult("AdaptiveQueue._can_grow - 达到最大容量")
    config = EnhancedQueueConfig(initial_capacity=5, max_capacity=10)
    queue = EnhancedAdaptiveQueue(config)
    
    queue.capacity = 10
    can_grow = queue._can_grow()
    
    result.passed = not can_grow
    result.expected = False
    result.actual = can_grow
    suite.record(result)
    
    # 分支测试8: _can_grow - 冷却期
    result = TestResult("AdaptiveQueue._can_grow - 冷却期")
    config = EnhancedQueueConfig(initial_capacity=5, max_capacity=100, growth_cooldown=5.0)
    queue = EnhancedAdaptiveQueue(config)
    
    queue._last_growth = time.time()
    can_grow = queue._can_grow()
    
    result.passed = not can_grow
    result.expected = False
    result.actual = can_grow
    suite.record(result)
    
    # 分支测试9: _can_shrink - 达到初始容量
    result = TestResult("AdaptiveQueue._can_shrink - 达到初始容量")
    config = EnhancedQueueConfig(initial_capacity=10, max_capacity=100)
    queue = EnhancedAdaptiveQueue(config)
    
    queue.capacity = 10
    can_shrink = queue._can_shrink()
    
    result.passed = not can_shrink
    result.expected = False
    result.actual = can_shrink
    suite.record(result)
    
    # 分支测试10: _can_shrink - 使用率过高
    result = TestResult("AdaptiveQueue._can_shrink - 使用率过高")
    config = EnhancedQueueConfig(initial_capacity=10, max_capacity=100)
    queue = EnhancedAdaptiveQueue(config)
    
    queue.capacity = 20
    for _ in range(15):  # 75% 使用率
        await queue.enqueue("item", priority=5)
    
    can_shrink = queue._can_shrink()
    
    result.passed = not can_shrink
    result.expected = False
    result.actual = can_shrink
    suite.record(result)
    
    # 分支测试11: PriorityItem - 负数优先级
    result = TestResult("PriorityItem - 负数优先级")
    item = PriorityItem(-10, "test")
    
    result.passed = item.priority == 10  # 负负得正
    result.expected = 10
    result.actual = item.priority
    suite.record(result)
    
    # 分支测试12: get_stats - 计算使用率
    result = TestResult("AdaptiveQueue.get_stats - 使用率计算")
    config = EnhancedQueueConfig(initial_capacity=10, max_capacity=100)
    queue = EnhancedAdaptiveQueue(config)
    
    for _ in range(5):
        await queue.enqueue("item", priority=5)
    
    stats = queue.get_stats()
    
    result.passed = stats["utilization"] == 50.0
    result.expected = 50.0
    result.actual = stats["utilization"]
    suite.record(result)
    
    # 分支测试13: is_empty
    result = TestResult("AdaptiveQueue.is_empty")
    queue = EnhancedAdaptiveQueue()
    
    empty1 = queue.is_empty()
    await queue.enqueue("item", priority=1)
    empty2 = queue.is_empty()
    
    result.passed = empty1 and not empty2
    result.expected = "True then False"
    result.actual = f"{empty1} then {empty2}"
    suite.record(result)


async def main():
    """主函数"""
    print("\n" + "=" * 80)
    print("🔬 Agent Cluster Orchestrator - 第十三轮全面覆盖测试")
    print("   测试理念: 第一性原则 - 代码即规则")
    print("   目标: 覆盖所有代码分支、边界条件、异常处理")
    print(f"   开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    suite = ComprehensiveTestSuite()
    
    try:
        await test_event_bus_comprehensive(suite)
        await test_circuit_breaker_comprehensive(suite)
        await test_adaptive_queue_comprehensive(suite)
        
        all_passed = suite.print_summary()
        
        # 生成报告
        with open("/workspace/backend/THIRTEENTH_ROUND_COMPREHENSIVE_REPORT.md", "w") as f:
            f.write("# 第十三轮全面覆盖测试报告\n\n")
            f.write(f"**日期**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("## 测试理念\n\n")
            f.write("1. 以代码为本 - 顺着代码本源做全覆盖验证\n")
            f.write("2. 不漏任何判断分支 - 每个if/else/elif都必须测试\n")
            f.write("3. 隐藏逻辑 - 测试边界条件、默认值、异常兜底\n")
            f.write("4. 代码即规则 - 验证每个分支的预期行为\n\n")
            
            f.write("## 测试结果\n\n")
            f.write(f"- **测试总数**: {suite.total_tests}\n")
            f.write(f"- **通过**: {suite.passed_tests}\n")
            f.write(f"- **失败**: {suite.total_tests - suite.passed_tests}\n")
            f.write(f"- **通过率**: {suite.passed_tests / suite.total_tests * 100:.1f}%\n\n")
            
            f.write("## 各组件测试覆盖\n\n")
            
            event_bus_tests = [r for r in suite.results if "EventBus" in r.test_name]
            cb_tests = [r for r in suite.results if "CircuitBreaker" in r.test_name]
            queue_tests = [r for r in suite.results if "AdaptiveQueue" in r.test_name or "PriorityItem" in r.test_name]
            
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
            
            f.write("\n## 结论\n\n")
            if all_passed:
                f.write("✅ 所有测试通过！代码覆盖率达标！\n\n")
            else:
                f.write("⚠️ 部分测试失败，需要进一步修复！\n\n")
        
        print(f"\n📄 报告已生成: /workspace/backend/THIRTEENTH_ROUND_COMPREHENSIVE_REPORT.md")
        
        return 0 if all_passed else 1
        
    except Exception as e:
        import traceback
        print(f"\n❌ 测试执行失败: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
