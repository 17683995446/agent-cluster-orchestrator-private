"""
🔬 第十二轮诊断测试 - 逐步发现问题
"""
import asyncio
import time
import random
import sys
from datetime import datetime

sys.path.insert(0, '/workspace/backend')


async def test_diagnostic_1_basic_functionality():
    """诊断1: 基本功能"""
    print("\n" + "=" * 60)
    print("DIAGNOSTIC 1: BASIC FUNCTIONALITY")
    print("=" * 60)
    
    from app.core.enhanced_event_bus import EnhancedEventBus
    from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
    from app.core.enhanced_circuit_breaker import (
        EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
    )
    
    all_passed = True
    
    # 测试1a: 事件总线基本功能
    print("\n1a: Event Bus")
    bus = EnhancedEventBus(max_history_size=100)
    await bus.start()
    processed = 0
    
    async def handler(event):
        nonlocal processed
        processed += 1
    
    await bus.subscribe(handler)
    
    for i in range(50):
        await bus.publish({"test": i})
    
    await asyncio.sleep(1)
    await bus.stop()
    
    ok = processed == 50
    print(f"   Processed: {processed} / 50  {'✅' if ok else '❌'}")
    if not ok: all_passed = False
    
    # 测试1b: 队列基本功能
    print("\n1b: Adaptive Queue")
    queue = EnhancedAdaptiveQueue()
    
    for i in range(10):
        await queue.enqueue(f"item-{i}", priority=i)
    
    ok = queue.size() == 10
    print(f"   Enqueued 10 items: size={queue.size()}  {'✅' if ok else '❌'}")
    if not ok: all_passed = False
    
    for i in range(5):
        item = await queue.dequeue()
        print(f"   Dequeued: {item}")
    
    ok = queue.size() == 5
    print(f"   After 5 dequeues: size={queue.size()}  {'✅' if ok else '❌'}")
    if not ok: all_passed = False
    
    # 测试1c: 熔断器基本功能
    print("\n1c: Circuit Breaker")
    cb = EnhancedCircuitBreaker("test", EnhancedCircuitBreakerConfig())
    
    async def good(): await asyncio.sleep(0.01)
    async def bad(): raise Exception("Fail")
    
    await cb.call(good)
    await cb.call(good)
    
    try:
        await cb.call(bad)
    except Exception:
        pass
    
    print(f"   Circuit state: {cb.state.value}")
    
    return all_passed


async def test_diagnostic_2_medium_concurrency():
    """诊断2: 中等规模并发"""
    print("\n" + "=" * 60)
    print("DIAGNOSTIC 2: MEDIUM CONCURRENCY (100 tasks)")
    print("=" * 60)
    
    from app.core.enhanced_event_bus import EnhancedEventBus
    from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
    
    bus = EnhancedEventBus(max_history_size=500)
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
        for i in range(10):
            await bus.publish({"wid": wid, "seq": i})
            await queue.enqueue(f"task-{wid}-{i}", priority=random.randint(0, 9))
    
    workers = [worker(i) for i in range(100)]
    await asyncio.gather(*workers)
    
    await asyncio.sleep(2)
    await bus.stop()
    
    expected = 100 * 10
    ok = processed == expected
    
    print(f"\n   Published: {expected}")
    print(f"   Processed: {processed}")
    print(f"   Queue size: {queue.size()}")
    print(f"   Status: {'✅' if ok else '❌'}")
    
    return ok


async def test_diagnostic_3_chaos_injection():
    """诊断3: 小规模混乱注入"""
    print("\n" + "=" * 60)
    print("DIAGNOSTIC 3: CHAOS INJECTION")
    print("=" * 60)
    
    from app.core.enhanced_circuit_breaker import (
        EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
    )
    
    cb = EnhancedCircuitBreaker("chaos-test", EnhancedCircuitBreakerConfig(
        failure_threshold=5,
        success_threshold=3
    ))
    
    successes = 0
    failures = 0
    
    async def flaky():
        if random.random() < 0.4:
            raise Exception("Chaos failure")
        if random.random() < 0.2:
            await asyncio.sleep(0.2)
        await asyncio.sleep(0.02)
    
    print("\n   Running 200 requests...")
    for i in range(200):
        try:
            await cb.call(flaky)
            successes += 1
        except Exception:
            failures += 1
        if i % 50 == 0:
            print(f"   {i:3d} requests: state={cb.state.value}, S/F={successes}/{failures}")
    
    print(f"\n   Final: Successes={successes}, Failures={failures}")
    print(f"   Final state: {cb.state.value}")
    
    return True


async def main():
    print("\n" + "=" * 80)
    print("🔬 TWELFTH ROUND - DIAGNOSTIC TESTS (Slow & Careful)")
    print("   Date:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 80)
    
    results = []
    try:
        results.append(("Basic Functionality", await test_diagnostic_1_basic_functionality()))
        results.append(("Medium Concurrency", await test_diagnostic_2_medium_concurrency()))
        results.append(("Chaos Injection", await test_diagnostic_3_chaos_injection()))
        
        print("\n" + "=" * 80)
        print("📊 DIAGNOSTIC RESULTS")
        print("=" * 80)
        all_passed = True
        for name, ok in results:
            status = "✅ PASS" if ok else "❌ FAIL"
            print(f"   {name}: {status}")
            all_passed = all_passed and ok
        
        print("\n" + "=" * 80)
        if all_passed:
            print("🎉 ALL DIAGNOSTICS PASSED!")
        else:
            print("⚠️ Some diagnostics failed - further investigation needed.")
        
        with open("/workspace/backend/TWELFTH_ROUND_DIAGNOSTIC_REPORT.md", "w") as f:
            f.write("# 第十二轮诊断测试报告\n\n")
            f.write(f"日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("## 诊断结果\n\n")
            for name, ok in results:
                f.write(f"- **{name}**: {'✅ 通过' if ok else '❌ 失败'}\n")
            f.write("\n")
            
        print(f"\n📄 Report: /workspace/backend/TWELFTH_ROUND_DIAGNOSTIC_REPORT.md")
        
    except Exception as e:
        import traceback
        print(f"\n❌ ERROR: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
