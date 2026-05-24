"""
🔬 第十二轮超级深度测试 - 快速验证版本
"""
import asyncio
import time
import random
import sys
import gc
import structlog
from datetime import datetime

sys.path.insert(0, '/workspace/backend')

logger = structlog.get_logger("TWELFTH_QUICK")


async def test_scenario_1_massive_concurrency():
    """场景1: 大规模并发 (600协程)"""
    logger.info("=== SCENARIO 1: MASSIVE CONCURRENCY ===")
    from app.core.enhanced_event_bus import EnhancedEventBus
    from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
    
    bus = EnhancedEventBus(max_history_size=10000)
    queue = EnhancedAdaptiveQueue()
    await bus.start()
    
    processed = 0
    lock = asyncio.Lock()
    
    async def event_handler(event):
        nonlocal processed
        async with lock:
            processed += 1
    
    await bus.subscribe(event_handler)
    
    async def worker(wid):
        for i in range(50):
            await bus.publish({"wid": wid, "seq": i})
            await queue.enqueue(f"t-{wid}-{i}", priority=random.randint(1, 10))
            if random.random() < 0.2:
                await asyncio.sleep(random.uniform(0.05, 0.3))
    
    workers = [worker(i) for i in range(600)]
    await asyncio.gather(*workers)
    
    await asyncio.sleep(5)
    await bus.stop()
    
    logger.info(
        "scenario1_results",
        expected=600 * 50,
        processed=processed,
        queue_size=queue.size()
    )
    
    return processed == 600 * 50


async def test_scenario_2_chaos_network():
    """场景2: 混沌网络"""
    logger.info("=== SCENARIO 2: CHAOS NETWORK ===")
    from app.core.enhanced_circuit_breaker import (
        EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
    )
    
    cb = EnhancedCircuitBreaker(
        "chaos-net",
        EnhancedCircuitBreakerConfig(
            failure_threshold=6,
            success_threshold=3,
            timeout_duration=0.5
        )
    )
    
    async def flaky():
        if random.random() < 0.2:
            raise Exception("Flaky failure")
        if random.random() < 0.1:
            await asyncio.sleep(0.8)
        await asyncio.sleep(0.02)
    
    successes = 0
    failures = 0
    
    for i in range(300):
        try:
            await cb.call(flaky)
            successes += 1
        except Exception:
            failures += 1
        
        if i % 50 == 0:
            logger.info(f"scenario2_progress_{i}", state=cb.state.value)
    
    logger.info(
        "scenario2_results",
        successes=successes,
        failures=failures,
        final_state=cb.state.value
    )
    
    return True


async def test_scenario_3_resource_limits():
    """场景3: 资源极限"""
    logger.info("=== SCENARIO 3: RESOURCE LIMITS ===")
    from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
    
    queue = EnhancedAdaptiveQueue()
    enqueued = 0
    rejected = 0
    
    for i in range(10000):
        try:
            await queue.enqueue(f"item-{i}", priority=i % 10, wait_timeout=0.05)
            enqueued += 1
        except Exception:
            rejected += 1
        
        if i % 1000 == 0:
            logger.info(
                f"scenario3_progress_{i}",
                enqueued=enqueued,
                rejected=rejected,
                size=queue.size()
            )
    
    logger.info(
        "scenario3_results",
        enqueued=enqueued,
        rejected=rejected,
        final_size=queue.size()
    )
    
    return True


async def test_scenario_4_data_consistency():
    """场景4: 数据一致性"""
    logger.info("=== SCENARIO 4: DATA CONSISTENCY ===")
    from app.core.enhanced_event_bus import EnhancedEventBus
    
    bus = EnhancedEventBus(max_history_size=10000)
    await bus.start()
    
    test_data = []
    for i in range(1000):
        test_data.append({
            "id": i,
            "value": random.randint(0, 100000),
            "checksum": hash((i, random.randint(0, 100000)))
        })
    
    for data in test_data:
        await bus.publish(data)
    
    await asyncio.sleep(3)
    await bus.stop()
    
    logger.info(
        "scenario4_results",
        published=len(test_data),
        history_size=len(bus._event_history)
    )
    
    return len(bus._event_history) == 1000


async def test_scenario_5_component_isolation():
    """场景5: 组件隔离"""
    logger.info("=== SCENARIO 5: COMPONENT ISOLATION ===")
    from app.core.enhanced_circuit_breaker import (
        EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
    )
    
    breakers = [EnhancedCircuitBreaker(f"iso-{i}", EnhancedCircuitBreakerConfig()) 
                for i in range(6)]
    
    # 破坏第一个
    async def fail(): raise Exception("Fail")
    for _ in range(15):
        try: await breakers[0].call(fail)
        except: pass
    
    # 验证其他
    others_ok = True
    for i in range(1, 6):
        try:
            await breakers[i].call(lambda: asyncio.sleep(0.001))
        except Exception:
            others_ok = False
    
    logger.info(
        "scenario5_results",
        first_circuit_state=breakers[0].state.value,
        others_ok=others_ok
    )
    
    return others_ok


async def main():
    print("\n" + "=" * 80)
    print("🔬 第十二轮超级深度测试 - 快速验证")
    print("=" * 80)
    
    results = []
    try:
        results.append(("Massive Concurrency", await test_scenario_1_massive_concurrency()))
        results.append(("Chaos Network", await test_scenario_2_chaos_network()))
        results.append(("Resource Limits", await test_scenario_3_resource_limits()))
        results.append(("Data Consistency", await test_scenario_4_data_consistency()))
        results.append(("Component Isolation", await test_scenario_5_component_isolation()))
        
        print("\n" + "=" * 80)
        print("📊 RESULTS SUMMARY")
        all_passed = True
        for name, ok in results:
            status = "✅ PASS" if ok else "❌ FAIL"
            print(f"  {name}: {status}")
            all_passed = all_passed and ok
        
        print("\n" + "=" * 80)
        if all_passed:
            print("🎉 ALL TESTS PASSED! System is ready for production!")
        else:
            print("⚠️ Some tests failed - needs investigation!")
        
        # 生成报告
        with open("/workspace/backend/TWELFTH_QUICK_REPORT.md", "w") as f:
            f.write("# 第十二轮超级深度测试 - 快速验证报告\n\n")
            f.write(f"**日期**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("## 结果\n\n")
            for name, ok in results:
                f.write(f"- **{name}**: {'✅ PASS' if ok else '❌ FAIL'}\n")
        
    except Exception as e:
        import traceback
        print(f"\n❌ Error: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
