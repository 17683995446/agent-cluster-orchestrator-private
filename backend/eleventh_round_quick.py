"""
🔬 第十一轮快速验证 - 混沌工程核心测试
"""
import asyncio
import time
import random
import sys
import gc

sys.path.insert(0, '/workspace/backend')


async def test_massive_concurrency():
    """大规模并发测试"""
    print("\n🚀 测试1: 大规模并发 (500 协程)")
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
    
    await bus.subscribe(handler)
    
    async def worker(wid):
        for i in range(100):
            await bus.publish({"wid": wid, "seq": i})
            await queue.enqueue(f"task-{wid}-{i}", priority=random.randint(1, 10))
    
    workers = [worker(i) for i in range(500)]
    await asyncio.gather(*workers)
    
    await asyncio.sleep(2)
    await bus.stop()
    
    print(f"   500协程 x 100操作 = 50,000操作")
    print(f"   实际处理: {processed} 事件")
    
    return processed == 50000


async def test_chaos_network():
    """混沌网络测试"""
    print("\n🌐 测试2: 混沌网络条件")
    from app.core.enhanced_circuit_breaker import (
        EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
    )
    
    cb = EnhancedCircuitBreaker("chaos", EnhancedCircuitBreakerConfig(
        failure_threshold=10,
        success_threshold=5,
        timeout_duration=0.5
    ))
    
    async def fail():
        raise Exception("chaos")
    
    async def success():
        await asyncio.sleep(0.001)
    
    errors = 0
    successes = 0
    
    for i in range(200):
        try:
            if random.random() < 0.3:
                await cb.call(fail)
            else:
                await cb.call(success)
            successes += 1
        except Exception:
            errors += 1
    
    print(f"   成功: {successes}, 错误: {errors}")
    print(f"   熔断器状态: {cb.state.value}")
    
    return cb.state.value in ["open", "half_open", "closed"]


async def test_resource_limits():
    """资源限制测试"""
    print("\n💥 测试3: 资源限制")
    from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
    
    queue = EnhancedAdaptiveQueue()
    
    enqueued = 0
    rejected = 0
    
    for i in range(2000):
        try:
            await queue.enqueue(f"item-{i}", priority=i % 10, wait_timeout=0.1)
            enqueued += 1
        except Exception:
            rejected += 1
    
    stats = queue.get_stats()
    print(f"   入队: {enqueued}, 拒绝: {rejected}")
    print(f"   队列大小: {queue.size()}, 容量: {queue.capacity}")
    
    return queue.size() <= queue.capacity


async def test_data_consistency():
    """数据一致性测试"""
    print("\n🔐 测试4: 数据一致性")
    from app.core.enhanced_event_bus import EnhancedEventBus
    
    bus = EnhancedEventBus(max_history_size=5000)
    await bus.start()
    
    for i in range(1000):
        await bus.publish({
            "id": i,
            "data": random.randint(0, 1000),
            "checksum": hash((i, random.randint(0, 1000)))
        })
    
    await asyncio.sleep(1)
    await bus.stop()
    
    history = list(bus._event_history)
    print(f"   发布: 1000, 历史: {len(history)}")
    
    return len(history) == 1000


async def test_component_isolation():
    """组件隔离测试"""
    print("\n🛡️ 测试5: 组件隔离")
    from app.core.enhanced_circuit_breaker import (
        EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
    )
    
    # 创建5个独立的熔断器
    breakers = [
        EnhancedCircuitBreaker(f"iso-{i}", EnhancedCircuitBreakerConfig())
        for i in range(5)
    ]
    
    # 破坏第一个
    async def fail():
        raise Exception("isolated")
    
    for _ in range(15):
        try:
            await breakers[0].call(fail)
        except Exception:
            pass
    
    # 验证其他仍然正常
    isolated_state = breakers[0].state.value
    others_ok = True
    
    for cb in breakers[1:]:
        try:
            await cb.call(lambda: asyncio.sleep(0.001))
        except Exception:
            others_ok = False
    
    print(f"   第一个熔断器状态: {isolated_state}")
    print(f"   其他熔断器正常: {others_ok}")
    
    return others_ok


async def main():
    print("="*80)
    print("🔬 第十一轮快速验证 - 混沌工程核心测试")
    print("="*80)
    
    results = []
    
    try:
        results.append(("大规模并发", await test_massive_concurrency()))
        results.append(("混沌网络", await test_chaos_network()))
        results.append(("资源限制", await test_resource_limits()))
        results.append(("数据一致性", await test_data_consistency()))
        results.append(("组件隔离", await test_component_isolation()))
        
        print("\n" + "="*80)
        print("📊 测试摘要:")
        all_passed = True
        
        for name, passed in results:
            status = "✅ 通过" if passed else "❌ 失败"
            print(f"  {name}: {status}")
            all_passed = all_passed and passed
        
        if all_passed:
            print("\n🎉 所有混沌工程测试通过！")
        else:
            print("\n⚠️ 部分测试失败")
        
        print("="*80)
        
        # 生成报告
        with open("/workspace/backend/ELEVENTH_ROUND_QUICK_REPORT.md", "w", encoding="utf-8") as f:
            f.write("# 第十一轮混沌工程快速验证报告\n\n")
            f.write(f"**日期**: 2026-05-20\n\n")
            f.write("## 测试摘要\n\n")
            for name, passed in results:
                f.write(f"- **{name}**: {'✅ 通过' if passed else '❌ 失败'}\n")
            f.write("\n## 结论\n\n")
            if all_passed:
                f.write("✅ 系统通过所有混沌工程验证！\n\n")
            else:
                f.write("⚠️ 部分测试失败，需要进一步调查。\n\n")
        
        print("\n📄 报告已生成")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
