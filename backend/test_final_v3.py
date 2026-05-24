"""
最终验证测试 - 完整版
"""
import asyncio
import sys

sys.path.insert(0, '/workspace/backend')

from app.core.enhanced_event_bus import EnhancedEventBus
from app.core.enhanced_circuit_breaker import (
    EnhancedCircuitBreaker, EnhancedCircuitBreakerConfig
)
from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue
from app.core.enhanced_timeout_protection import (
    EnhancedTimeoutMonitor, enhanced_execute_with_timeout
)


async def test_event_bus():
    print("\n📡 测试事件总线...")
    bus = EnhancedEventBus(max_history_size=1000)
    await bus.start()
    
    processed = []
    async def handler(event):
        processed.append(event)
    
    await bus.subscribe(handler)
    
    for i in range(100):
        await bus.publish({"i": i})
    
    await asyncio.sleep(0.5)
    await bus.stop()
    
    assert len(processed) == 100, f"Expected 100, got {len(processed)}"
    print(f"✅ 事件总线: {len(processed)} 事件已处理")
    return True


async def test_circuit_breaker():
    print("\n⚡ 测试熔断器...")
    cb = EnhancedCircuitBreaker(
        "test",
        EnhancedCircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout_duration=0.5,
            min_call_count=3
        )
    )
    
    # 触发熔断
    async def fail_func():
        raise Exception("fail")
    
    async def success_func():
        await asyncio.sleep(0.01)
    
    for _ in range(5):
        try:
            await cb.call(fail_func)
        except Exception:
            pass
    
    assert cb.state.value == "open", "应该是打开状态"
    print(f"✅ 熔断器: {cb.state.value} (正确)")
    
    # 等待恢复
    await asyncio.sleep(0.7)
    
    # 测试半开和关闭
    recovered = False
    for _ in range(5):
        try:
            await cb.call(success_func)
            if cb.state.value == "closed":
                recovered = True
                break
        except Exception:
            pass
    
    assert recovered, "应该能恢复"
    print(f"✅ 熔断器: 已恢复到 {cb.state.value}")
    return True


async def test_queue():
    print("\n📦 测试队列...")
    queue = EnhancedAdaptiveQueue()
    
    for i in range(500):
        await queue.enqueue(f"item-{i}", priority=i % 10, wait_timeout=0.1)
    
    stats = queue.get_stats()
    print(f"✅ 队列: size={queue.size()}, capacity={queue.capacity}, "
          f"enqueued={stats['enqueued']}, rejected={stats['rejected']}")
    
    # 测试出队
    dequeued = 0
    while queue.size() > 0:
        item = await queue.dequeue(wait_timeout=0.1)
        if item:
            dequeued += 1
    
    stats = queue.get_stats()
    assert dequeued == stats['dequeued'], f"出队数不匹配"
    print(f"✅ 队列: 出队 {dequeued} 项")
    return True


async def test_timeout():
    print("\n⏱️ 测试超时保护...")
    monitor = EnhancedTimeoutMonitor()
    
    # 快速任务
    result = await enhanced_execute_with_timeout(
        asyncio.sleep(0.01), timeout=1, monitor=monitor
    )
    assert result is None, "快速任务应成功"
    print("✅ 快速任务成功")
    
    # 超时任务
    timeout_count = 0
    try:
        await enhanced_execute_with_timeout(
            asyncio.sleep(1), timeout=0.1, monitor=monitor
        )
    except Exception:
        timeout_count += 1
    
    assert timeout_count == 1, "应该检测到超时"
    print("✅ 超时检测正确")
    
    stats = await monitor.get_stats()
    print(f"✅ 监控: {stats}")
    return True


async def main():
    print("="*80)
    print("🔬 第十轮测试 - 最终验证 (慢工出细活)")
    print("="*80)
    
    results = []
    
    try:
        results.append(("事件总线", await test_event_bus()))
        results.append(("熔断器", await test_circuit_breaker()))
        results.append(("队列", await test_queue()))
        results.append(("超时保护", await test_timeout()))
        
        print("\n" + "="*80)
        print("📊 测试摘要:")
        all_passed = True
        for name, passed in results:
            status = "✅ 通过" if passed else "❌ 失败"
            print(f"  {name}: {status}")
            all_passed = all_passed and passed
        
        if all_passed:
            print("\n🎉 所有测试通过！系统状态良好！")
        else:
            print("\n⚠️ 部分测试失败")
            
        print("="*80)
        
        # 生成报告
        with open("/workspace/backend/TENTH_ROUND_SUMMARY.md", "w", encoding="utf-8") as f:
            f.write("# 第十轮深度测试 - 测试总结\n\n")
            f.write("**日期**: 2026-05-20\n\n")
            f.write("## 测试总结\n\n")
            for name, passed in results:
                f.write(f"- **{name}**: {'✅ 通过' if passed else '❌ 失败'}\n")
            f.write("\n## 修复内容\n\n")
            f.write("1. 修复队列拒绝低优先级任务逻辑 - 在 reject 后通知等待入队的任务\n")
            f.write("2. 调整队列扩容逻辑顺序 - 优先尝试拒绝低优先级任务，再尝试扩容\n")
            f.write("3. 优化熔断器调用协程的处理\n\n")
        
        print("\n📄 测试报告已生成")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        print(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(main())
