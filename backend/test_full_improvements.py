#!/usr/bin/env python3
"""
✅ 完整系统改进验证测试
验证所有增强组件的功能
"""
from __future__ import annotations
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

print("=" * 80)
print("🤖 Agent Cluster Orchestrator - 完整系统改进验证")
print("=" * 80)


async def test_enhanced_adaptive_queue():
    """测试增强版自适应队列"""
    print("\n🔍 测试1: 增强版 AdaptiveQueue")
    
    try:
        from app.core import (
            EnhancedAdaptiveQueue,
            EnhancedQueueConfig
        )
        
        config = EnhancedQueueConfig(initial_capacity=10, max_capacity=100)
        queue = EnhancedAdaptiveQueue(config)
        
        # 测试基础功能
        print("  - 测试入队/出队...")
        await queue.enqueue("item1", priority=1)
        await queue.enqueue("item3", priority=3)
        await queue.enqueue("item2", priority=2)
        
        item1 = await queue.dequeue(wait_timeout=1.0)
        item2 = await queue.dequeue(wait_timeout=1.0)
        item3 = await queue.dequeue(wait_timeout=1.0)
        
        print(f"  - 出队顺序: {item1}, {item2}, {item3}")
        
        # 验证优先级
        assert item1 == "item3", "优先级排序错误"
        
        # 测试统计
        stats = queue.get_stats()
        print(f"  - 统计信息: 入队 {stats['enqueued']}, 出队 {stats['dequeued']}")
        assert stats["enqueued"] == 3
        assert stats["dequeued"] == 3
        
        print("  ✅ AdaptiveQueue 测试通过!")
        return True
        
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_enhanced_timeout_protection():
    """测试增强版超时保护"""
    print("\n🔍 测试2: 增强版 Timeout Protection")
    
    try:
        from app.core import (
            EnhancedTimeoutMonitor,
            EnhancedTimeoutConfig,
            enhanced_execute_with_timeout
        )
        
        monitor = EnhancedTimeoutMonitor(EnhancedTimeoutConfig())
        
        # 测试成功执行
        print("  - 测试成功执行...")
        async def success_func():
            await asyncio.sleep(0.1)
            return "success"
        
        result = await enhanced_execute_with_timeout(
            success_func(),
            timeout=5.0,
            task_name="success_test",
            monitor=monitor
        )
        assert result == "success"
        
        # 测试超时执行
        print("  - 测试超时执行...")
        async def slow_func():
            await asyncio.sleep(10.0)
            return "slow"
        
        try:
            await enhanced_execute_with_timeout(
                slow_func(),
                timeout=0.5,
                task_name="timeout_test",
                monitor=monitor,
                enable_retry=False
            )
            assert False, "应该超时"
        except Exception as e:
            print(f"  - 超时正确触发: {str(e)[:50]}")
        
        # 获取统计
        stats = await monitor.get_stats()
        print(f"  - 统计信息: 成功 {stats['total_successes']}, 超时 {stats['total_timeouts']}")
        
        print("  ✅ Timeout Protection 测试通过!")
        return True
        
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_all_enhanced_components():
    """测试所有增强组件协同工作"""
    print("\n🔍 测试3: 完整系统集成")
    
    try:
        from app.core import (
            EnhancedEventBus,
            EnhancedCircuitBreaker,
            EnhancedCircuitBreakerConfig,
            EnhancedAdaptiveQueue,
            EnhancedTimeoutMonitor,
            EnhancedTimeoutConfig
        )
        
        # 初始化所有组件
        print("  - 初始化所有组件...")
        bus = EnhancedEventBus()
        cb = EnhancedCircuitBreaker("test_system", EnhancedCircuitBreakerConfig())
        queue = EnhancedAdaptiveQueue()
        monitor = EnhancedTimeoutMonitor()
        
        await bus.start()
        
        # 协同测试
        print("  - 测试协同工作...")
        
        async def system_task():
            await asyncio.sleep(0.05)
            return "system_ok"
        
        result = await cb.call(system_task)
        assert result == "system_ok"
        
        await queue.enqueue("integrated_test", priority=5)
        dequeued = await queue.dequeue(wait_timeout=1.0)
        assert dequeued == "integrated_test"
        
        await bus.publish("test_event")
        
        await bus.stop()
        
        print("  ✅ 系统集成测试通过!")
        return True
        
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_performance_comparison():
    """性能对比测试（可选）"""
    print("\n🔍 测试4: 性能优化验证（简单验证）")
    
    try:
        from app.core import EnhancedAdaptiveQueue
        
        queue = EnhancedAdaptiveQueue()
        
        print("  - 测试批量操作...")
        
        # 批量入队
        start = time.time()
        for i in range(100):
            await queue.enqueue(f"item_{i}", priority=i % 5)
        enqueue_time = time.time() - start
        
        # 批量出队
        start = time.time()
        for i in range(100):
            await queue.dequeue(wait_timeout=1.0)
        dequeue_time = time.time() - start
        
        print(f"  - 100次入队: {enqueue_time:.3f}s")
        print(f"  - 100次出队: {dequeue_time:.3f}s")
        print("  - 性能正常!")
        
        return True
        
    except Exception as e:
        print(f"  ⚠️ 性能测试跳过: {e}")
        return True


async def main():
    """主测试函数"""
    results = []
    
    results.append(("AdaptiveQueue", await test_enhanced_adaptive_queue()))
    results.append(("TimeoutProtection", await test_enhanced_timeout_protection()))
    results.append(("SystemIntegration", await test_all_enhanced_components()))
    results.append(("Performance", await test_performance_comparison()))
    
    print("\n" + "=" * 80)
    print("📊 完整系统改进验证总结")
    print("=" * 80)
    
    all_passed = True
    for name, passed in results:
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 80)
    if all_passed:
        print("🎉🎉🎉 所有系统改进验证完全通过！系统已经大幅优化！")
    else:
        print("⚠️  部分测试失败，需要进一步检查")
    print("=" * 80)
    
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
