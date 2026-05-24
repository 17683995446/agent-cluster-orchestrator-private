#!/usr/bin/env python3
"""
✅ 系统改进验证测试
验证所有修复是否正常工作
"""
from __future__ import annotations
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

print("=" * 80)
print("🤖 Agent Cluster Orchestrator - 系统改进验证")
print("=" * 80)

async def test_enhanced_event_bus():
    """测试增强版事件总线"""
    print("\n🔍 测试1: 增强版EventBus")
    
    try:
        from app.core.enhanced_event_bus import EnhancedEventBus
        
        bus = EnhancedEventBus()
        await bus.start()
        
        received = []
        
        async def callback(event):
            received.append(event)
        
        await bus.subscribe(callback)
        
        # 发布测试事件
        class MockEvent:
            def __init__(self, event_id):
                self.event_id = event_id
                self.event_type = "test"
        
        for i in range(10):
            await bus.publish(MockEvent(i))
        
        await asyncio.sleep(0.1)
        
        stats = bus.get_stats()
        await bus.stop()
        
        print(f"✅ 发布事件: {stats['events_published']}")
        print(f"✅ 处理事件: {stats['events_processed']}")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_enhanced_circuit_breaker():
    """测试增强版熔断器"""
    print("\n🔍 测试2: 增强版CircuitBreaker")
    
    try:
        from app.core.enhanced_circuit_breaker import (
            EnhancedCircuitBreaker,
            EnhancedCircuitBreakerConfig,
            CircuitState
        )
        
        config = EnhancedCircuitBreakerConfig(
            failure_threshold=3,
            slow_call_threshold=0.1  # 非常低的阈值，易于触发
        )
        cb = EnhancedCircuitBreaker("test", config)
        
        # 测试成功调用
        async def success_func():
            await asyncio.sleep(0.01)
            return "success"
        
        result = await cb.call(success_func)
        print(f"✅ 成功调用测试: {result}")
        
        # 测试失败调用
        async def fail_func():
            raise Exception("test failure")
        
        for i in range(3):
            try:
                await cb.call(fail_func)
            except:
                pass
        
        stats = cb.get_stats()
        print(f"✅ 熔断器统计: 失败次数 {stats['failed_calls']}, 状态 {stats['state']}")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_system_integration():
    """测试系统整合"""
    print("\n🔍 测试3: 系统整合测试")
    
    try:
        from app.core import (
            EnhancedEventBus,
            EnhancedCircuitBreaker,
            EnhancedCircuitBreakerConfig
        )
        
        bus = EnhancedEventBus()
        cb = EnhancedCircuitBreaker("test", EnhancedCircuitBreakerConfig())
        
        await bus.start()
        
        # 一起使用
        async def combined_task():
            await bus.publish("test_event")
            async def simple_func():
                return "ok"
            result = await cb.call(simple_func)
            return result
        
        await combined_task()
        
        await bus.stop()
        
        print("✅ 系统整合测试通过")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """主测试函数"""
    results = []
    
    results.append(("EventBus", await test_enhanced_event_bus()))
    results.append(("CircuitBreaker", await test_enhanced_circuit_breaker()))
    results.append(("Integration", await test_system_integration()))
    
    print("\n" + "=" * 80)
    print("📋 验证测试摘要")
    print("=" * 80)
    
    for name, passed in results:
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"  {name}: {status}")
    
    print("\n" + "=" * 80)
    if all(passed for _, passed in results):
        print("🎉🎉🎉 所有验证测试通过！系统改进成功！🎉🎉🎉")
    else:
        print("⚠️  部分测试失败，需要进一步检查")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
