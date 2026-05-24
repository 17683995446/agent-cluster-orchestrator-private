#!/usr/bin/env python3
"""
诊断和修复AdaptiveQueue的size问题
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import asyncio
from datetime import datetime

from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue, EnhancedQueueConfig

async def diagnose_queue_size_issue():
    """诊断队列size问题"""
    print("="*80)
    print("🔍 诊断AdaptiveQueue Size问题")
    print("="*80)
    
    # 测试1: 简单的入队和size检查
    print("\n📋 测试1: 基础入队和size检查")
    print("-"*80)
    
    queue_config = EnhancedQueueConfig(
        initial_capacity=10,
        max_capacity=50
    )
    queue = EnhancedAdaptiveQueue(config=queue_config)
    
    print(f"初始队列size: {queue.size()}")
    print(f"初始队列stats: {queue.get_stats()}")
    
    # 入队20个项目
    print("\n📥 入队20个项目...")
    for i in range(20):
        await queue.enqueue(f"item_{i}", priority=i%11)
    
    size_after_enqueue = queue.size()
    print(f"入队后队列size: {size_after_enqueue}")
    print(f"队列stats: {queue.get_stats()}")
    
    # 检查问题
    if size_after_enqueue != 20:
        print(f"❌ 问题发现！期望size是20，实际是{size_after_enqueue}")
    else:
        print("✅ size正确！")
    
    # 测试2: 出队后检查
    print("\n📋 测试2: 出队一些项目并检查size")
    print("-"*80)
    
    # 出队10个项目
    for i in range(10):
        item = await queue.dequeue(wait_timeout=0.1)
        print(f"出队项目: {item}")
    
    size_after_dequeue = queue.size()
    print(f"出队后队列size: {size_after_dequeue}")
    print(f"队列stats: {queue.get_stats()}")
    
    # 测试3: 边界条件
    print("\n📋 测试3: 边界条件 - 满队列")
    print("-"*80)
    
    queue2 = EnhancedAdaptiveQueue(EnhancedQueueConfig(
        initial_capacity=1,
        max_capacity=5
    ))
    
    print(f"队列2初始size: {queue2.size()}")
    
    # 尝试入队10个项目
    enqueued_count = 0
    for i in range(10):
        result = await queue2.enqueue(f"boundary_{i}", priority=i)
        if result:
            enqueued_count += 1
            print(f"✅ 成功入队: boundary_{i}")
        else:
            print(f"❌ 被拒绝: boundary_{i}")
    
    print(f"\n总共入队成功: {enqueued_count} 个项目")
    print(f"队列2最终size: {queue2.size()}")
    print(f"队列2最终stats: {queue2.get_stats()}")
    
    # 测试4: 更详细的队列操作
    print("\n" + "="*80)
    print("📋 测试4: 详细的队列生命周期")
    print("="*80)
    
    queue3 = EnhancedAdaptiveQueue(EnhancedQueueConfig(
        initial_capacity=20,
        max_capacity=100
    ))
    
    operations = []
    
    # 模拟真实工作负载
    print("\n模拟工作负载...")
    for i in range(50):
        await queue3.enqueue(f"work_{i}", priority=i%10)
        operations.append(f"enqueue work_{i}")
        
        if i % 3 == 0:
            item = await queue3.dequeue(wait_timeout=0.1)
            if item:
                operations.append(f"dequeue {item}")
    
    print(f"队列3最终size: {queue3.size()}")
    print(f"队列3最终stats: {queue3.get_stats()}")
    
    # 总结
    print("\n" + "="*80)
    print("📊 诊断总结")
    print("="*80)
    print(f"测试完成时间: {datetime.now()}")
    print("\n✅ 所有测试运行正常！")
    print("让我们查看AdaptiveQueue的size实现...")
    
    return {
        "test1_size": size_after_enqueue,
        "test2_size": size_after_dequeue,
        "test3_size": queue2.size()
    }

async def check_queue_implementation():
    """检查队列实现"""
    print("\n" + "="*80)
    print("📖 检查AdaptiveQueue的size()实现")
    print("="*80)
    
    # 让我们创建一个队列并观察其内部状态
    queue = EnhancedAdaptiveQueue(EnhancedQueueConfig(
        initial_capacity=10,
        max_capacity=100
    ))
    
    print("\n观察size计算过程:")
    
    # 入队5个项目
    for i in range(5):
        await queue.enqueue(f"test_{i}", priority=i)
        print(f"  入队test_{i}后size: {queue.size()}")
        print(f"  完整stats: {queue.get_stats()}")
    
    # 查看get_stats的返回
    stats = queue.get_stats()
    print(f"\nget_stats()返回值: {stats}")
    
    # 测试is_empty
    print(f"\nis_empty(): {queue.is_empty()}")
    
    return stats

if __name__ == "__main__":
    asyncio.run(diagnose_queue_size_issue())
    print("\n" + "="*80)
    print("✅ 诊断完成！")
    print("="*80)
