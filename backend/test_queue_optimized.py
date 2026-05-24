"""
测试修复后的队列 - 防止无限增长
"""
import asyncio
import sys
sys.path.insert(0, '/workspace/backend')

from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue


async def test_queue_optimized():
    print("✅ 测试优化后的队列")
    
    queue = EnhancedAdaptiveQueue()
    
    print(f"初始容量: {queue.capacity}")
    print(f"最大容量: {queue.config.max_capacity}")
    
    # 测试入队大量项目
    print("\n📦 测试入队 2000 个项目（优先级不同）")
    for i in range(2000):
        # 使用不同的优先级
        priority = i % 10
        success = await queue.enqueue(f"item-{i}", priority=priority)
        if i % 200 == 0:
            stats = queue.get_stats()
            print(f"  第 {i} 个: success={success}, size={queue.size()}, "
                  f"capacity={queue.capacity}, enqueued={stats['enqueued']}, "
                  f"rejected={stats['rejected']}, growth={stats['growth_count']}")
    
    stats = queue.get_stats()
    print(f"\n✅ 最终状态: size={queue.size()}, capacity={queue.capacity}, "
          f"enqueued={stats['enqueued']}, rejected={stats['rejected']}, "
          f"growth={stats['growth_count']}")
    
    # 测试出队
    print("\n📤 测试出队")
    for i in range(500):
        item = await queue.dequeue()
        if i % 100 == 0:
            print(f"  第 {i} 个: item={item}, size={queue.size()}")
    
    stats = queue.get_stats()
    print(f"\n✅ 最终状态: dequeued={stats['dequeued']}, shrink={stats['shrink_count']}")
    
    print("\n✅ 队列优化测试完成！")


if __name__ == "__main__":
    asyncio.run(test_queue_optimized())
