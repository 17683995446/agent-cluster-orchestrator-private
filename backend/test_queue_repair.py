"""
🔍 测试队列修复
"""
import asyncio
import sys

sys.path.insert(0, '/workspace/backend')

from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue


async def test_queue_repair():
    print("✅ 测试队列修复")
    
    queue = EnhancedAdaptiveQueue()
    
    print(f"初始容量: {queue.capacity}")
    print(f"最大容量: {queue.config.max_capacity}")
    
    # 测试入队超过容量，应该会 reject 低优先级
    print("\n📦 测试入队 2000 个项目，优先级 5")
    for i in range(2000):
        success = await queue.enqueue(f"item-{i}", priority=5)
        if i % 200 == 0:
            stats = queue.get_stats()
            print(f"  第 {i} 个: success={success}, size={queue.size()}, enqueued={stats['enqueued']}, rejected={stats['rejected']}")
    
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
    print(f"\n✅ 最终状态: dequeued={stats['dequeued']}")
    
    print("\n✅ 队列修复测试完成")


if __name__ == "__main__":
    asyncio.run(test_queue_repair())
