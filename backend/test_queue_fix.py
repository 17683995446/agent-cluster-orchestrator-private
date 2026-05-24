"""
🔍 队列容量测试 - 验证修复
"""
import asyncio
import sys
sys.path.insert(0, '/workspace/backend')

from app.core.enhanced_adaptive_queue import EnhancedAdaptiveQueue

async def main():
    print("🔍 测试队列容量限制修复")
    
    queue = EnhancedAdaptiveQueue()
    
    print(f"初始容量: {queue.capacity}")
    print(f"最大容量: {queue.config.max_capacity}")
    
    # 测试超过容量限制
    for i in range(1200):
        await queue.enqueue(f"item-{i}", priority=1)
        if i % 200 == 0:
            stats = queue.get_stats()
            print(f"入队 {i} 个, 当前大小: {queue.size()}, 容量: {queue.capacity}, 拒绝: {stats['rejected']}")
    
    stats = queue.get_stats()
    print(f"\n最终统计:")
    print(f"  当前大小: {queue.size()}")
    print(f"  容量: {queue.capacity}")
    print(f"  入队: {stats['enqueued']}")
    print(f"  拒绝: {stats['rejected']}")
    print(f"  扩容次数: {stats['growth_count']}")
    
    if queue.size() <= queue.config.max_capacity:
        print("\n✅ 修复成功！队列容量限制正常工作")
    else:
        print("\n❌ 修复失败！队列超过最大容量")

if __name__ == "__main__":
    asyncio.run(main())
