using System.Collections.Concurrent;

namespace Vostok.AirlockConsumer
{
    public class BoundedBlockingQueue<T> : BlockingCollection<T>
    {
        public BoundedBlockingQueue(int maxQueueSize)
            : base(new ConcurrentQueue<T>(), maxQueueSize)
        {
        }
    }
}