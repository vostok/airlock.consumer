using System.Collections.Generic;

namespace Vostok.AirlockConsumer
{
    public interface IMessageProcessor<T>
    {
        void Process(IEnumerable<ConsumerEvent<T>> events);
    }
}