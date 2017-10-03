using System.Collections.Generic;

namespace Vostok.AirlockConsumer
{
    public interface IMessageProcessor<T>
    {
        void Process(List<AirlockEvent<T>> events);
    }
}