using System;
using System.Collections.Generic;

namespace Vostok.AirlockConsumer
{
    public abstract class SimpleAirlockEventProcessorBase<T> : IAirlockEventProcessor<T>
    {
        public DateTimeOffset? GetStartTimestampOnRebalance()
        {
            return null;
        }

        public abstract void Process(List<AirlockEvent<T>> events);

        public void Release(string routingKey)
        {
        }
    }
}