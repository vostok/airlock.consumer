using System;
using System.Collections.Generic;

namespace Vstk.AirlockConsumer
{
    public abstract class SimpleAirlockEventProcessorBase<T> : IAirlockEventProcessor<T>
    {
        public DateTimeOffset? GetStartTimestampOnRebalance(string routingKey)
        {
            return null;
        }

        public abstract void Process(List<AirlockEvent<T>> events, ProcessorMetrics processorMetrics);

        public void Release(string routingKey)
        {
        }
    }
}