using System;
using System.Collections.Generic;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer
{
    public abstract class SimpleAirlockEventProcessorBase<T> : IAirlockEventProcessor<T>
    {
        public DateTimeOffset? GetStartTimestampOnRebalance(string routingKey)
        {
            return null;
        }

        public abstract void Process(List<AirlockEvent<T>> events, ICounter messageProcessedCounter);

        public void Release(string routingKey)
        {
        }
    }
}