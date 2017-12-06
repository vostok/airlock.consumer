using System;
using System.Collections.Generic;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer
{
    public interface IAirlockEventProcessor
    {
        string ProcessorId { get; }

        /// <summary>
        ///     return null to start consumption according to kafka-consumer default behavior (i.e. from the last commited offset,
        ///     or the latest / earliest offset)
        /// </summary>
        DateTimeOffset? GetStartTimestampOnRebalance(string routingKey);

        void Process(List<AirlockEvent<byte[]>> events, ICounter messageProcessedCounter);

        void Release(string routingKey);
    }

    public interface IAirlockEventProcessor<T>
    {
        /// <summary>
        ///     return null to start consumption according to kafka-consumer default behavior (i.e. from the last commited offset,
        ///     or the latest / earliest offset)
        /// </summary>
        DateTimeOffset? GetStartTimestampOnRebalance(string routingKey);

        void Process(List<AirlockEvent<T>> events, ICounter messageProcessedCounter);

        void Release(string routingKey);
    }
}