using System;
using System.Collections.Generic;

namespace Vostok.AirlockConsumer
{
    public interface IAirlockEventProcessor
    {
        string ProcessorId { get; }

        /// return null to start consumption according to kafka-consumer default behavior (i.e. from the last commited offset, or the latest / earliest offset)
        DateTimeOffset? GetStartTimestampOnRebalance(string routingKey);

        void Process(List<AirlockEvent<byte[]>> events);

        void Release(string routingKey);
    }

    public interface IAirlockEventProcessor<T>
    {
        /// return null to start consumption according to kafka-consumer default behavior (i.e. from the last commited offset, or the latest / earliest offset)
        DateTimeOffset? GetStartTimestampOnRebalance(string routingKey);

        void Process(List<AirlockEvent<T>> events);

        void Release(string routingKey);
    }
}