using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Airlock;

namespace Vostok.AirlockConsumer
{
    public class DefaultAirlockEventProcessor<T> : IAirlockEventProcessor
    {
        private readonly IAirlockDeserializer<T> airlockDeserializer;
        private readonly IAirlockEventProcessor<T> airlockEventProcessor;

        public DefaultAirlockEventProcessor(IAirlockDeserializer<T> airlockDeserializer, IAirlockEventProcessor<T> airlockEventProcessor)
        {
            this.airlockDeserializer = airlockDeserializer;
            this.airlockEventProcessor = airlockEventProcessor;
            ProcessorId = airlockEventProcessor.GetType().Name;
        }

        public string ProcessorId { get; }

        public DateTimeOffset? GetStartTimestampOnRebalance(string routingKey)
        {
            return airlockEventProcessor.GetStartTimestampOnRebalance(routingKey);
        }

        public void Process(List<AirlockEvent<byte[]>> events, ProcessorMetrics processorMetrics)
        {
            var airlockEvents = events.Select(x => new AirlockEvent<T>
            {
                RoutingKey = x.RoutingKey,
                Timestamp = x.Timestamp,
                Payload = airlockDeserializer.Deserialize(new ByteBufferAirlockSource(x.Payload)),
            }).ToList();
            airlockEventProcessor.Process(airlockEvents, processorMetrics);
        }

        public void Release(string routingKey)
        {
            airlockEventProcessor.Release(routingKey);
        }
    }
}