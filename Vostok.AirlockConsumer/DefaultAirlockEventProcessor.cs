using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vostok.Airlock;

namespace Vostok.AirlockConsumer
{
    public class DefaultAirlockEventProcessor<T> : IAirlockEventProcessor, IDisposable
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

        public async Task ProcessAsync(List<AirlockEvent<byte[]>> events)
        {
            var airlockEvents = events.Select(x => new AirlockEvent<T>
            {
                RoutingKey = x.RoutingKey,
                Timestamp = x.Timestamp,
                Payload = airlockDeserializer.Deserialize(
                    
                    new SimpleAirlockSource(x.Payload)),
            }).ToList();
            await airlockEventProcessor.ProcessAsync(airlockEvents).ConfigureAwait(false);
        }

        public void Dispose()
        {
            (airlockEventProcessor as IDisposable)?.Dispose(); // todo (avk, 05.10.2017): it's really questionable decision to dispose something that you don't own
        }
    }
}