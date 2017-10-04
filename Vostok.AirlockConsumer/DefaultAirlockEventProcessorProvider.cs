using System.Collections.Concurrent;
using Vostok.Airlock;

namespace Vostok.AirlockConsumer
{
    public class DefaultAirlockEventProcessorProvider<T, TDeserializer> : IAirlockEventProcessorProvider
        where TDeserializer : IAirlockDeserializer<T>, new()
    {
        private readonly string routingKeySuffix;
        private readonly IAirlockEventProcessor<T> processor;
        private readonly IAirlockDeserializer<T> airlockDeserializer = new TDeserializer();
        private readonly ConcurrentDictionary<string, IAirlockEventProcessor> processors = new ConcurrentDictionary<string, IAirlockEventProcessor>();

        public DefaultAirlockEventProcessorProvider(string routingKeySuffix, IAirlockEventProcessor<T> processor)
        {
            this.routingKeySuffix = routingKeySuffix;
            this.processor = processor;
        }

        public IAirlockEventProcessor TryGetProcessor(string routingKey)
        {
            return routingKey.EndsWith(routingKeySuffix)
                ? processors.GetOrAdd(routingKey, _ => new DefaultAirlockEventProcessor<T>(airlockDeserializer, processor))
                : null;
        }
    }
}