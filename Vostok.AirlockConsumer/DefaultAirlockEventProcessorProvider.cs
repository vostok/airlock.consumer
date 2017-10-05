using Vostok.Airlock;

namespace Vostok.AirlockConsumer
{
    public class DefaultAirlockEventProcessorProvider<T, TDeserializer> : IAirlockEventProcessorProvider
        where TDeserializer : IAirlockDeserializer<T>, new()
    {
        private readonly string routingKeySuffix;
        private readonly DefaultAirlockEventProcessor<T> processor;
        private readonly IAirlockDeserializer<T> airlockDeserializer = new TDeserializer();

        public DefaultAirlockEventProcessorProvider(string routingKeySuffix, IAirlockEventProcessor<T> processor)
        {
            this.routingKeySuffix = routingKeySuffix;
            this.processor = new DefaultAirlockEventProcessor<T>(airlockDeserializer, processor);
        }

        public IAirlockEventProcessor TryGetProcessor(string routingKey)
        {
            return routingKey.EndsWith(routingKeySuffix) ? processor : null;
        }
    }
}