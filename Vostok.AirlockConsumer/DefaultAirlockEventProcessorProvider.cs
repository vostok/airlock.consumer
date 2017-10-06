using Vostok.Airlock;

namespace Vostok.AirlockConsumer
{
    public class DefaultAirlockEventProcessorProvider<T, TDeserializer> : IAirlockEventProcessorProvider
        where TDeserializer : IAirlockDeserializer<T>, new()
    {
        private readonly DefaultAirlockEventProcessor<T> processor;
        private readonly IAirlockDeserializer<T> airlockDeserializer = new TDeserializer();

        public DefaultAirlockEventProcessorProvider(IAirlockEventProcessor<T> processor)
        {
            this.processor = new DefaultAirlockEventProcessor<T>(airlockDeserializer, processor);
        }

        public IAirlockEventProcessor GetProcessor(string routingKey) => processor;
    }
}