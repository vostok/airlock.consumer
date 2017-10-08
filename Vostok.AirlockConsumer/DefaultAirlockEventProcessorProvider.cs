using Vostok.Airlock;

namespace Vostok.AirlockConsumer
{
    // todo (avk, 08.10.2017): use one processor instance per project in order to isolate airlock abuse consequences
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