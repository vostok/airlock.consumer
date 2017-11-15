using Vostok.Airlock;
using Vostok.Airlock.Metrics;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class MetricsAggregatorAirlockEventProcessorProvider : IAirlockEventProcessorProvider
    {
        private readonly IMetricScope rootMetricScope;
        private readonly IAirlockClient airlockClient;
        private readonly MetricsAggregatorSettings settings;
        private readonly MetricEventSerializer airlockDeserializer = new MetricEventSerializer();

        public MetricsAggregatorAirlockEventProcessorProvider(
            IMetricScope rootMetricScope,
            IAirlockClient airlockClient,
            MetricsAggregatorSettings settings)
        {
            this.rootMetricScope = rootMetricScope;
            this.airlockClient = airlockClient;
            this.settings = settings;
        }

        public IAirlockEventProcessor GetProcessor(string routingKey)
        {
            var processor = new MetricsAggregatorProcessor(airlockClient, rootMetricScope, settings, routingKey);
            return new DefaultAirlockEventProcessor<MetricEvent>(airlockDeserializer, processor);
        }
    }
}