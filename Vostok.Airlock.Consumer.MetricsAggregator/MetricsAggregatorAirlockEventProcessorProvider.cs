using Vostok.Metrics;

namespace Vostok.Airlock.Consumer.MetricsAggregator
{
    public class MetricsAggregatorAirlockEventProcessorProvider : IAirlockEventProcessorProvider
    {
        private readonly IMetricScope rootMetricScope;
        private readonly IAirlockBatchClient airlockClient;
        private readonly MetricsAggregatorSettings settings;

        public MetricsAggregatorAirlockEventProcessorProvider(
            IMetricScope rootMetricScope,
            IAirlockBatchClient airlockClient,
            MetricsAggregatorSettings settings)
        {
            this.rootMetricScope = rootMetricScope;
            this.airlockClient = airlockClient;
            this.settings = settings;
        }

        public IAirlockEventProcessor GetProcessor(string routingKey)
        {
            return new MetricsAggregatorProcessor(airlockClient, rootMetricScope, settings, routingKey);
        }
    }
}