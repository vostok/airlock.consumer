using Vstk.Airlock;
using Vstk.Metrics;

namespace Vstk.AirlockConsumer.MetricsAggregator
{
    public class MetricsAggregatorAirlockEventProcessorProvider : IAirlockEventProcessorProvider
    {
        private readonly IMetricScope rootMetricScope;
        private readonly IAirlockClient airlockClient;
        private readonly MetricsAggregatorSettings settings;

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
            return new MetricsAggregatorProcessor(airlockClient, rootMetricScope, settings, routingKey);
        }
    }
}