using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.Airlock.Consumer.MetricsAggregator
{
    public class MetricsAggregatorAirlockEventProcessorProvider : IAirlockEventProcessorProvider
    {
        private readonly IMetricScope rootMetricScope;
        private readonly IAirlockBatchClient airlockClient;
        private readonly MetricsAggregatorSettings settings;
        private readonly ILog log;

        public MetricsAggregatorAirlockEventProcessorProvider(
            IMetricScope rootMetricScope,
            IAirlockBatchClient airlockClient,
            MetricsAggregatorSettings settings,
            ILog log)
        {
            this.rootMetricScope = rootMetricScope;
            this.airlockClient = airlockClient;
            this.settings = settings;
            this.log = log;
        }

        public IAirlockEventProcessor GetProcessor(string routingKey)
        {
            return new MetricsAggregatorProcessor(airlockClient, rootMetricScope, settings, routingKey, log);
        }
    }
}