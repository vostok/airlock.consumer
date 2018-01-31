using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class MetricsAggregatorAirlockEventProcessorProvider : IAirlockEventProcessorProvider
    {
        private readonly IMetricScope rootMetricScope;
        private readonly IMetricSender metricSender;
        private readonly MetricsAggregatorSettings settings;

        public MetricsAggregatorAirlockEventProcessorProvider(
            IMetricScope rootMetricScope,
            IMetricSender metricSender,
            MetricsAggregatorSettings settings)
        {
            this.rootMetricScope = rootMetricScope;
            this.metricSender = metricSender;
            this.settings = settings;
        }

        public IAirlockEventProcessor GetProcessor(string routingKey)
        {
            return new MetricsAggregatorProcessor(metricSender, rootMetricScope, settings, routingKey);
        }
    }
}