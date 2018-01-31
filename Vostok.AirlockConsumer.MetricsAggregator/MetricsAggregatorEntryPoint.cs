using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class MetricsAggregatorEntryPoint : ConsumerApplication
    {
        public static void Main()
        {
            new ConsumerApplicationHost<MetricsAggregatorEntryPoint>().Run();
        }

        protected override string ServiceName => "metrics-aggregator";

        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings
        {
            MaxBatchSize = 1000,
            MaxProcessorQueueSize = 100000
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, AirlockEnvironmentVariables environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new MetricsAggregatorRotingKeyFilter();
            var settings = new MetricsAggregatorSettings();
            processorProvider = new MetricsAggregatorAirlockEventProcessorProvider(rootMetricScope, new MetricSender(GraphiteSink, log), settings);
        }
    }
}