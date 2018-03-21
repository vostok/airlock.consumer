using Vostok.Airlock.Metrics;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.Airlock.Consumer.Metrics
{
    public class MetricsAirlockConsumerEntryPoint : ConsumerApplication
    {
        public static void Main()
        {
            new ConsumerApplicationHost<MetricsAirlockConsumerEntryPoint>().Run();
        }

        protected override string ServiceName => "consumer-metric";

        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings
        {
            MaxBatchSize = 10000,
            MaxProcessorQueueSize = 100000
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, AirlockEnvironmentVariables environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(Airlock.RoutingKey.MetricsSuffix);
            var graphiteUri = GetGraphiteUri(log, environmentVariables);
            processorProvider = new DefaultAirlockEventProcessorProvider<MetricEvent, MetricEventSerializer>(project => new MetricsAirlockEventProcessor(graphiteUri, log));
        }
    }
}