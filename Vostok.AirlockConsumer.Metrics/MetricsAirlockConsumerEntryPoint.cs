using System;
using System.Collections.Generic;
using Vostok.Airlock.Metrics;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Metrics
{
    public class MetricsAirlockConsumerEntryPoint : ConsumerApplication
    {
        private const string defaultGraphiteEndpoint = "graphite:2003";

        public static void Main()
        {
            new ConsumerApplicationHost<MetricsAirlockConsumerEntryPoint>().Run();
        }

        protected override string ServiceName => "consumer-metric";
        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings()
        {
            MaxBatchSize = 10000,
            MaxProcessorQueueSize = 100000
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, Dictionary<string, string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(Airlock.RoutingKey.MetricsSuffix);
            var graphiteUri = GetGraphiteUri();
            processorProvider = processorProvider = new DefaultAirlockEventProcessorProvider<MetricEvent, MetricEventSerializer>(project => new MetricsAirlockEventProcessor(graphiteUri, log));
        }

        private Uri GetGraphiteUri()
        {
            var graphiteEndpoint = GetSettingByName("GRAPHITE_ENDPOINT", defaultGraphiteEndpoint);
            return new Uri("tcp://" + graphiteEndpoint);
        }
    }
}