using System;
using System.Linq;
using Vostok.Airlock;
using Vostok.Airlock.Metrics;
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
            routingKeyFilter = new MetricAggregatorKeyFilter();

            AirlockSerializerRegistry.Register(new MetricEventSerializer());

            var settings = new MetricsAggregatorSettings();
            processorProvider = new MetricsAggregatorAirlockEventProcessorProvider(rootMetricScope, AirlockClient, settings);
        }

        private class MetricAggregatorKeyFilter : IRoutingKeyFilter
        {
            public bool Matches(string routingKey)
            {
                if (!RoutingKey.TryParse(routingKey, out var _, out var _, out var _, out var suffix))
                    return false;
                var lastSuffix = suffix.LastOrDefault();
                if (lastSuffix == null)
                    return false;
                return lastSuffix.Equals(RoutingKey.AppEventsSuffix, StringComparison.OrdinalIgnoreCase) || lastSuffix.Equals(RoutingKey.TracesSuffix, StringComparison.OrdinalIgnoreCase);
            }
        }
    }
}