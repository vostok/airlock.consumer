using System;
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
            // todo (spaceorc 05.10.2017) "-events" это не очень красиво - подумать и исправить как-то https://github.com/vostok/airlock.consumer/issues/18
            routingKeyFilter = new DirtyRoutingKeyFilter("-events");

            AirlockSerializerRegistry.Register(new MetricEventSerializer());

            var settings = new MetricsAggregatorSettings();
            processorProvider = new MetricsAggregatorAirlockEventProcessorProvider(rootMetricScope, AirlockClient, settings);
        }

        private class DirtyRoutingKeyFilter : IRoutingKeyFilter
        {
            private readonly string suffix;

            public DirtyRoutingKeyFilter(string suffix)
            {
                this.suffix = suffix;
            }

            public bool Matches(string routingKey) => routingKey.EndsWith(suffix, StringComparison.OrdinalIgnoreCase);
        }
    }
}