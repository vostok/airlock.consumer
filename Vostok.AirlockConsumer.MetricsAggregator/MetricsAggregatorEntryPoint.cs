using System;
using System.Collections.Generic;
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
        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings()
        {
            MaxBatchSize = 1000,
            MaxProcessorQueueSize = 100000
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, Dictionary<string,string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            // todo (spaceorc 05.10.2017) "-events" это не очень красиво - подумать и исправить как-то https://github.com/vostok/airlock.consumer/issues/18
            routingKeyFilter = new DirtyRoutingKeyFilter("-events");

            AirlockSerializerRegistry.Register(new MetricEventSerializer());
            var airlockConfig = GetAirlockConfig(log, environmentVariables);
            var airlockClient = new AirlockClient(airlockConfig, log);

            var settings = new MetricsAggregatorSettings();
            var processor = new MetricAirlockEventProcessor(
                routingKey =>
                {
                    var initialBorders = CreateBorders(DateTimeOffset.UtcNow, settings);
                    var metricAggregator = new MetricAggregator(
                        rootMetricScope,
                        new BucketKeyProvider(),
                        airlockClient,
                        settings.MetricAggregationPastGap,
                        initialBorders,
                        routingKey);
                    var eventsTimestampProvider = new EventsTimestampProvider(1000);
                    var metricResetDaemon = new MetricResetDaemon(eventsTimestampProvider, settings, metricAggregator);
                    return new MetricAggregationService(
                        metricAggregator,
                        eventsTimestampProvider,
                        metricResetDaemon,
                        initialBorders);
                });
            processorProvider = new DefaultAirlockEventProcessorProvider<MetricEvent, MetricEventSerializer>(project => processor);
        }

        private static Borders CreateBorders(DateTimeOffset timestamp, MetricsAggregatorSettings settings)
        {
            var future = timestamp + settings.MetricAggregationFutureGap;
            var past = timestamp - settings.MetricAggregationPastGap - settings.MetricAggregationStartGap;
            return new Borders(past, future);
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