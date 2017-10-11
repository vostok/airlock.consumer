using System;
using System.Collections.Generic;
using Vostok.Airlock;
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

        protected sealed override void DoInitialize(ILog log, Dictionary<string, string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            // todo (spaceorc 05.10.2017) "-events" это не очень красиво - подумать и исправить как-то
            routingKeyFilter = new DirtyRoutingKeyFilter("-events");

            AirlockSerializerRegistry.Register(new MetricEventSerializer());
            var airlockConfig = GetAirlockConfig(log, environmentVariables);
            var airlockClient = new AirlockClient(airlockConfig, log);

            var settings = new MetricsAggregatorSettings();
            var rootMetricScope = new RootMetricScope(
                new MetricConfiguration
                {
                    // todo (spaceorc 05.10.2017) get proj and env from settings
                    Reporter = new AirlockMetricReporter(airlockClient, RoutingKey.CreatePrefix("vostok", "env", "metrics-aggregator"))
                });
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
            // todo (avk, 11.10.2017): wait for airlock gate to start
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