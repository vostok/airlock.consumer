using System;
using System.Net;
using Vostok.Airlock;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class MetricsAggregatorEntryPoint
    {
        private static void Main(string[] args)
        {
            var settingsFromFile = Configuration.TryGetSettingsFromFile(args);
            var log = Logging.Configure((string)settingsFromFile?["airlock.consumer.log.file.pattern"] ?? "..\\log\\actions-{Date}.txt");
            var bootstrapServers = (string)settingsFromFile?["bootstrap.servers"] ?? "localhost:9092";
            const string consumerGroupId = nameof(MetricsAggregatorEntryPoint);
            var clientId = (string)settingsFromFile?["client.id"] ?? Dns.GetHostName();
            var settings = new MetricsAggregatorSettings
            {
                MetricAggregationPastGap = ParseTimeSpan(settingsFromFile?["airlock.metricsAggregator.pastGap"], 20.Seconds()),
                MetricAggregationFutureGap = ParseTimeSpan(settingsFromFile?["airlock.metricsAggregator.pastGap"], 1.Hours()),
                MetricAggregationStartGap = ParseTimeSpan(settingsFromFile?["airlock.metricsAggregator.startGap"], 10.Minutes())
            };
            //TODO create IAirlock here
            var rootMetricScope = new RootMetricScope(new MetricConfiguration());
            IAirlock airlock = null;
            var processor = new MetricAirlockEventProcessor(
                routingKey =>
                {
                    var initialBorders = CreateBorders(DateTimeOffset.UtcNow, settings);
                    var metricAggregator = new MetricAggregator(
                        rootMetricScope,
                        new BucketKeyProvider(),
                        airlock,
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

            var processorProvider = new DefaultAirlockEventProcessorProvider<MetricEvent, MetricEventSerializer>(":metric_events", processor);
            var consumer = new ConsumerGroupHost(bootstrapServers, consumerGroupId, clientId, true, log, processorProvider);
            
            consumer.Start();
            Console.ReadLine();
            consumer.Stop();
            processor.Stop();
        }

        private static Borders CreateBorders(DateTimeOffset timestamp, MetricsAggregatorSettings settings)
        {
            var future = timestamp + settings.MetricAggregationFutureGap;
            var past = timestamp - settings.MetricAggregationPastGap - settings.MetricAggregationStartGap;
            return new Borders(past, future);
        }

        private static TimeSpan ParseTimeSpan(object value, TimeSpan defaultValue)
        {
            return value == null ? defaultValue : TimeSpan.Parse(value.ToString());
        }
    }
}