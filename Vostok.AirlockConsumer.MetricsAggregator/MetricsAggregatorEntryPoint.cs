using System;
using System.Net;
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
            var aggregatorSettings = new MetricsAggregatorSettings
            {
                MetricAggregationPastGap = ParseTimeSpan(settingsFromFile?["airlock.metricsAggregator.pastGap"], 20.Seconds()),
                MetricAggregationFutureGap = ParseTimeSpan(settingsFromFile?["airlock.metricsAggregator.pastGap"], 1.Hours())
            };
            //TODO create IAirlock here
            var rootMetricScope = new RootMetricScope(new MetricConfiguration());
            var metricAggregator = new MetricAggregator(rootMetricScope, new BucketKeyProvider(), null, CreateBorders(DateTimeOffset.UtcNow, aggregatorSettings));
            var processor = new MetricAirlockEventProcessor(metricAggregator);
            var processorProvider = new DefaultAirlockEventProcessorProvider<MetricEvent, MetricEventSerializer>(":metric_events", processor);
            var consumer = new ConsumerGroupHost(bootstrapServers, consumerGroupId, clientId, true, log, processorProvider);
            var clock = new MetricClock(1.Minutes());
            clock.Register(timestamp => metricAggregator.Reset(CreateBorders(timestamp, aggregatorSettings)));
            clock.Start();
            consumer.Start();
            Console.ReadLine();
            consumer.Stop();
            clock.Stop();
        }

        private static Borders CreateBorders(DateTimeOffset timestamp, MetricsAggregatorSettings settings)
        {
            return new Borders(timestamp - settings.MetricAggregationPastGap, timestamp + settings.MetricAggregationFutureGap);
        }

        private static TimeSpan ParseTimeSpan(object value, TimeSpan defaultValue)
        {
            return value == null ? defaultValue : TimeSpan.Parse(value.ToString());
        }
    }
}