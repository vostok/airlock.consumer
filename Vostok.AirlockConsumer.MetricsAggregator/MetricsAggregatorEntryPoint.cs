using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vostok.Airlock;
using Vostok.Clusterclient.Topology;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class MetricsAggregatorEntryPoint
    {
        private static void Main(string[] args)
        {
            var settingsFromFile = Configuration.TryGetSettingsFromFile(args);
            var log = Logging.Configure((string)settingsFromFile?["airlock.consumer.log.file.pattern"] ?? "..\\log\\actions-{Date}.txt");
            var kafkaBootstrapEndpoints = (string)settingsFromFile?["bootstrap.servers"] ?? "devops-kafka1.dev.kontur.ru:9092";
            var airlockApiKey = (string)settingsFromFile?["airlock.apikey"] ?? "UniversalApiKey";
            var airlockReplicas = ((List<object>)settingsFromFile?["airlock.endpoints"] ?? new List<object> { "http://192.168.0.75:8888/" }).Cast<string>().Select(x => new Uri(x)).ToArray();
            const string consumerGroupId = nameof(MetricsAggregatorEntryPoint);
            var settings = new MetricsAggregatorSettings
            {
                MetricAggregationPastGap = ParseTimeSpan(settingsFromFile?["airlock.metricsAggregator.pastGap"], 20.Seconds()),
                MetricAggregationFutureGap = ParseTimeSpan(settingsFromFile?["airlock.metricsAggregator.futureGap"], 1.Hours()),
                MetricAggregationStartGap = ParseTimeSpan(settingsFromFile?["airlock.metricsAggregator.startGap"], 10.Minutes())
            };
            var airlockConfig = new AirlockConfig
            {
                ApiKey = airlockApiKey,
                ClusterProvider = new FixedClusterProvider(airlockReplicas)
            };
            var airlockClient = new AirlockClient(airlockConfig, log);
            var rootMetricScope = new RootMetricScope(new MetricConfiguration
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

            // todo (spaceorc 05.10.2017) "-events" это не очень красиво - подумать и исправить как-то
            var routingKeyFilter = new DirtyRoutingKeyFilter("-events");
            var processorProvider = new DefaultAirlockEventProcessorProvider<MetricEvent, MetricEventSerializer>(project => processor);
            var consumerGroupHostSettings = new ConsumerGroupHostSettings(kafkaBootstrapEndpoints, consumerGroupId);
            var consumer = new ConsumerGroupHost(consumerGroupHostSettings, log, processorProvider, routingKeyFilter);
            
            consumer.Start();
            log.Info($"Consumer '{consumerGroupId}' started");
            var tcs = new TaskCompletionSource<int>();
            Console.CancelKeyPress += (_, e) =>
            {
                log.Info("Stop signal received");
                tcs.TrySetResult(0);
                e.Cancel = true;
            };
            tcs.Task.GetAwaiter().GetResult();
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

        public class DirtyRoutingKeyFilter : IRoutingKeyFilter
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