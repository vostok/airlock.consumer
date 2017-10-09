using System;
using System.Threading.Tasks;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Metrics
{
    public class MetricsAirlockConsumerEntryPoint
    {
        public static void Main(string[] args)
        {
            var settingsFromFile = Configuration.TryGetSettingsFromFile(args);
            var log = Logging.Configure((string)settingsFromFile?["airlock.consumer.log.file.pattern"] ?? "..\\log\\actions-{Date}.txt");
            var kafkaBootstrapEndpoints = (string)settingsFromFile?["bootstrap.servers"] ?? "devops-kafka1.dev.kontur.ru:9092";
            var graphiteHost = (string)settingsFromFile?["airlock.consumer.graphite.host"] ?? "graphite-relay.skbkontur.ru";
            var graphitePort = 2003; // TODO (int?)settingsFromFile?["airlock.consumer.graphite.port"] ?? 2003;
            const string consumerGroupId = nameof(MetricsAirlockConsumerEntryPoint);
            var processorProvider = new DefaultAirlockEventProcessorProvider<MetricEvent, MetricEventSerializer>(project => new MetricsAirlockEventProcessor(graphiteHost, graphitePort, log));
            var settings = new ConsumerGroupHostSettings(kafkaBootstrapEndpoints, consumerGroupId);
            var consumer = new ConsumerGroupHost(settings, log, processorProvider, new DefaultRoutingKeyFilter(Airlock.RoutingKey.MetricsSuffix));
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
        }
    }
}