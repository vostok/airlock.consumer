using System;
using System.Net;
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
            var graphitePort = (int?)settingsFromFile?["airlock.consumer.graphite.port"] ?? 2003;
            const string consumerGroupId = nameof(MetricsAirlockConsumerEntryPoint);
            var clientId = (string)settingsFromFile?["client.id"] ?? Dns.GetHostName();
            var processor = new MetricsAirlockEventProcessor(graphiteHost, graphitePort, log);
            var processorProvider = new DefaultAirlockEventProcessorProvider<MetricEvent, MetricEventSerializer>(".metrics", processor);
            var consumer = new ConsumerGroupHost(kafkaBootstrapEndpoints, consumerGroupId, clientId, true, log, processorProvider);
            consumer.Start();
            Console.ReadLine();
            consumer.Stop();
        }
    }
}