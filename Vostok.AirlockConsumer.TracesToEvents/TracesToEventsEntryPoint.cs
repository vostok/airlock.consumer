using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Vostok.Airlock;
using Vostok.Clusterclient.Topology;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.TracesToEvents
{
    public class TracesToEventsEntryPoint
    {
        public static void Main(string[] args)
        {
            var settingsFromFile = Configuration.TryGetSettingsFromFile(args);
            var log = Logging.Configure((string)settingsFromFile?["airlock.consumer.log.file.pattern"] ?? "..\\log\\actions-{Date}.txt");
            var bootstrapServers = (string)settingsFromFile?["bootstrap.servers"] ?? "localhost:9092";
            var airlockApiKey = (string)settingsFromFile?["airlock.apikey"] ?? "UniversalApiKey";
            var airlockReplicas = ((List<object>)settingsFromFile?["airlock.endpoints"] ?? new List<object>{"http://192.168.0.75:8888/"}).Cast<string>().Select(x => new Uri(x)).ToArray();
            const string consumerGroupId = nameof(TracesToEventsEntryPoint);
            var clientId = (string)settingsFromFile?["client.id"] ?? Dns.GetHostName();

            var airlockConfig = new AirlockConfig
            {
                ApiKey = airlockApiKey,
                ClusterProvider = new FixedClusterProvider(airlockReplicas)
            };
            var airlock = new Airlock.Airlock(airlockConfig, log);
            var processor = new TracesToEventsProcessor(airlock);
            var processorProvider = new DefaultAirlockEventProcessorProvider<Span, SpanAirlockSerializer>(".traces", processor);
            var consumer = new ConsumerGroupHost(bootstrapServers, consumerGroupId, clientId, true, log, processorProvider);
            consumer.Start();
            Console.ReadLine();
            consumer.Stop();
        }
    }
}