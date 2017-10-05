using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Vostok.Airlock;
using Vostok.Clusterclient.Topology;
using Vostok.Logging;
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
            var airlockClient = new AirlockClient(airlockConfig, log);
            var processor = new TracesToEventsProcessor(airlockClient);
            var processorProvider = new DefaultAirlockEventProcessorProvider<Span, SpanAirlockSerializer>(RoutingKey.Separator + RoutingKey.TracesSuffix, processor);
            var consumer = new ConsumerGroupHost(bootstrapServers, consumerGroupId, clientId, true, log, processorProvider);
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