using System;
using System.Linq;
using System.Net;
using System.Threading;
using Vostok.Airlock;
using Vostok.Clusterclient.Topology;
using Vostok.Logging;

namespace Vostok.AirlockConsumer.Sample
{
    public static class SampleEventConsumerEntryPoint
    {
        private const string AirlockApiKey = "UniversalApiKey";
        private const string DefaultRoutingKey = "prj.dev.srv.dt";
        private const string AirlockGateEndpoint = "http://devops-bots1.dev.kontur.ru:8888";
        private const string KafkaBootstrapEndpoints = "devops-kafka1.dev.kontur.ru:9092";
        private static readonly ManualResetEventSlim stopSignal = new ManualResetEventSlim();

        public static void Main(string[] args)
        {
            AirlockSerializerRegistry.Register(new SampleEventSerializer());
            var log = Logging.Configure("log/actions-{Date}.txt");
            Console.CancelKeyPress += (_, e) =>
            {
                log.Warn("Stop signal received");
                stopSignal.Set();
                e.Cancel = true;
            };
            try
            {
                if (!args.Any())
                    log.Error("Mode is not provided");
                else if (args[0] == "--producer")
                    RunProducer(log, args.Skip(1).ToArray());
                else if (args[0] == "--consumer")
                    RunConsumer(log, args.Skip(1).ToArray());
                else
                    log.Error($"Invalid mode: {args[0]}");
            }
            catch (Exception e)
            {
                log.Fatal(e);
                throw;
            }
        }

        private static void RunProducer(ILog log, string[] args)
        {
            var routingKeys = args.Any() ? args : new[] {DefaultRoutingKey};
            log.Info($"Producer started for routingKeys: [{string.Join(", ", routingKeys)}]");
            var airlockClient = new AirlockClient(new AirlockConfig
            {
                ApiKey = AirlockApiKey,
                ClusterProvider = new FixedClusterProvider(new Uri(AirlockGateEndpoint)),
            }, log.FilterByLevel(LogLevel.Warn));
            do
            {
                foreach (var routingKey in routingKeys)
                {
                    var message = $"msg@{DateTimeOffset.UtcNow:O}";
                    airlockClient.Push(routingKey, new SampleEvent {Message = message});
                    log.Info($"Pushed {message} into {routingKey}");
                }
            } while (!stopSignal.Wait(TimeSpan.FromSeconds(1)));
            log.Info("Producer finished");
        }

        private static void RunConsumer(ILog log, string[] args)
        {
            log.Info("Consumer started");
            var consumerGroupId = $"group@{Dns.GetHostName()}";
            var recedeGap = TryParseRecedeGap(args.FirstOrDefault());
            var routingKeys = (recedeGap.HasValue ? args.Skip(1) : args).ToArray();
            if (!routingKeys.Any())
                routingKeys = new[] {DefaultRoutingKey};
            var processor = new SampleDataAirlockEventProcessor(log, recedeGap);
            var processorProvider = new DefaultAirlockEventProcessorProvider<SampleEvent, SampleEventSerializer>(processor);
            var settings = new ConsumerGroupHostSettings(KafkaBootstrapEndpoints, consumerGroupId, autoResetOffsetPolicy: AutoResetOffsetPolicy.Earliest);
            var consumer = new ConsumerGroupHost(settings, log, processorProvider, new SampleRoutingKeyFilter(routingKeys));
            consumer.Start();
            do
            {
            } while (!stopSignal.Wait(TimeSpan.FromSeconds(1)));
            consumer.Stop();
            log.Info("Consumer finished");
        }

        private static TimeSpan? TryParseRecedeGap(string recedeGap)
        {
            return string.IsNullOrEmpty(recedeGap) || !TimeSpan.TryParse(recedeGap, out var recedeGapTimeSpan) ? (TimeSpan?)null : recedeGapTimeSpan;
        }
    }
}