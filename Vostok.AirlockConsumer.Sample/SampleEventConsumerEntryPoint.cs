using System;
using System.Linq;
using System.Net;
using System.Runtime.Loader;
using System.Threading;
using Vostok.Airlock;
using Vostok.Clusterclient.Topology;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Sample
{
    public static class SampleEventConsumerEntryPoint
    {
        private const string defaultRoutingKey = "prj.dev.srv.dt";
        private const string airlockApiKey = "UniversalApiKey";
        private const string airlockGateEndpoint = "http://gate:6306";
        private const string kafkaBootstrapEndpoints = "kafka:9092";
        private static readonly ManualResetEventSlim stopSignal = new ManualResetEventSlim();

        public static void Main(string[] args)
        {
            AirlockSerializerRegistry.Register(new SampleEventSerializer());
            var log = Logging.Configure();
            Console.CancelKeyPress += (_, e) =>
            {
                log.Warn("Ctrl+C is pressed -> terminating...");
                stopSignal.Set();
                e.Cancel = true;
            };
            AssemblyLoadContext.Default.Unloading += ctx =>
            {
                log.Warn("AssemblyLoadContext.Default.Unloading event is fired -> terminating...");
                stopSignal.Set();
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
                Environment.Exit(1);
            }
        }

        private static void RunProducer(ILog log, string[] args)
        {
            var routingKeys = args.Any() ? args : new[] {defaultRoutingKey};
            log.Info($"Producer started for routingKeys: [{string.Join(", ", routingKeys)}]");
            var airlockClient = new AirlockClient(new AirlockConfig
            {
                ApiKey = airlockApiKey,
                ClusterProvider = new FixedClusterProvider(new Uri(airlockGateEndpoint)),
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
                routingKeys = new[] {defaultRoutingKey};
            var routingKeyFilter = new SampleRoutingKeyFilter(routingKeys);
            var processorProvider = new DefaultAirlockEventProcessorProvider<SampleEvent, SampleEventSerializer>(project => new SampleDataAirlockEventProcessor(log, recedeGap));
            var settings = new ConsumerGroupHostSettings(kafkaBootstrapEndpoints, consumerGroupId, new ProcessorHostSettings(), autoResetOffsetPolicy: AutoResetOffsetPolicy.Earliest);

            IMetricScope rootMetricScope = new RootMetricScope(
                new MetricConfiguration
                {
                    Reporter = new FakeMetricReporter()
                });

            var consumer = new ConsumerGroupHost(settings, log, rootMetricScope, routingKeyFilter, processorProvider);
            consumer.Start();
            stopSignal.Wait(Timeout.Infinite);
            consumer.Stop();
            log.Info("Consumer finished");
        }

        private static TimeSpan? TryParseRecedeGap(string recedeGap)
        {
            return string.IsNullOrEmpty(recedeGap) || !TimeSpan.TryParse(recedeGap, out var recedeGapTimeSpan) ? (TimeSpan?)null : recedeGapTimeSpan;
        }
    }
}