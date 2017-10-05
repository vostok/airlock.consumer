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
        private const string DefaultRoutingKey = "sample.dev";
        private const string SampleDataType = "airlock-sample-data";
        private const string AirlockGateEndpoint = "http://devops-bots1.dev.kontur.ru:8888";
        private const string KafkaBootstrapEndpoints = "devops-kafka1.dev.kontur.ru:9092";
        private static readonly ManualResetEventSlim stopSignal = new ManualResetEventSlim();

        public static void Main(string[] args)
        {
            AirlockSerializerRegistry.Register(new SampleEventSerializer());
            var log = Logging.Configure("..\\log\\actions-{Date}.txt");
            Console.CancelKeyPress += (_, e) =>
            {
                log.Info("Stop signal received");
                stopSignal.Set();
                e.Cancel = true;
            };
            try
            {
                if (!args.Any())
                    log.Error("Mode is not provided");
                else if (args[0] == "--producer")
                    RunProducer(log, routingKey: args.Length > 1 ? args[1] : DefaultRoutingKey);
                else if (args[0] == "--consumer")
                    RunConsumer(log);
                else
                    log.Error($"Invalid mode: {args[0]}");
            }
            catch (Exception e)
            {
                log.Fatal(e);
                throw;
            }
        }

        private static void RunProducer(ILog log, string routingKey)
        {
            log.Info("Producer started");
            var airlockClient = new AirlockClient(new AirlockConfig
            {
                ApiKey = AirlockApiKey,
                ClusterProvider = new FixedClusterProvider(new Uri(AirlockGateEndpoint)),
            }, log);
            do
            {
                airlockClient.Push($"{routingKey}.{SampleDataType}", new SampleEvent
                {
                    Message = $"hello-o-o-o-o-o-o-o-o-o!!! @{DateTimeOffset.UtcNow:O}"
                });
            } while (!stopSignal.Wait(TimeSpan.FromSeconds(1)));
            log.Info("Producer finished");
        }

        private static void RunConsumer(ILog log)
        {
            log.Info("Consumer started");
            var consumerGroupId = $"SampleConsumer-{Dns.GetHostName()}";
            var clientId = Dns.GetHostName();
            var processor = new SampleDataAirlockEventProcessor(log);
            var processorProvider = new DefaultAirlockEventProcessorProvider<SampleEvent, SampleEventSerializer>(SampleDataType, processor);
            var consumer = new ConsumerGroupHost(KafkaBootstrapEndpoints, consumerGroupId, clientId, true, log, processorProvider);
            consumer.Start();
            do
            {

            } while (!stopSignal.Wait(TimeSpan.FromSeconds(1)));
            consumer.Stop();
            log.Info("Consumer finished");
        }
    }
}