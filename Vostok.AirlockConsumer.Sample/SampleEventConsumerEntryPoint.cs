using System;
using System.Net;

namespace Vostok.AirlockConsumer.Sample
{
    public static class SampleEventConsumerEntryPoint
    {
        public static void Main(string[] args)
        {
            var log = Logging.Configure("..\\log\\actions-{Date}.txt");
            const string bootstrapServers = "localhost:9092";
            const string consumerGroupId = nameof(SampleEventConsumerEntryPoint);
            var clientId = Dns.GetHostName();
            var processor = new SampleDataAirlockEventProcessor(log);
            var processorProvider = new DefaultAirlockEventProcessorProvider<SampleEvent, SampleEventSerializer>(":airlock-sample-data", processor);
            var consumer = new ConsumerGroupHost(bootstrapServers, consumerGroupId, clientId, true, log, processorProvider);
            consumer.Start();
            Console.ReadLine();
            consumer.Stop();
        }
    }
}