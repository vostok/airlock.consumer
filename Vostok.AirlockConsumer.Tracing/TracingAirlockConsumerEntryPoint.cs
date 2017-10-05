using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Vostok.Logging;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class TracingAirlockConsumerEntryPoint
    {
        private static void Main(string[] args)
        {
            var settingsFromFile = Configuration.TryGetSettingsFromFile(args);
            var log = Logging.Configure((string)settingsFromFile?["airlock.consumer.log.file.pattern"] ?? "..\\log\\actions-{Date}.txt");
            var kafkaBootstrapEndpoints = (string)settingsFromFile?["bootstrap.servers"] ?? "devops-kafka1.dev.kontur.ru:9092";
            const string consumerGroupId = nameof(TracingAirlockConsumerEntryPoint);
            var clientId = (string)settingsFromFile?["client.id"] ?? Dns.GetHostName();
            var keyspace = (string)settingsFromFile?["cassandra.keyspace"] ?? "airlock";
            var tableName = (string)settingsFromFile?["cassandra.spans.tablename"] ?? "spans";
            var nodes = ((List<object>)settingsFromFile?["cassandra.endpoints"] ?? new List<object>{ "localhost:9042" }).Cast<string>();
            try
            {
                var sessionKeeper = new CassandraSessionKeeper(nodes, keyspace);
                var dataScheme = new CassandraDataScheme(sessionKeeper.Session, tableName);
                dataScheme.CreateTableIfNotExists();
                var processor = new TracingAirlockEventProcessor(dataScheme);
                var processorProvider = new DefaultAirlockEventProcessorProvider<Span, SpanAirlockSerializer>(":traces", processor);
                var consumer = new ConsumerGroupHost(kafkaBootstrapEndpoints, consumerGroupId, clientId, true, log, processorProvider);
                consumer.Start();
                Console.ReadLine();
                consumer.Stop();
            }
            catch (Exception e)
            {
                log.Error(e);
                throw;
            }
        }
    }
}