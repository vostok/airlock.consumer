using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vostok.Airlock;
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
            var keyspace = (string)settingsFromFile?["cassandra.keyspace"] ?? "airlock";
            var tableName = (string)settingsFromFile?["cassandra.spans.tablename"] ?? "spans";
            var nodes = ((List<object>)settingsFromFile?["cassandra.endpoints"] ?? new List<object>{ "localhost:9042" }).Cast<string>();
            try
            {
                var retryExecutionStrategySettings = new CassandraRetryExecutionStrategySettings(settingsFromFile);
                var sessionKeeper = new CassandraSessionKeeper(nodes, keyspace);
                var retryExecutionStrategy = new CassandraRetryExecutionStrategy(retryExecutionStrategySettings, log, sessionKeeper.Session);
                var dataScheme = new CassandraDataScheme(sessionKeeper.Session, tableName);
                dataScheme.CreateTableIfNotExists();
                var processor = new TracingAirlockEventProcessor(dataScheme, retryExecutionStrategy, int.Parse(settingsFromFile?["cassandra.max.threads"]?.ToString() ?? "1000"));
                var processorProvider = new DefaultAirlockEventProcessorProvider<Span, SpanAirlockSerializer>(processor);
                var settings = new ConsumerGroupHostSettings(kafkaBootstrapEndpoints, consumerGroupId);
                var consumer = new ConsumerGroupHost(settings, log, processorProvider, new DefaultRoutingKeyFilter(RoutingKey.Separator + RoutingKey.TracesSuffix));
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
            catch (Exception e)
            {
                log.Error(e);
                throw;
            }
        }
    }
}