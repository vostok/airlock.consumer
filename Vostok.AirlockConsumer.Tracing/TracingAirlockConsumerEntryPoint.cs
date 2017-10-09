using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vostok.Airlock;
using Vostok.Contrails.Client;
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
            var nodes = ((List<object>)settingsFromFile?["cassandra.endpoints"] ?? new List<object>{ "localhost:9042" }).Cast<string>();
            try
            {
                var contrailsClientSettings = new ContrailsClientSettings()
                {
                    CassandraNodes = nodes,
                    Keyspace = keyspace,
                    CassandraRetryExecutionStrategySettings = GetCassandraSettings(settingsFromFile)
                };
                var contrailsClient = new ContrailsClient(contrailsClientSettings, log);
                var maxCassandraTasks = int.Parse(settingsFromFile?["cassandra.max.threads"]?.ToString() ?? "1000");
                var processorProvider = new DefaultAirlockEventProcessorProvider<Span, SpanAirlockSerializer>(project => new TracingAirlockEventProcessor(contrailsClient, maxCassandraTasks));
                var settings = new ConsumerGroupHostSettings(kafkaBootstrapEndpoints, consumerGroupId);
                var consumer = new ConsumerGroupHost(settings, log, processorProvider, new DefaultRoutingKeyFilter(RoutingKey.TracesSuffix));
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

        private static CassandraRetryExecutionStrategySettings GetCassandraSettings(Dictionary<string, object> settings)
        {
            var retryExecutionStrategySettings = new CassandraRetryExecutionStrategySettings();
            if (settings == null)
                return retryExecutionStrategySettings;
            if (settings["cassandra.save.retry.max.attempts"] != null)
            {
                retryExecutionStrategySettings.CassandraSaveRetryMaxAttempts = int.Parse(settings["cassandra.save.retry.max.attempts"].ToString());
            }
            if (settings["cassandra.save.retry.min.delay"] != null)
            {
                retryExecutionStrategySettings.CassandraSaveRetryMinDelay = TimeSpan.Parse(settings["cassandra.save.retry.min.delay"].ToString());
            }
            if (settings["cassandra.save.retry.max.delay"] != null)
            {
                retryExecutionStrategySettings.CassandraSaveRetryMaxDelay = TimeSpan.Parse(settings["cassandra.save.retry.max.delay"].ToString());
            }
            return retryExecutionStrategySettings;
        }
    }
}