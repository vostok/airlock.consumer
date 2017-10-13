using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Airlock;
using Vostok.Contrails.Client;
using Vostok.Logging;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class TracingAirlockConsumerEntryPoint : ConsumerApplication
    {
        private const string defaultCassandraEndpoints = "cassandra:9042";

        public static void Main()
        {
            new ConsumerApplicationHost<TracingAirlockConsumerEntryPoint>().Run();
        }

        protected sealed override void DoInitialize(ILog log, Dictionary<string, string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(RoutingKey.TracesSuffix);
            var contrailsClientSettings = GetContrailsClientSettings(log, environmentVariables);
            var contrailsClient = new ContrailsClient(contrailsClientSettings, log);
            processorProvider = new DefaultAirlockEventProcessorProvider<Span, SpanAirlockSerializer>(project => new TracingAirlockEventProcessor(contrailsClient, maxCassandraTasks: 1000));
        }

        private static ContrailsClientSettings GetContrailsClientSettings(ILog log, Dictionary<string, string> environmentVariables)
        {
            if (!environmentVariables.TryGetValue("AIRLOCK_CASSANDRA_ENDPOINTS", out var cassandraEndpoints))
                cassandraEndpoints = defaultCassandraEndpoints;
            var contrailsClientSettings = new ContrailsClientSettings
            {
                CassandraNodes = cassandraEndpoints.Split(";", StringSplitOptions.RemoveEmptyEntries).Select(x => x).ToArray(),
                Keyspace = "airlock",
                CassandraRetryExecutionStrategySettings = new CassandraRetryExecutionStrategySettings(),
            };
            log.Info($"ContrailsClientSettings: {contrailsClientSettings.ToPrettyJson()}");
            return contrailsClientSettings;
        }
    }
}