using System;
using System.Linq;
using Vstk.Airlock;
using Vstk.Airlock.Tracing;
using Vstk.Contrails.Client;
using Vstk.Logging;
using Vstk.Metrics;
using Vstk.Tracing;

namespace Vstk.AirlockConsumer.Tracing
{
    public class TracingAirlockConsumerEntryPoint : ConsumerApplication
    {
        private const string defaultCassandraEndpoints = "cassandra:9042";

        public static void Main()
        {
            new ConsumerApplicationHost<TracingAirlockConsumerEntryPoint>().Run();
        }

        protected override string ServiceName => "consumer-tracing";

        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings
        {
            MaxBatchSize = 3000,
            MaxProcessorQueueSize = 100000
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, AirlockEnvironmentVariables environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(RoutingKey.TracesSuffix);
            var contrailsClientSettings = GetContrailsClientSettings(log, environmentVariables);
            var contrailsClient = new ContrailsClient(contrailsClientSettings, log);
            processorProvider = new DefaultAirlockEventProcessorProvider<Span, SpanAirlockSerializer>(project => new TracingAirlockEventProcessor(contrailsClient, log, maxCassandraTasks: 1000));
        }

        private static ContrailsClientSettings GetContrailsClientSettings(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            var cassandraEndpoints = environmentVariables.GetValue("CASSANDRA_ENDPOINTS", defaultCassandraEndpoints);
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