using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Airlock;
using Vostok.Airlock.Tracing;
using Vostok.Contrails.Client;
using Vostok.Logging;
using Vostok.Metrics;
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

        protected override string ServiceName => "consumer-tracing";

        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings()
        {
            MaxBatchSize = 3000,
            MaxProcessorQueueSize = 100000
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, Dictionary<string, string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(RoutingKey.TracesSuffix);
            var contrailsClientSettings = GetContrailsClientSettings();
            var contrailsClient = new ContrailsClient(contrailsClientSettings, log);
            processorProvider = new DefaultAirlockEventProcessorProvider<Span, SpanAirlockSerializer>(project => new TracingAirlockEventProcessor(contrailsClient, log, maxCassandraTasks: 1000));
        }

        private ContrailsClientSettings GetContrailsClientSettings()
        {
            var cassandraEndpoints = GetSettingByName("CASSANDRA_ENDPOINTS", defaultCassandraEndpoints);
            var contrailsClientSettings = new ContrailsClientSettings
            {
                CassandraNodes = cassandraEndpoints.Split(";", StringSplitOptions.RemoveEmptyEntries).Select(x => x).ToArray(),
                Keyspace = "airlock",
                CassandraRetryExecutionStrategySettings = new CassandraRetryExecutionStrategySettings(),
            };
            return contrailsClientSettings;
        }
    }
}