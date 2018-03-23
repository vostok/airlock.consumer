using System;
using System.Linq;
using Vostok.Airlock.Logging;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.Airlock.Consumer.Logs
{
    public class ElasticLogsIndexerEntryPoint : ConsumerApplication
    {
        private const string defaultElasticEndpoints = "http://elasticsearch:9200";

        public static void Main()
        {
            new ConsumerApplicationHost<ElasticLogsIndexerEntryPoint>().Run();
        }

        protected override string ServiceName => "consumer-logs";

        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings
        {
            MaxBatchSize = 100000,
            MaxProcessorQueueSize = 1000000
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, AirlockEnvironmentVariables environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(RoutingKey.LogsSuffix);
            var elasticUris = GetElasticUris(log, environmentVariables);
            processorProvider = new DefaultAirlockEventProcessorProvider<LogEventData, LogEventDataSerializer>(project => new LogAirlockEventProcessor(elasticUris, log));
        }

        private static Uri[] GetElasticUris(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            var elasticEndpoints = environmentVariables.GetValue("ELASTICSEARCH_ENDPOINTS", defaultElasticEndpoints);
            var elasticUris = elasticEndpoints.Split(";", StringSplitOptions.RemoveEmptyEntries).Select(x => new Uri(x)).ToArray();
            log.Info($"ElasticUris: {elasticUris.ToPrettyJson()}");
            return elasticUris;
        }
    }
}