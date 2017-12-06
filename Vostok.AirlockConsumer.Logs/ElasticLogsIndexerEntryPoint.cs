using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Airlock;
using Vostok.Airlock.Logging;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Logs
{
    public class ElasticLogsIndexerEntryPoint : ConsumerApplication
    {
        private const string defaultElasticEndpoints = "http://elasticsearch:9200";

        public static void Main()
        {
            new ConsumerApplicationHost<ElasticLogsIndexerEntryPoint>().Run();
        }

        protected override string ServiceName => "consumer-logs";

        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings()
        {
            MaxBatchSize = 100000,
            MaxProcessorQueueSize = 1000000
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, Dictionary<string, string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(RoutingKey.LogsSuffix);
            var elasticUris = GetElasticUris();
            processorProvider = new DefaultAirlockEventProcessorProvider<LogEventData, LogEventDataSerializer>(project => new LogAirlockEventProcessor(elasticUris, log));
        }

        private Uri[] GetElasticUris()
        {
            var elasticEndpoints = GetSettingByName("ELASTICSEARCH_ENDPOINTS", defaultElasticEndpoints);
            var elasticUris = elasticEndpoints.Split(";", StringSplitOptions.RemoveEmptyEntries).Select(x => new Uri(x)).ToArray();
            return elasticUris;
        }
    }
}