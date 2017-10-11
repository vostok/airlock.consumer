﻿using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Airlock;
using Vostok.Logging;
using Vostok.Logging.Airlock;

namespace Vostok.AirlockConsumer.Logs
{
    public class ElasticLogsIndexerEntryPoint : ConsumerApplication
    {
        private const string defaultElasticEndpoints = "http://elastic:9200";

        public static void Main()
        {
            new ConsumerApplicationHost<ElasticLogsIndexerEntryPoint>().Run();
        }

        protected sealed override void DoInitialize(ILog log, Dictionary<string, string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(RoutingKey.LogsSuffix);
            var elasticUris = GetElasticUris(log, environmentVariables);
            processorProvider = new DefaultAirlockEventProcessorProvider<LogEventData, LogEventDataSerializer>(project => new LogAirlockEventProcessor(elasticUris, log));
            // todo (avk, 11.10.2017): wait for elastic to start
        }

        private static Uri[] GetElasticUris(ILog log, Dictionary<string, string> environmentVariables)
        {
            if (!environmentVariables.TryGetValue("AIRLOCK_ELASTICSEARCH_ENDPOINTS", out var elasticEndpoints))
                elasticEndpoints = defaultElasticEndpoints;
            var elasticUris = elasticEndpoints.Split(";", StringSplitOptions.RemoveEmptyEntries).Select(x => new Uri(x)).ToArray();
            log.Info($"ElasticUris: {elasticUris.ToPrettyJson()}");
            return elasticUris;
        }
    }
}