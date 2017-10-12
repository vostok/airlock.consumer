using System;
using System.Collections.Generic;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Metrics
{
    public class MetricsAirlockConsumerEntryPoint : ConsumerApplication
    {
        private const string defaultGraphiteEndpoint = "graphite:2003";

        public static void Main()
        {
            new ConsumerApplicationHost<MetricsAirlockConsumerEntryPoint>().Run();
        }

        protected sealed override void DoInitialize(ILog log, Dictionary<string, string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(Airlock.RoutingKey.MetricsSuffix);
            var graphiteUri = GetGraphiteUri(log, environmentVariables);
            processorProvider = processorProvider = new DefaultAirlockEventProcessorProvider<MetricEvent, MetricEventSerializer>(project => new MetricsAirlockEventProcessor(graphiteUri, log));
            // todo (avk, 11.10.2017): wait for graphite to start https://github.com/vostok/airlock.consumer/issues/11
        }

        private static Uri GetGraphiteUri(ILog log, Dictionary<string, string> environmentVariables)
        {
            if (!environmentVariables.TryGetValue("AIRLOCK_GRAPHITE_ENDPOINT", out var graphiteEndpoint))
                graphiteEndpoint = defaultGraphiteEndpoint;
            var graphiteUri = new Uri("tcp://" + graphiteEndpoint);
            log.Info($"GraphiteUri: {graphiteUri}");
            return graphiteUri;
        }
    }
}