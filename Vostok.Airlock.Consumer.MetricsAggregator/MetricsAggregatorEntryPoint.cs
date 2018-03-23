using System;
using System.Linq;
using Vostok.Clusterclient.Topology;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.Airlock.Consumer.MetricsAggregator
{
    public class MetricsAggregatorEntryPoint : ConsumerApplication
    {
        private const string defaultAirlockGateEndpoints = "http://gate:6306";
        private const string defaultAirlockGateApiKey = "UniversalApiKey";
        private AirlockClient airlockClient;

        public static void Main()
        {
            new ConsumerApplicationHost<MetricsAggregatorEntryPoint>().Run();
        }

        public override void Dispose()
        {
            base.Dispose();
            airlockClient?.Dispose();
        }

        protected override string ServiceName => "metrics-aggregator";

        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings
        {
            MaxBatchSize = 1000,
            MaxProcessorQueueSize = 100000
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, AirlockEnvironmentVariables environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            airlockClient = CreateAirlockClient(log, environmentVariables);
            routingKeyFilter = new MetricsAggregatorRotingKeyFilter();
            var settings = new MetricsAggregatorSettings();
            processorProvider = new MetricsAggregatorAirlockEventProcessorProvider(rootMetricScope, airlockClient, settings);
        }

        private static AirlockClient CreateAirlockClient(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            var airlockConfig = GetAirlockConfig(log, environmentVariables);
            var airlockClientLog = Logging.Configure("./log/airlock-{Date}.log", writeToConsole: false);
            return new AirlockClient(airlockConfig, airlockClientLog);
        }

        private static AirlockConfig GetAirlockConfig(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            var airlockGateApiKey = environmentVariables.GetValue("GATE_API_KEY", defaultAirlockGateApiKey);
            var airlockGateEndpoints = environmentVariables.GetValue("GATE_ENDPOINTS", defaultAirlockGateEndpoints);
            var airlockGateUris = airlockGateEndpoints.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries).Select(x => new Uri(x)).ToArray();
            var airlockConfig = new AirlockConfig
            {
                ApiKey = airlockGateApiKey,
                ClusterProvider = new FixedClusterProvider(airlockGateUris),
            };
            log.Info($"AirlockConfig: {airlockConfig.ToPrettyJson()}");
            return airlockConfig;
        }
    }
}